use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use lance::Result as LanceResult;
use lance_context_api::schema_spec::{ColumnSpec, ColumnType, SchemaSpec, ID_COLUMN};
use serde_json::json;

use super::*;
use crate::generic_codec::Row;
use crate::{GenericStore, GenericStoreOptions};

fn spec() -> SchemaSpec {
    SchemaSpec::new(vec![
        (
            ID_COLUMN.to_string(),
            ColumnSpec::required(ColumnType::String { large: false }),
        ),
        ("score".to_string(), ColumnSpec::new(ColumnType::Float32)),
    ])
}

fn rows(range: std::ops::Range<usize>) -> Vec<Row> {
    range
        .map(|i| {
            json!({"id": format!("r{i}"), "score": i as f32})
                .as_object()
                .unwrap()
                .clone()
        })
        .collect()
}

fn ids_of_rows(rows: &[Row]) -> Vec<String> {
    rows.iter()
        .map(|r| r[ID_COLUMN].as_str().unwrap().to_string())
        .collect()
}

fn sealing() -> GenericStoreOptions {
    GenericStoreOptions {
        seal_on_add: true,
        ..Default::default()
    }
}

/// A store whose rows are all merged into the base table, so a read view has
/// no pending generation.
async fn merged_store(uri: &str, n: usize) -> GenericStore {
    let mut store = GenericStore::open(uri, spec(), sealing()).await.unwrap();
    store.add(&rows(0..n)).await.unwrap();
    store.cleanup_wal().await.unwrap();
    assert_eq!(store.pending_wal_generations().await.unwrap(), 0);
    assert_eq!(store.count_base_rows().await.unwrap(), n);
    store
}

/// Delegates to the scanner, then drops `drop_id` from the result.
#[derive(Debug)]
struct DroppingLookup {
    drop_id: &'static str,
}

#[async_trait]
impl IdLookup for DroppingLookup {
    async fn lookup(
        &self,
        view: IdLookupView<'_>,
        ids: &[&str],
        columns: &[String],
    ) -> LanceResult<Option<Vec<RecordBatch>>> {
        let kept: Vec<&str> = ids
            .iter()
            .copied()
            .filter(|id| *id != self.drop_id)
            .collect();
        ScannerIdLookup.lookup(view, &kept, columns).await
    }

    fn name(&self) -> &'static str {
        "dropping"
    }
}

/// Covers no view.
#[derive(Debug)]
struct NeverCovers;

#[async_trait]
impl IdLookup for NeverCovers {
    async fn lookup(
        &self,
        _: IdLookupView<'_>,
        _: &[&str],
        _: &[String],
    ) -> LanceResult<Option<Vec<RecordBatch>>> {
        Ok(None)
    }

    fn name(&self) -> &'static str {
        "never"
    }
}

fn want(ids: &[&str]) -> Vec<String> {
    ids.iter().map(|s| (*s).to_string()).collect()
}

#[tokio::test]
async fn comparing_reports_a_planted_difference_and_nothing_when_arms_agree() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("t").to_str().unwrap().to_string();
    let store = merged_store(&uri, 50).await;

    let agreeing = ComparingIdLookup::new(Arc::new(ScannerIdLookup), Arc::new(ScannerIdLookup));
    let report = agreeing.report();
    let store = store.with_id_lookup(Arc::new(agreeing));
    store
        .get_many(&want(&["r1", "r2", "missing"]), None)
        .await
        .unwrap();
    assert_eq!(report.counts(), (1, 0, 0, 0));

    let planted = ComparingIdLookup::new(
        Arc::new(ScannerIdLookup),
        Arc::new(DroppingLookup { drop_id: "r2" }),
    );
    let report = planted.report();
    let store = store.with_id_lookup(Arc::new(planted));
    let got = store.get_many(&want(&["r1", "r2"]), None).await.unwrap();
    assert_eq!(
        ids_of_rows(&got),
        ["r1", "r2"],
        "the caller sees the primary"
    );
    assert_eq!(report.counts(), (0, 1, 0, 0));
}

#[tokio::test]
async fn an_arm_that_covers_nothing_fails_alone_and_falls_back_when_routed() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("t").to_str().unwrap().to_string();
    let store = merged_store(&uri, 10).await;

    let store = store.with_id_lookup(Arc::new(NeverCovers));
    let error = store.get_many(&want(&["r1"]), None).await.unwrap_err();
    assert!(error.to_string().contains("does not cover"), "{error}");

    let routed = RoutedIdLookup::new(Arc::new(NeverCovers), Arc::new(ScannerIdLookup));
    let report = routed.report();
    let store = store.with_id_lookup(Arc::new(routed));
    let got = store.get_many(&want(&["r3", "r1"]), None).await.unwrap();
    assert_eq!(ids_of_rows(&got), ["r3", "r1"]);
    assert_eq!(report.counts(), (0, 1));
}

#[cfg(feature = "quack")]
mod quack_arm {
    use super::*;
    use crate::id_lookup::quack::tests_support::{keep_wanted_for_test, lane_candidates_for_test};

    #[tokio::test]
    async fn quack_returns_exactly_the_requested_ids_in_caller_order() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("t").to_str().unwrap().to_string();
        let store = merged_store(&uri, 1000)
            .await
            .with_id_lookup(Arc::new(QuackIdLookup::new()));

        let got = store
            .get_many(&want(&["r777", "missing", "r5", "r999", "r0"]), None)
            .await
            .unwrap();
        assert_eq!(ids_of_rows(&got), ["r777", "r5", "r999", "r0"]);
        assert_eq!(got[0]["score"], json!(777.0));
        assert!(store
            .get_many(&want(&["nope"]), None)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn quack_agrees_with_the_scanner_on_every_query() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("t").to_str().unwrap().to_string();
        let comparing =
            ComparingIdLookup::new(Arc::new(ScannerIdLookup), Arc::new(QuackIdLookup::new()));
        let report = comparing.report();
        let store = merged_store(&uri, 3000)
            .await
            .with_id_lookup(Arc::new(comparing));

        let mut queries = 0u64;
        for start in (0..3200).step_by(97) {
            // Strided sets that run past the end, so some ids are missing.
            let ids: Vec<String> = (start..start + 40)
                .step_by(7)
                .map(|i| format!("r{i}"))
                .collect();
            store.get_many(&ids, None).await.unwrap();
            queries += 1;
        }
        let (agreed, differed, not_covered, errors) = report.counts();
        assert_eq!((differed, not_covered, errors), (0, 0, 0));
        assert_eq!(agreed, queries);
    }

    #[tokio::test]
    async fn quack_does_not_cover_a_view_with_pending_generations() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("t").to_str().unwrap().to_string();
        let store = merged_store(&uri, 20).await;
        // One more sealed generation, not merged.
        store.add(&rows(20..25)).await.unwrap();
        assert!(store.pending_wal_generations().await.unwrap() > 0);

        let routed = RoutedIdLookup::new(Arc::new(QuackIdLookup::new()), Arc::new(ScannerIdLookup));
        let report = routed.report();
        let store = store.with_id_lookup(Arc::new(routed));
        let got = store.get_many(&want(&["r22", "r3"]), None).await.unwrap();
        assert_eq!(ids_of_rows(&got), ["r22", "r3"]);
        assert_eq!(report.counts(), (0, 1), "the scanner answered");
    }

    #[tokio::test]
    async fn quack_rebuilds_its_lane_for_a_new_version() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("t").to_str().unwrap().to_string();
        let quack = Arc::new(QuackIdLookup::new());
        let mut store = merged_store(&uri, 10).await.with_id_lookup(quack.clone());
        assert!(store
            .get_many(&want(&["r12"]), None)
            .await
            .unwrap()
            .is_empty());

        store.add(&rows(10..15)).await.unwrap();
        store.cleanup_wal().await.unwrap();
        let got = store.get_many(&want(&["r12", "r1"]), None).await.unwrap();
        assert_eq!(ids_of_rows(&got), ["r12", "r1"]);
    }

    #[test]
    fn a_key_collision_yields_both_rows_and_the_id_check_keeps_one() {
        // Distinct keys: the mask keeps only the row with the asked key.
        let candidates = lane_candidates_for_test(&[7, 8, 9, 8], &[100, 101, 102, 103], 8);
        assert_eq!(candidates, [101, 103]);
        assert!(lane_candidates_for_test(&[7, 8, 9], &[100, 101, 102], 5).is_empty());

        // Two rows with the same key: the mask keeps both.
        let candidates = lane_candidates_for_test(&[7, 7, 9], &[100, 101, 102], 7);
        assert_eq!(candidates, [100, 101]);

        // The id check drops the row whose id was not requested.
        let kept = keep_wanted_for_test(&["a", "b", "c"], &["b"]);
        assert_eq!(kept, ["b"]);
    }
}

/// Delegates to the scanner and counts calls.
#[derive(Debug, Default)]
struct CountingLookup {
    calls: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl IdLookup for CountingLookup {
    async fn lookup(
        &self,
        view: IdLookupView<'_>,
        ids: &[&str],
        columns: &[String],
    ) -> LanceResult<Option<Vec<RecordBatch>>> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        ScannerIdLookup.lookup(view, ids, columns).await
    }

    fn name(&self) -> &'static str {
        "counting"
    }
}

#[tokio::test]
async fn get_resolves_through_the_store_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("t").to_str().unwrap().to_string();
    let counting = Arc::new(CountingLookup::default());
    let store = merged_store(&uri, 10)
        .await
        .with_id_lookup(counting.clone());
    let found = store.get("r4", None).await.unwrap().unwrap();
    assert_eq!(found["score"], json!(4.0));
    assert!(store.get("missing", None).await.unwrap().is_none());
    assert_eq!(counting.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
}

/// Column names of `batches`, which must agree.
fn names(batches: &[RecordBatch]) -> Vec<String> {
    batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

#[tokio::test]
async fn lookup_plain_returns_exactly_the_requested_columns_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("t").to_str().unwrap().to_string();
    let store = merged_store(&uri, 10).await;
    let dataset = lance::Dataset::open(&uri).await.unwrap();
    let _ = store;
    let arm = ScannerIdLookup;

    // Without `id`: it is read for the lookup and dropped.
    let only_score = lookup_plain(&arm, &dataset, &["r1", "r2"], &["score"])
        .await
        .unwrap();
    assert_eq!(names(&only_score), ["score"]);
    assert_eq!(
        only_score.iter().map(RecordBatch::num_rows).sum::<usize>(),
        2
    );

    // Against schema order (`id`, `score`).
    let reversed = lookup_plain(&arm, &dataset, &["r1"], &["score", "id"])
        .await
        .unwrap();
    assert_eq!(names(&reversed), ["score", "id"]);
}

#[cfg(feature = "quack")]
mod quack_plain {
    use super::*;

    #[tokio::test]
    async fn quack_keeps_the_requested_column_order_on_a_plain_dataset() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("t").to_str().unwrap().to_string();
        let _store = merged_store(&uri, 30).await;
        let dataset = lance::Dataset::open(&uri).await.unwrap();
        let arm = QuackIdLookup::new();
        let got = lookup_plain(&arm, &dataset, &["r7", "r29", "nope"], &["score", "id"])
            .await
            .unwrap();
        assert_eq!(names(&got), ["score", "id"]);
        assert_eq!(
            ids_of(&got).unwrap(),
            ["r29", "r7"].map(String::from).into()
        );
    }

    /// The arm itself, not `lookup_plain`, returns the requested order:
    /// `take_rows` alone would return schema order (`id`, `score`).
    #[tokio::test]
    async fn the_quack_arm_returns_columns_in_requested_order() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("t").to_str().unwrap().to_string();
        let _store = merged_store(&uri, 5).await;
        let dataset = lance::Dataset::open(&uri).await.unwrap();
        let columns = ["score".to_string(), ID_COLUMN.to_string()];
        let got = QuackIdLookup::new()
            .lookup(IdLookupView::plain(&dataset), &["r2"], &columns)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(names(&got), ["score", "id"]);
    }

    #[test]
    fn the_default_arm_prefers_quack() {
        assert_eq!(default_id_lookup().name(), "routed");
    }
}
