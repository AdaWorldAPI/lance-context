//! Lookup by record id, behind one seam.
//!
//! [`GenericStore::get_many`](crate::GenericStore::get_many) resolves ids
//! through an [`IdLookup`]. The default, [`ScannerIdLookup`], is the Lance
//! LSM scanner with an `id IN (...)` filter, which is what the store did
//! before this seam existed. With the `quack` feature,
//! [`QuackIdLookup`](crate::QuackIdLookup) answers the same question from a
//! `u64` key lane evaluated by lance-graph's Quack, and fetches the rows by
//! row id.
//!
//! Two wrappers combine arms:
//!
//! - [`ComparingIdLookup`] returns the primary arm's rows and compares them,
//!   as a set of ids, with a shadow arm's rows. Differences are counted and
//!   logged; the caller never sees the shadow's answer.
//! - [`RoutedIdLookup`] asks a preferred arm first and uses the fallback
//!   only for views the preferred arm does not cover. Both counts are kept so
//!   the split is visible.
//!
//! An arm returns `Ok(None)` for a view it does not cover. The Quack arm
//! covers views with no flushed write-ahead-log generations, because its key
//! lane is built from the base dataset only.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow_array::{Array, LargeStringArray, RecordBatch, StringArray};
use async_trait::async_trait;
use datafusion::prelude::{col, lit, Expr};
use futures::TryStreamExt;
use lance::dataset::mem_wal::scanner::{LsmScanner, ShardSnapshot};
use lance::{Dataset, Error as LanceError, Result as LanceResult};
use lance_context_api::schema_spec::ID_COLUMN;

use crate::store_base::{ListSource, StorageBase};

#[cfg(feature = "quack")]
mod quack;
#[cfg(feature = "quack")]
pub use quack::QuackIdLookup;

/// The view a lookup reads: one base dataset version and the write-ahead-log
/// generations captured with it.
#[derive(Clone, Copy)]
pub struct IdLookupView<'a> {
    /// The base dataset, at the version the caller is reading.
    pub dataset: &'a Dataset,
    /// The flushed generations captured for `dataset`.
    pub snapshots: &'a [ShardSnapshot],
}

impl IdLookupView<'_> {
    /// Whether no flushed generation is pending, so the base dataset alone
    /// holds every readable row.
    #[must_use]
    pub fn is_base_only(&self) -> bool {
        self.snapshots.iter().all(|s| s.sstables.is_empty())
    }
}

/// Resolves record ids to the rows that hold them.
///
/// `ids` are distinct. An id with no row is absent from the result. Row order
/// is not specified. `columns` always contains the id column.
#[async_trait]
pub trait IdLookup: Send + Sync + fmt::Debug {
    /// The rows for `ids` in `view`, or `None` if this arm does not cover
    /// `view`.
    async fn lookup(
        &self,
        view: IdLookupView<'_>,
        ids: &[&str],
        columns: &[String],
    ) -> LanceResult<Option<Vec<RecordBatch>>>;

    /// A short name for logs and reports.
    fn name(&self) -> &'static str;
}

/// The Lance LSM scanner with an `id IN (...)` filter. Covers every view.
#[derive(Debug, Default, Clone, Copy)]
pub struct ScannerIdLookup;

#[async_trait]
impl IdLookup for ScannerIdLookup {
    async fn lookup(
        &self,
        view: IdLookupView<'_>,
        ids: &[&str],
        columns: &[String],
    ) -> LanceResult<Option<Vec<RecordBatch>>> {
        let filter = col(ID_COLUMN).in_list(ids.iter().map(|id| lit(*id)).collect(), false);
        let scanner = StorageBase::lsm_scanner_for_dataset(
            view.dataset,
            ID_COLUMN,
            ListSource::All,
            view.snapshots.to_vec(),
        );
        let scanner = batch_get_scanner(scanner, &filter, columns)?;
        let read = async {
            let mut stream = scanner.try_into_stream().await?;
            let mut batches = Vec::new();
            while let Some(batch) = stream.try_next().await? {
                batches.push(batch);
            }
            Ok::<_, LanceError>(batches)
        }
        .await;
        match read {
            Err(error) if is_wal_pk_batch_error(&error) => {
                // Lance 9's batched WAL membership probe can reject a PK
                // sidecar that its point lookup can still read. Discard
                // partial results and retry all keys through that native
                // route against the SAME captured view. Other errors
                // propagate; no missing row is inferred from a failed read.
                tracing::warn!(
                    "batch get retrying native point lookups after a WAL PK index error"
                );
                let refs: Vec<&str> = columns.iter().map(String::as_str).collect();
                let mut batches = Vec::new();
                for id in ids {
                    let scanner = StorageBase::lsm_scanner_for_dataset(
                        view.dataset,
                        ID_COLUMN,
                        ListSource::All,
                        view.snapshots.to_vec(),
                    )
                    .project(&refs)?
                    .filter_expr(col(ID_COLUMN).eq(lit(*id)));
                    let mut stream = scanner.try_into_stream().await?;
                    while let Some(batch) = stream.try_next().await? {
                        batches.push(batch);
                    }
                }
                Ok(Some(batches))
            }
            result => result.map(Some),
        }
    }

    fn name(&self) -> &'static str {
        "scanner"
    }
}

pub(crate) fn batch_get_scanner(
    scanner: LsmScanner,
    filter: &Expr,
    columns: &[String],
) -> LanceResult<LsmScanner> {
    let refs: Vec<&str> = columns.iter().map(String::as_str).collect();
    // Lance 9 routes a bare PK IN to lookup_many_via_per_key, which serializes
    // disk reads. Explicit OFFSET 0 selects its general LSM scan: one indexed
    // IN scan per source with Lance's native cross-generation suppression.
    // This is a compatibility adapter using public APIs; the plan regression
    // test guards it when Lance changes its routing. No LIMIT: source-local
    // caps can underfill after newer generations suppress older matches.
    scanner
        .project(&refs)?
        .filter_expr(filter.clone())
        .limit(None, Some(0))
}

pub(crate) fn is_wal_pk_batch_error(error: &LanceError) -> bool {
    error
        .to_string()
        .contains("RowAddrTreeMap::from_sorted_iter called with non-sorted input")
}

/// The ids present in `batches`, as a set.
///
/// # Errors
///
/// If a batch has no string id column.
pub fn ids_of(batches: &[RecordBatch]) -> LanceResult<BTreeSet<String>> {
    let mut out = BTreeSet::new();
    for batch in batches {
        let column = batch.column_by_name(ID_COLUMN).ok_or_else(|| {
            LanceError::invalid_input(format!("lookup result has no `{ID_COLUMN}` column"))
        })?;
        if let Some(a) = column.as_any().downcast_ref::<StringArray>() {
            out.extend(
                (0..a.len())
                    .filter(|&i| a.is_valid(i))
                    .map(|i| a.value(i).to_string()),
            );
        } else if let Some(a) = column.as_any().downcast_ref::<LargeStringArray>() {
            out.extend(
                (0..a.len())
                    .filter(|&i| a.is_valid(i))
                    .map(|i| a.value(i).to_string()),
            );
        } else {
            return Err(LanceError::invalid_input(format!(
                "lookup result `{ID_COLUMN}` column is not a string column"
            )));
        }
    }
    Ok(out)
}

/// Counters a [`ComparingIdLookup`] keeps.
#[derive(Debug, Default)]
pub struct ComparisonReport {
    /// Lookups where the shadow answered and agreed.
    pub agreed: AtomicU64,
    /// Lookups where the shadow answered with a different set of ids.
    pub differed: AtomicU64,
    /// Lookups the shadow did not cover.
    pub not_covered: AtomicU64,
    /// Lookups where the shadow returned an error.
    pub shadow_errors: AtomicU64,
}

impl ComparisonReport {
    /// `(agreed, differed, not_covered, shadow_errors)`.
    #[must_use]
    pub fn counts(&self) -> (u64, u64, u64, u64) {
        (
            self.agreed.load(Ordering::Relaxed),
            self.differed.load(Ordering::Relaxed),
            self.not_covered.load(Ordering::Relaxed),
            self.shadow_errors.load(Ordering::Relaxed),
        )
    }
}

/// Answers from `primary`, compares with `shadow`.
///
/// Both arms read the same [`IdLookupView`]. Results are compared as sets of
/// ids, because neither arm promises an order. A shadow error is counted and
/// logged and never fails the lookup.
#[derive(Debug, Clone)]
pub struct ComparingIdLookup {
    primary: Arc<dyn IdLookup>,
    shadow: Arc<dyn IdLookup>,
    report: Arc<ComparisonReport>,
}

impl ComparingIdLookup {
    /// Compare `shadow` against `primary`.
    #[must_use]
    pub fn new(primary: Arc<dyn IdLookup>, shadow: Arc<dyn IdLookup>) -> Self {
        Self {
            primary,
            shadow,
            report: Arc::default(),
        }
    }

    /// The counters, shared with every clone of this wrapper.
    #[must_use]
    pub fn report(&self) -> Arc<ComparisonReport> {
        self.report.clone()
    }
}

#[async_trait]
impl IdLookup for ComparingIdLookup {
    async fn lookup(
        &self,
        view: IdLookupView<'_>,
        ids: &[&str],
        columns: &[String],
    ) -> LanceResult<Option<Vec<RecordBatch>>> {
        let primary = self.primary.lookup(view, ids, columns).await?;
        let Some(primary_batches) = primary else {
            return Ok(None);
        };
        match self.shadow.lookup(view, ids, columns).await {
            Ok(None) => {
                self.report.not_covered.fetch_add(1, Ordering::Relaxed);
            }
            Ok(Some(shadow_batches)) => {
                let expected = ids_of(&primary_batches)?;
                let got = ids_of(&shadow_batches)?;
                if expected == got {
                    self.report.agreed.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.report.differed.fetch_add(1, Ordering::Relaxed);
                    let missing: Vec<_> = expected.difference(&got).collect();
                    let extra: Vec<_> = got.difference(&expected).collect();
                    tracing::warn!(
                        primary = self.primary.name(),
                        shadow = self.shadow.name(),
                        dataset_version = view.dataset.version().version,
                        ?missing,
                        ?extra,
                        "id lookup arms disagree"
                    );
                }
            }
            Err(error) => {
                self.report.shadow_errors.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(shadow = self.shadow.name(), %error, "id lookup shadow failed");
            }
        }
        Ok(Some(primary_batches))
    }

    fn name(&self) -> &'static str {
        "comparing"
    }
}

/// Counters a [`RoutedIdLookup`] keeps.
#[derive(Debug, Default)]
pub struct RoutingReport {
    /// Lookups answered by the preferred arm.
    pub preferred: AtomicU64,
    /// Lookups the preferred arm did not cover, answered by the fallback.
    pub fallback: AtomicU64,
}

impl RoutingReport {
    /// `(preferred, fallback)`.
    #[must_use]
    pub fn counts(&self) -> (u64, u64) {
        (
            self.preferred.load(Ordering::Relaxed),
            self.fallback.load(Ordering::Relaxed),
        )
    }
}

/// Answers from `preferred` where it covers the view, else from `fallback`.
///
/// An error from `preferred` fails the lookup; only `None` (not covered)
/// routes to the fallback.
#[derive(Debug, Clone)]
pub struct RoutedIdLookup {
    preferred: Arc<dyn IdLookup>,
    fallback: Arc<dyn IdLookup>,
    report: Arc<RoutingReport>,
}

impl RoutedIdLookup {
    /// Prefer `preferred`; use `fallback` for views it does not cover.
    #[must_use]
    pub fn new(preferred: Arc<dyn IdLookup>, fallback: Arc<dyn IdLookup>) -> Self {
        Self {
            preferred,
            fallback,
            report: Arc::default(),
        }
    }

    /// The counters, shared with every clone of this wrapper.
    #[must_use]
    pub fn report(&self) -> Arc<RoutingReport> {
        self.report.clone()
    }
}

#[async_trait]
impl IdLookup for RoutedIdLookup {
    async fn lookup(
        &self,
        view: IdLookupView<'_>,
        ids: &[&str],
        columns: &[String],
    ) -> LanceResult<Option<Vec<RecordBatch>>> {
        if let Some(batches) = self.preferred.lookup(view, ids, columns).await? {
            self.report.preferred.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(batches));
        }
        self.report.fallback.fetch_add(1, Ordering::Relaxed);
        self.fallback.lookup(view, ids, columns).await
    }

    fn name(&self) -> &'static str {
        "routed"
    }
}

#[cfg(test)]
mod tests;
