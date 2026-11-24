//! The Quack arm of [`IdLookup`].
//!
//! The base dataset's `id` column is read once per dataset version into two
//! parallel `u64` lanes: a 64-bit key per id, and the row id Lance assigned
//! the row. A lookup is a Quack filter over the key lane, one ternary match
//! per requested id joined by OR, lowered to a mask-risc program whose
//! terminal keeps the surviving rows as a mask. The set bits of that mask
//! index the row-id lane, and the rows are fetched with `take_rows`.
//!
//! The key is a hash, so a fetched row is kept only if its id string is one
//! that was requested. A collision costs one extra row read and never an
//! extra row in the result.
//!
//! The lane is built from the base dataset only. A view with pending
//! write-ahead-log generations is not covered: [`QuackIdLookup::lookup`]
//! returns `Ok(None)` and a wrapper decides what answers instead.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use arrow_array::{Array, BooleanArray, LargeStringArray, RecordBatch, StringArray, UInt64Array};
use arrow_select::filter::filter_record_batch;
use async_trait::async_trait;
use futures::TryStreamExt;
use lance::{Dataset, Error as LanceError, Result as LanceResult};
use lance_context_api::schema_spec::ID_COLUMN;
use lance_graph_mask_risc::{
    execute_into, words_for, Foreign, LaneRef, Out, Planes, Scratch, Value,
};
use lance_graph_quack::{lower, Agg, Cmp, Col, Filter, Query};

use super::{IdLookup, IdLookupView};

const ROW_ID: &str = "_rowid";
const KEY_LANE: Col = Col(0);

/// The 64-bit key of an id. `DefaultHasher::new()` uses fixed keys, so the
/// value is the same for the life of the process, which is the life of a lane.
fn key_of(id: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    id.hash(&mut hasher);
    hasher.finish()
}

/// The two lanes for one dataset version.
#[derive(Debug)]
struct IdLane {
    uri: String,
    version: u64,
    keys: Vec<u64>,
    row_ids: Vec<u64>,
}

impl IdLane {
    async fn build(dataset: &Dataset) -> LanceResult<Self> {
        let mut scanner = dataset.scan();
        scanner.project(&[ID_COLUMN])?.with_row_id();
        let mut stream = scanner.try_into_stream().await?;
        let mut keys = Vec::new();
        let mut row_ids = Vec::new();
        while let Some(batch) = stream.try_next().await? {
            let ids = batch.column_by_name(ID_COLUMN).ok_or_else(|| {
                LanceError::invalid_input(format!("scan has no `{ID_COLUMN}` column"))
            })?;
            let rows = batch
                .column_by_name(ROW_ID)
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| LanceError::invalid_input("scan has no `_rowid` column"))?;
            push_keys(ids.as_ref(), &mut keys)?;
            row_ids.extend(rows.values().iter().copied());
        }
        if keys.len() != row_ids.len() {
            return Err(LanceError::invalid_input(
                "id lane and row-id lane differ in length",
            ));
        }
        Ok(Self {
            uri: dataset.uri().to_string(),
            version: dataset.version().version,
            keys,
            row_ids,
        })
    }

    fn matches(&self, dataset: &Dataset) -> bool {
        self.version == dataset.version().version && self.uri == dataset.uri()
    }

    /// Row ids whose key equals the key of one of `ids`, in lane order.
    fn candidates(&self, ids: &[&str]) -> LanceResult<Vec<u64>> {
        let keys: Vec<u64> = ids.iter().map(|id| key_of(id)).collect();
        self.candidates_for_keys(&keys)
    }

    /// Row ids whose key is one of `keys`, in lane order.
    fn candidates_for_keys(&self, keys: &[u64]) -> LanceResult<Vec<u64>> {
        let n = self.keys.len();
        if n == 0 || keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut seen = HashSet::new();
        let legs: Vec<Filter> = keys
            .iter()
            .copied()
            .filter(|key| seen.insert(*key))
            .map(|key| {
                Filter::cmp(
                    KEY_LANE,
                    Cmp::MatchU64 {
                        pattern: key,
                        care: u64::MAX,
                    },
                )
            })
            .collect();
        let filter = if legs.len() == 1 {
            legs.into_iter().next().expect("one leg")
        } else {
            Filter::or(legs)
        };
        let program = lower(&Query {
            filter,
            agg: Agg::Rows,
        })
        .map_err(|e| LanceError::invalid_input(format!("quack lowering failed: {e:?}")))?;

        let lanes = [LaneRef::U64(&self.keys)];
        let masks: [&[u64]; 0] = [];
        let planes = Planes {
            n_rows: n,
            masks: &masks,
            lanes: &lanes,
        };
        let mut scratch = Scratch::for_program(&program, n)
            .map_err(|e| LanceError::invalid_input(format!("quack scratch failed: {e:?}")))?;
        let mut kept = vec![0u64; words_for(n)];
        let value = execute_into(
            &program,
            &planes,
            &Foreign::NONE,
            &mut scratch,
            Out::Mask(&mut kept),
        )
        .map_err(|e| LanceError::invalid_input(format!("quack execution failed: {e:?}")))?;
        if !matches!(value, Value::Mask(_)) {
            return Err(LanceError::invalid_input(format!(
                "quack returned {value:?} for a row query"
            )));
        }

        let mut out = Vec::new();
        for (word_index, &word) in kept.iter().enumerate() {
            let mut bits = word;
            while bits != 0 {
                let row = word_index * 64 + bits.trailing_zeros() as usize;
                bits &= bits - 1;
                if row < n {
                    out.push(self.row_ids[row]);
                }
            }
        }
        Ok(out)
    }
}

fn push_keys(ids: &dyn Array, keys: &mut Vec<u64>) -> LanceResult<()> {
    if let Some(a) = ids.as_any().downcast_ref::<StringArray>() {
        keys.extend((0..a.len()).map(|i| key_of(a.value(i))));
    } else if let Some(a) = ids.as_any().downcast_ref::<LargeStringArray>() {
        keys.extend((0..a.len()).map(|i| key_of(a.value(i))));
    } else {
        return Err(LanceError::invalid_input(format!(
            "`{ID_COLUMN}` is not a string column"
        )));
    }
    Ok(())
}

/// Keep the rows of `batch` whose id is in `wanted`.
fn keep_wanted(batch: &RecordBatch, wanted: &HashSet<&str>) -> LanceResult<RecordBatch> {
    let column = batch.column_by_name(ID_COLUMN).ok_or_else(|| {
        LanceError::invalid_input(format!("take result has no `{ID_COLUMN}` column"))
    })?;
    let keep: BooleanArray = if let Some(a) = column.as_any().downcast_ref::<StringArray>() {
        (0..a.len())
            .map(|i| Some(a.is_valid(i) && wanted.contains(a.value(i))))
            .collect()
    } else if let Some(a) = column.as_any().downcast_ref::<LargeStringArray>() {
        (0..a.len())
            .map(|i| Some(a.is_valid(i) && wanted.contains(a.value(i))))
            .collect()
    } else {
        return Err(LanceError::invalid_input(format!(
            "`{ID_COLUMN}` is not a string column"
        )));
    };
    Ok(filter_record_batch(batch, &keep)?)
}

/// Id lookup through a Quack program over a per-version key lane.
///
/// Covers views with no pending write-ahead-log generations. The lane for
/// the most recent dataset version is cached; a different version rebuilds
/// it.
#[derive(Debug, Default)]
pub struct QuackIdLookup {
    /// The newest lane built per dataset URI.
    lanes: Mutex<HashMap<String, Arc<IdLane>>>,
}

/// Lanes kept at once. A lane for a URI beyond this evicts an arbitrary one.
const MAX_LANES: usize = 64;

impl QuackIdLookup {
    /// An arm with an empty lane cache.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    async fn lane_for(&self, dataset: &Dataset) -> LanceResult<Arc<IdLane>> {
        {
            let cached = self.lanes.lock().expect("lane cache lock");
            if let Some(lane) = cached
                .get(dataset.uri())
                .filter(|lane| lane.matches(dataset))
            {
                return Ok(lane.clone());
            }
        }
        let lane = Arc::new(IdLane::build(dataset).await?);
        let mut cached = self.lanes.lock().expect("lane cache lock");
        if cached.len() >= MAX_LANES && !cached.contains_key(dataset.uri()) {
            if let Some(evict) = cached.keys().next().cloned() {
                cached.remove(&evict);
            }
        }
        cached.insert(dataset.uri().to_string(), lane.clone());
        Ok(lane)
    }
}

#[async_trait]
impl IdLookup for QuackIdLookup {
    async fn lookup(
        &self,
        view: IdLookupView<'_>,
        ids: &[&str],
        columns: &[String],
    ) -> LanceResult<Option<Vec<RecordBatch>>> {
        if view.lsm && !view.is_base_only() {
            return Ok(None);
        }
        let lane = self.lane_for(view.dataset).await?;
        let row_ids = lane.candidates(ids)?;
        if row_ids.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let refs: Vec<&str> = columns.iter().map(String::as_str).collect();
        let projection = view.dataset.schema().project(&refs)?;
        let batch = view.dataset.take_rows(&row_ids, projection).await?;
        let wanted: HashSet<&str> = ids.iter().copied().collect();
        let kept = keep_wanted(&batch, &wanted)?;
        // Callers read by position. Lance's projection already keeps the
        // requested order (measured: removing this line fails no test); this
        // states the contract instead of relying on that.
        Ok(Some(vec![super::project_in_order(&kept, &refs)?]))
    }

    fn name(&self) -> &'static str {
        "quack"
    }
}

#[cfg(test)]
pub(crate) mod tests_support {
    use std::sync::Arc;

    use arrow_array::{Float32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    pub(crate) fn lane_candidates_for_test(keys: &[u64], row_ids: &[u64], key: u64) -> Vec<u64> {
        let lane = IdLane {
            uri: String::new(),
            version: 0,
            keys: keys.to_vec(),
            row_ids: row_ids.to_vec(),
        };
        lane.candidates_for_keys(&[key]).unwrap()
    }

    pub(crate) fn keep_wanted_for_test(ids: &[&str], wanted: &[&str]) -> Vec<String> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(ID_COLUMN, DataType::Utf8, false),
            Field::new("score", DataType::Float32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(Float32Array::from(vec![0.0; ids.len()])),
            ],
        )
        .unwrap();
        let wanted: HashSet<&str> = wanted.iter().copied().collect();
        let kept = keep_wanted(&batch, &wanted).unwrap();
        let column = kept
            .column_by_name(ID_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        (0..column.len())
            .map(|i| column.value(i).to_string())
            .collect()
    }
}
