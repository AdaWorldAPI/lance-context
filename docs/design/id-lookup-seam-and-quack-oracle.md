# Id lookup seam and a second lookup engine as oracle

Status: proposal. Nothing here is implemented or measured.

## Summary

Almost every use of DataFusion in `lance-context-core` is a lookup by `id`
(`id = x` or `id IN (...)`). This proposal puts those lookups behind one small
trait, `IdLookup`, whose default implementation is today's Lance scanner filter,
so behaviour does not change. A second implementation, living outside this
repository, can then answer the same lookups from a different engine and be
compared against the default on a pinned dataset version.

The second engine is lance-graph's Quack over an id lane. The plan on that side
is `ontology-jit-compile-v1.md` §10, §10a and §10b in
`AdaWorldAPI/lance-graph` (merged in lance-graph #2163 and #2164). This document
is the lance-context half and can be read without it.

## Where DataFusion is used today

Read at `5adf73f`.

| Role | Where | In this proposal |
|---|---|---|
| Lookup by id | `generic_store.rs` 373, 423, 636; `rollout_append.rs` 868; `annotation_staging.rs` 712, 777; `document_store.rs` 204, 229, 246 | routed through `IdLookup` |
| Caller-supplied filter | `generic_store.rs` 561, 876-888; `rollout_store.rs` 1313, 1376 | unchanged |
| Merge-key read (`ScalarValue`) | `store_base.rs` | unchanged; it is not a query |
| SQL over rollouts (`SessionContext`, `MemTable`, `SortExec`, `GlobalLimitExec`, SQL parser) | `rollout_store.rs`, `query_sql()` at 1587 | unchanged |
| Stream adapter types | `document_store/query.rs` | unchanged |
| Ingest materializer | `lance-context-ingest`, optional feature `materializer` | unchanged |

DataFusion stays a dependency. Lance itself depends on it and re-exports it
(`lance::deps::datafusion`), so removing it from this crate's manifest would not
remove it from a build. The aim is narrower: id lookups stop being tied to one
engine.

## The seam

```rust
/// Resolves record ids to the rows that hold them, at one dataset version.
pub trait IdLookup: Send + Sync {
    /// The rows for `ids` in `dataset`. An id with no row is absent from the
    /// result. Order is not specified; callers that need the input order
    /// re-order by id, as `get_many` does today.
    async fn lookup(&self, dataset: &Dataset, ids: &[&str], columns: &[String])
        -> LanceResult<Vec<RecordBatch>>;
}
```

- The default, `ScannerIdLookup`, is the code at the call sites above moved into
  one place: `scan().project(columns).filter_expr(col("id").in_list(...))`,
  and the single-id form for one id.
- Stores take an `Arc<dyn IdLookup>` through their options, defaulting to
  `ScannerIdLookup`. No public signature of a store method changes.
- The trait takes the `Dataset` it must read. An implementation never opens or
  pins a dataset itself, so it cannot answer from a different version than the
  caller is reading.

The exact signature is open (see Open questions); the constraint is that every
id-lookup call site listed above goes through it and nothing else does.

## A second implementation as oracle

A second `IdLookup` is supplied from outside this repository. It keeps its own
table of `id -> row address` for a dataset version, resolves ids there, and
takes the rows from Lance by address.

Run in three phases:

1. The scanner answers. The second engine answers the same call and the two
   results are compared as sets of ids. A difference is recorded with the ids
   and the dataset version. Callers see the scanner's result.
2. After agreement has held for a stated number of lookups and versions, the
   second engine answers and the scanner is the shadow.
3. The scanner arm is kept as a test oracle only.

What makes the comparison fair:

- Both arms read the same `Dataset` value, so the same version.
- Reads that go through the write-ahead log (`read_consistent`,
  `ListSource`) see rows that are not in the base dataset yet. The comparison
  either runs on base-dataset lookups only, or the second engine's table covers
  the unmerged generations too. Otherwise the oracle reports differences that
  are only freshness.
- Results are compared as sets. Neither arm promises an order.

A wrapper, `ComparingIdLookup { primary, shadow, report }`, belongs in this
repository so the comparison logic is tested here; the second engine does not.

## What the second engine needs from Lance

- **Row addresses that survive compaction.** Tests in this crate already create
  datasets with `enable_stable_row_ids: true`
  (`annotation_staging/tests/`). A table keyed by row address is only valid
  across versions when the store was created that way; otherwise it is rebuilt
  per version. Which stores are created with stable row ids outside tests was
  not checked.
- **Take by address.** Rows are fetched with Lance's take path. No scanner
  filter is involved on that arm.

## Not in scope

- Replacing caller-supplied filters or `query_sql()`.
- Removing the DataFusion dependency.
- Any dependency from this repository on lance-graph. The second engine
  implements a trait defined here; nothing here imports it.

## Tests

- With `ScannerIdLookup`, the existing suite passes unchanged.
- Every id-lookup call site goes through the trait: a counting `IdLookup` sees
  one call per `get`, `get_many` and the listed staging and document reads.
- `ComparingIdLookup` reports a planted difference (a shadow that drops one id)
  and reports nothing when the two arms agree.
- A lookup of rows that are only in the write-ahead log produces no false
  difference under the chosen fairness rule.

## Open questions

1. Whether the trait returns record batches, as above, or row addresses with
   the take left to the caller. Addresses are the smaller contract; batches
   keep today's call sites almost unchanged.
2. How long agreement must hold before phase 2.
3. Whether this is offered to `lance-format/lance-context` upstream or kept on
   this fork.
