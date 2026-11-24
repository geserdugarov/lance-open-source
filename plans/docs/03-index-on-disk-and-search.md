# Lance — Vector Index On-Disk Format & Query Execution

**Source baseline:** `main` at `67faafe65` (2026-11-16), workspace `15.0.0-beta.22`.

**Scope.** What a committed vector index looks like on disk, how it is
referenced from the manifest, and the exact sequence of calls that happens
when a user runs `scanner.nearest(...)`.

**Audience.** Contributors debugging index loading, query latency, recall,
prefilter behavior, or index lifecycle (rebuilds, compaction, deltas).

---

## 1. The link from manifest to physical index files

At commit time, each trained physical index segment contributes one
`IndexMetadata` entry and writes its payload to `_indices/<segment-uuid>/`. A logical index is
identified by `name` and may contain one or many disjoint segments.

```
  _versions/<version-key>.manifest       <- the committed manifest
  ┌─────────────────────────────────┐
  │ schema                          │
  │ fragments: [...]                │
  │ indices:                        │
  │   ┌──────────────────────────┐  │        _indices/abc-def-…/
  │   │ IndexMetadata {          │  │        ┌─────────────────────────┐
  │   │   uuid: abc-def-…        │──┼───▶    │  <part 0 storage>       │
  │   │   name: "emb_idx"        │  │        │  <part 1 storage>       │
  │   │   fields: [2]            │  │        │  ...                    │
  │   │   fragment_bitmap: 0..8  │  │        │  <IVF + quantizer meta> │
  │   │   dataset_version: 17    │  │        └─────────────────────────┘
  │   │   index_details: Any{…}  │  │
  │   │   files: [IndexFile{..}] │  │
  │   │ }                        │  │
  │   └──────────────────────────┘  │
  │ index_section: <file offset>    │
  └─────────────────────────────────┘
```

The diagram shows one physical segment. Additional entries with
`name = "emb_idx"` point to sibling UUID directories and cover disjoint
fragment subsets.

`IndexMetadata` — `rust/lance-table/src/format/index.rs`:

```rust
pub struct IndexMetadata {
    pub uuid: Uuid,                               // unique forever; never re-used
    pub fields: Vec<i32>,                         // keys, or keys + carried suffix
    pub covering_fields: Vec<i32>,                // suffix or independent carried fields
    pub name: String,                             // human-readable
    pub dataset_version: u64,                     // version this index was built over
    pub fragment_bitmap: Option<RoaringBitmap>,   // fragments covered
    pub index_details: Option<Arc<prost_types::Any>>,  // type-specific proto
    pub index_version: i32,                       // format version of the index itself
    pub created_at: Option<DateTime<Utc>>,        // when the index was built (None for older indices)
    pub base_id: Option<u32>,                     // optional key into Manifest::base_paths
                                                  // (used when index files live outside the dataset root)
    pub files: Option<Vec<IndexFile>>,            // files stored by this segment + sizes
    pub clustering_id: Option<u32>,               // manifest clustering definition, if set
    pub metadata: HashMap<String, String>,        // provider-owned segment metadata
}
```

`covering_fields` describes extra payload dependencies of a covering index;
it is empty for the vector segments discussed here. Use `keyed_fields()` and
`dependency_fields()` to interpret the metadata. A proper trailing subset of
`fields` denotes the suffix layout; an independent declaration keeps every
`fields` entry as a key and may also carry a key's value. The manifest's
`FLAG_INDEPENDENT_COVERING_FIELDS` selects the persisted layout for the whole
index section; transaction files retain the suffix representation.

`IndexFile` includes an optional `file_metadata_size_bytes` hint for opening
the Lance container; its footer remains authoritative. Changes to an existing
segment preserve its clustering ID and arbitrary metadata, including unknown
keys. A newly built segment carries only the metadata its creator assigns.

Two key invariants:

1. **Name vs UUID.** The name identifies the logical index; each UUID identifies
   one immutable physical segment and its `_indices/<uuid>/` directory. Adding,
   merging, or rebuilding payloads creates new UUIDs. Old directories remain
   reachable from older manifests until garbage collection.

2. **Coverage is the union of effective segment coverage.** In the simple
   append-only case this is the union of `fragment_bitmap` values. Deferred
   compaction can leave historical fragment IDs in metadata; the fragment
   reuse index (FRI) translates them before the planner decides coverage.
   Current fragments outside the effective union must be flat-scanned.

A definition-only index is the exception to the trained-payload layout: it
stores the requested configuration without index files or indexed coverage.
Whole-table builds can use this when too few valid vectors exist to train;
search stays on the flat path until optimization materializes the index.

Directory-resolution helpers:

- `Dataset::indices_dir()` → `<dataset_root>/_indices/`
- `Dataset::indice_files_dir(idx)` → the indices **base** directory for that index
  (typically `<dataset_root>/_indices/`, but redirected via `IndexMetadata::base_id`
  when the index lives outside the dataset root). Callers append `<index.uuid>/`
  themselves to reach the segment files. Both helpers live in
  `rust/lance/src/dataset.rs`.

---

## 2. Physical layout of an IVF_PQ (or IVF_HNSW_PQ) index

The files inside each vector segment use the Lance container (the filenames
end in `.idx`) with schema-metadata keys that tell readers how to interpret
their contents.

```
   _indices/<segment-uuid>/
   ├── index.idx
   │    ├─ IVF centroids and partition/sub-index batches
   │    ├─ HNSW graph data for HNSW variants
   │    └─ metadata such as `lance:ivf` and `lance:hnsw`
   └── auxiliary.idx
        ├─ explicit row IDs, partitioned with quantized codes/raw vectors
        ├─ per-partition quantizer metadata (`storage_metadata`)
        └─ quantizer buffers, including PQ codebooks where applicable

   Both files use the Lance container: pages, global buffers, column
   metadata, offset tables, and footer.
```

Ordinary current builders write quantizer metadata through `storage_metadata`.
The creation merger and legacy writers also write the quantizer-specific keys
`lance:pq`, `lance:sq`, and `lance:rabit`; these are not universal keys emitted
by every current build.

For HNSW variants, `index.idx` also carries per-partition graph batches with
the adjacency lists, layers, and entry points. The generic trait that lets IVF
and its sub-indices share this framework is
`IvfSubIndex` in `rust/lance-index/src/vector/v3/subindex.rs`.

Relevant storage modules:

| Subsystem | Path |
|---|---|
| IVF model serialization | `rust/lance-index/src/vector/ivf/storage.rs` |
| PQ storage (codebook + metadata) | `rust/lance-index/src/vector/pq/storage.rs` |
| SQ storage | `rust/lance-index/src/vector/sq/storage.rs` *(parallel)* |
| HNSW storage | `rust/lance-index/src/vector/hnsw/` |
| Shared sub-index trait | `rust/lance-index/src/vector/v3/subindex.rs` |
| Shuffler (partitions rows during build) | `rust/lance-index/src/vector/v3/shuffler.rs` |

Row-id mapping. Index codes are laid out contiguously **per partition** in
the order they were shuffled; each partition carries its own row-id column
so that results can be mapped back to global dataset row IDs.

The index's algorithm/layout version is separate from the Lance container
version. `dataset_format_version` in `rust/lance/src/dataset/index.rs` selects
an exact container format from the dataset's persisted storage policy (with
an explicit V2.0 choice for legacy datasets). A physical merge inherits the
first input auxiliary file's exact format; upgrading the library does not rewrite
existing index files or force every index file to 2.2.

---

## 3. The query execution pipeline

User calls (Python):

```python
results = ds.scanner(
    nearest={
        "column": "embedding",
        "q": q,
        "k": 10,
        "nprobes": 20,
        "refine_factor": 10,
    },
    filter="category = 'cats'",
    prefilter=True,
    limit=10,
).to_table()
```

Single-query, same-column Rust pipeline (execution nodes are in `io/exec/knn.rs`):

```text
Scanner::nearest(col, q, k) -> Query
`-- Scanner::vector_search: resolve index and effective segment coverage
    |
    |-- No usable index: FLAT PATH
    |   `-- Scan -> KNNVectorDistance -> exact top-k (no refinement)
    |
    `-- Usable index: ANN PATH
        |-- ANNIvfPartitionExec: rank each segment's IVF partitions
        |-- ANNIvfSubIndexExec: search selected partitions (flat or HNSW)
        |-- Merge segment candidates by distance
        |-- Optional exact refinement, when refine_factor is set:
        |   `-- take(candidate row IDs) -> raw vectors -> exact top-k
        `-- knn_combined, unless fast_search omits fallback:
            |-- ANN branch results (already refined if requested)
            |-- Flat KNN over uncovered fragments and stale overlay rows
            `-- Take missing raw vectors -> union -> flat KNN rescore -> top-k
                (RQ code-only refinement retains scores; see section 6)

Both paths -> postfilter when prefilter=false -> projected RecordBatch stream
              (prefilter=true applies the predicate before search)
```

The ANN branch fans out across every compatible physical segment, merges
their candidates, optionally refines them, and then merges the flat path for
uncovered fragments and stale rows. By default that merge recomputes exact
distances for the union, even without explicit refinement. There is no
deduplication step; coverage and ownership
masks determine which rows each branch searches. The prefilter and postfilter
cases are alternative plans; prefiltering is not a
late stage after ANN/refinement.

---

## 4. The ANN path in detail

For `IVF_PQ` the ANN branch above expands as follows for each segment
(the sketch assumes the default PQ8; PQ4 uses 16-entry tables and packed
codes):

```
  q = query vector (f32, D-dim)
  IvfModel.centroids : [k × D] f32

  1. Compute dist(q, c_i) for all i in [0, k)     ── SIMD L2/Cosine/Dot
  2. Sort ascending → pick the `nprobes` nearest centroid IDs
  3. For each selected partition p:
       a. Load PQ codes from the segment (cached if hot):
             codes_p : [len_p × M] uint8
       b. Build the 1-to-M distance tables for q against the PQ codebook:
             tbl : [M × 256] f32
             tbl[m][c] = dist(q[sub_m], codebook[m][c])
       c. For each code row v in codes_p:
             approx_dist = Σ_m tbl[m][ v[m] ]
          (tight SIMD loop in dist_table)
       d. Maintain a bounded top-k heap across all probed partitions.
  4. Output the segment's top-k (row_id, approx_dist); merge segment outputs.
```

For `IVF_HNSW_PQ` step 3(c) is replaced by an HNSW graph walk where each
distance computation inside the graph uses the same PQ lookup table.

### Fixed and automatic probing

The sketch above uses explicit `nprobes`. When explicit effort or probe bounds
do not override it, it sets both `Query.minimum_nprobes` and `maximum_nprobes`
to the same value. Without those controls,
the scanner starts with a minimum of 1 and no explicit maximum; centroid
distances select an initial budget, and late search can probe more partitions
when filtering leaves too few results.

`rust/lance/src/io/exec/knn/adaptive_probe.rs` selects a metric-aware
initial budget for L2, cosine, and Dot with `k <= 100000`, across IVF index
types, vector types, and refinement factors. An explicit maximum limits
adaptive probing without disabling it. Hamming and larger `k` retain the
older heuristic. Fixed probe bounds bypass both heuristics.

`LANCE_AUTO_PROBE_MARGIN`, `LANCE_AUTO_MIN_INITIAL_NPROBES`, and
`LANCE_AUTO_MAX_INITIAL_NPROBES` tune that eligible profile. Its cap limits the
initial budget, not subsequent late search. These are empirical latency/recall
controls, not a recall guarantee.

`search_effort` is a finite value in `[0, 1]`: 0 starts at the caller minimum
(at least one available partition), 0.5 preserves Auto, and 1 starts at all
available partitions subject to `maximum_nprobes`. Below 0.5, metric- and
`k`-dependent calibration anchors at 0.1 and 0.25 target 80% and 90% mean
**routing** recall, with interpolation between anchors. Above Auto, the budget
grows geometrically toward all available partitions. These empirical targets
exclude quantization/filtering losses and are not end-to-end ANN guarantees;
the calibration artifacts live in `benchmarks/auto-ivf-effort/`.
The caller minimum takes precedence
over the profile cap; the caller maximum and available partitions bound the
result. Later expansion for too few candidates is unchanged.

Python accepts it in the `nearest` dictionary or `ScannerBuilder.nearest`;
Rust exposes `Scanner::search_effort`. Explicit effort overrides `nprobes`,
including an explicit 0.5. Omitting effort preserves an explicit `nprobes`.
It does not change `ef`, `approx_mode`, or refinement.

### Batch queries

For fixed-size vector columns, Python accepts a 2-D query array in `q` and
returns up to `k` rows per query, tagged with an Int32 `query_index` column.
Eligible queries use `ANNIvfBatchExec`: rank partitions for each query,
group queries that need the same partition, load/search that partition once
for the group, and merge candidates separately for each query.

The shared path requires flat sub-indices (IVF_FLAT/PQ/SQ/RQ), positive
`minimum_nprobes`, no quantized refinement, no external row-address mask, and no
newer overlay rows to reconcile. Fixed probes support exact `refine_factor`.
Adaptive probes share scans for IVF_FLAT/PQ/SQ without exact refinement;
adaptive RQ, adaptive exact refinement, and explicitly parallel adaptive probing
fall back to per-query execution. The batch executor applies the same initial
budget and sequential late-search cutoff; across segments it consumes results
round-robin rather than completion order. Requested
fragments must be covered by the selected segments unless `fast_search`
permits indexed-only search; selecting segments without a fragment restriction
already means searching just those segments. Unsupported shapes use the
per-query path, preserving HNSW, quantized refinement, and
delta/overlay fallback behavior. Shared scans apply segment ownership before
top-k and can score chunks in parallel across queries.

Multivector columns use a separate WARP/XTR plan, not the shared batch-query
path. WARP is selected when all chosen segments support token scoring through
flat IVF sub-indices; HNSW and legacy IVF-PQ retain XTR. WARP probes 32
partitions per token by default and has no late expansion; its
`warp_imputation_rank` controls missing-token score estimation. See
`io/exec/knn/warp.rs` and `02-vector-indices.md`.

---

## 5. Unindexed fragments (the "delta") at query time

The canonical case: you built an index, then appended more data.

```
  Committed manifest (v=N)
  ┌──────────────────────────────┐
  │ fragments:                   │
  │   [0, 1, 2, 3, 4, 5, 6, 7]   │
  │ indices:                     │
  │   { uuid=X,                  │
  │     fragment_bitmap = {0..5} │   ← index covers frags 0–5
  │   }                          │
  └──────────────────────────────┘

  At query time:
      covered_fragments   = {0, 1, 2, 3, 4, 5}   → ANN via every segment
      uncovered_fragments = {6, 7}               → flat scan these
      final top-k         = union(ANN candidates + raw vectors, flat) → exact rescore → top-k
                            (RQ code-only exception: section 6)
```

Code: `rust/lance/src/dataset/scanner/vector.rs`, with `load_indices` declared
in `index/api.rs` and implemented in `index/lifecycle.rs`, raw loading and
unindexed-fragment helpers in `index/loading.rs`, and FRI translation in
`index/frag_reuse_reader.rs` (all under `rust/lance/src/`).
`index/provenance.rs` validates staged segments during lifecycle operations;
it is not the query-time coverage path.

- `Dataset::unindexed_fragments(index_name)` returns the complement of the
  same-name segment coverage union.
- `Scanner::vector_search` branches on whether the merge is needed.
- The ANN plan queries all compatible segments; `knn_combined` fetches missing
  vector values for those candidates, unions them with flat fallback results,
  and runs flat KNN again before top-k truncation. This drops and recomputes
  `_distance`; the code-only RQ exception is described in section 6.

Overlay files introduce a second stale-row case. The format contract requires
rows updated by an overlay newer than a segment's `dataset_version` (and
covering the indexed field) to be excluded from that segment and re-evaluated
with their current values. The current planner does this at row granularity:
it blocks stale addresses from vector ANN and re-scores only those rows through
a targeted `take` plus flat-KNN path. The check is field-aware and version-gated,
so unrelated overlays and overlays already incorporated by the index do not
force fallback work. `IndexVisibility` in `index/visibility.rs` builds this
snapshot-local selection and shares its conversion from physical addresses to
the result row domain between exclusion and replay.

Segment files may also retain stale rows after an in-place column rewrite has
removed their fragments from the segment bitmap. Lance applies each segment's
ownership mask inside partition search, **before** top-k selection, so stale
entries cannot consume candidate slots. Stable row IDs are resolved through
the current fragment row-ID sequences. Physical segment merge and optimization
also filter source rows before copying them into a new segment.

After deferred compaction, use the remapping plan to interpret historical
coverage and row addresses. Lance traces fully covered FRI transitions and
retains merged coverage only while indexed field data still matches. Reading
raw historical bitmaps as current fragment IDs can incorrectly empty an index
or claim stale coverage. The shared reader is in
`rust/lance/src/index/frag_reuse_reader.rs`.

A `fast_search=true` flag skips `knn_combined`, including both uncovered-fragment
and stale-overlay-row fallback. Stale rows are still masked out of ANN, so they
are omitted unless another task explicitly searches them. A fragment-only
fallback in an external adapter does not cover this row-level case.

---

## 6. Exact and quantized refinement

Approximate distances from PQ/SQ/RQ are noisy. `refine_factor` recomputes
distances for ANN candidates. With no delta or overlay fallback:

```
  Without refine:                       With refine (factor = 10):
  ─────────────────                     ──────────────────────────
   ANN → top-k    → return              ANN → top (k·10) candidates
                                        take() → raw vectors from data/
                                        SIMD exact distance
                                        → top-k → return
```

When fallback rows exist, `knn_combined` normally takes any missing ANN vector
values and runs the union through flat KNN. It drops the incoming `_distance`
and recomputes exact distances, making ANN and fallback candidates comparable
before final top-k. This implicit rescore adds raw-vector I/O even when
`refine_factor=None`; explicit refinement, if requested, precedes it. The
no-index flat path already computes exact distances and never refines.

- Implemented in `rust/lance/src/dataset/scanner/vector.rs` during plan building.
- `refine_factor=None` disables explicit ANN refinement, not the implicit
  fallback rescore. A positive factor enables exact re-ranking, including
  factor 1. Typical values are 5–30; zero is rejected.
- Explicit refinement costs a `take(row_ids)` and an exact-distance pass over
  `k·factor` vectors. Fallback can add the separate union rescore described above.

Because Lance is optimized for random access, `take(...)` on `k·10` rows is
typically a small, well-batched sequence of page-level IOVs.

For IVF_RQ flat indices, Rust `Scanner::quantized_refine(factor)` and Python
`nearest={..., "quantized_refine_factor": factor}` offer a separate code-only
rerank. A positive factor expands the one-bit candidate set, then the most
accurate stored RQ representation scores those candidates. It can be combined
with exact refinement, in which case it preserves enough candidates for the
raw-vector stage. Unindexed fragments and stale overlay rows still use flat
fallback and may read original vectors. Distance-range queries use Accurate RQ
search directly so the range applies before top-k truncation. Unsupported index
types and multivector columns are rejected.

**Code-only RQ exception:** with `quantized_refine_factor` set and
`refine_factor=None`, `knn_combined` keeps the ANN quantized scores, converts
flat bounds/scores to the index's distance scale, and sorts the union. It does
not take indexed vectors for an exact rescore. This is an explicit alternative
scoring contract, not the default mixed ANN/fallback path. See
`keep_quantized_distances` in `dataset/scanner/vector.rs`.

**Cross-column refinement:** Python accepts `refine_column`, `refine_q`, and
`refine_metric` together in `nearest`; Rust exposes
`Scanner::with_refine_column`. Supply an explicit positive `refine_factor`;
both columns must contain single Float32 fixed-size vectors. The planner obtains
`k * refine_factor` coarse candidates, including the normal indexed/delta/overlay
merge, then fetches the other column and scores in its vector space. Final
`_distance` and `distance_range` use the refinement metric. `use_index=False`
still performs coarse top-M selection first. Batch queries execute per query;
refinement queries must match their count and the refinement column's dimension.
Query filters are unsupported; scalar prefilter/postfilter semantics remain.

---

## 7. Prefilter vs postfilter

Example query: `WHERE category = 'cats'` combined with vector search.

- **Postfilter** (default, `scanner.prefilter = false`): run ANN first,
  filter the resulting `RecordBatch`. Can lose recall badly if the filter
  is selective and most top-k neighbours get filtered out. Good for
  non-selective filters.
- **Prefilter** (`scanner.prefilter = true`): evaluate the scalar filter
  first (using scalar indices where applicable), combine index matches with
  residual evaluation over uncovered/stale rows in the ANN fragment scope,
  then push the resulting mask into the ANN path so only those IDs are
  considered during partition scanning. More accurate but costs a filter
  pass up front.

On HNSW composites, setting `approx_mode=Fast` uses the ACORN-1 traversal for
this prefiltered graph search. It can reduce filter-aware graph-search latency
at a possible recall cost; `Normal` remains the default.

If a prefilter proves that a segment cannot contain a passing row, execution
skips that segment. Deletion masks are cached by dataset version and combined
with segment ownership; they must not be reused as if they were timeless
properties of an immutable index UUID.

The filter is represented as `Scanner::filter: LanceFilter`. Push-down wiring
happens in the plan builder (`vector_search` + KNN execution nodes), which lowers
`LanceFilter` into an `ExprFilterPlan` before execution.

External callers can also supply a row allow/block mask via
Rust `Scanner::with_row_id_prefilter` or Python's serialized
`row_addr_allowlist` / `row_addr_blocklist` scanner arguments. This mask combines
with scalar predicates and deletion/segment masks before candidate selection;
despite the Python argument names, it uses the dataset's `_rowid` domain:
physical row addresses without stable row IDs, and stable row IDs when that feature is
enabled. Construct the mask from the same dataset snapshot being queried.

Rust retains `with_row_addr_prefilter` as a deprecated alias with those same
`_rowid` semantics. The separate `with_physical_row_addr_prefilter` and
`with_fragment_slices` APIs select physical `_rowaddr` values for scans;
their physical selection path rejects ANN and FTS at this baseline.

---

## 8. Caching

Opening a segment loads its readers and shared metadata. Queries then load and
cache only the IVF partitions selected by `nprobes`; later queries can reuse
those partition entries. Native RaBitQ can also load small resident columns at
open and fetch sign/extended-code planes independently.

```
   GlobalIndexCache                           rust/lance/src/session/
   ┌───────────────────────┐                  index_caches.rs
   │                       │
   │   DSIndexCache(dsURI) │
   │   ┌─────────────────┐ │
   │   │                 │ │
   │   │ Segment cache    │ │
   │   │  keyed by       │ │
   │   │  (idx UUID,     │ │
   │   │   maybe FRI)    │ │
   │   │                 │ │
   │   │ entries:        │ │
   │   │  • IvfModel     │ │
   │   │  • Quantizer    │ │
   │   │  • partition key │ │
   │   │    → graph/codes │ │
   │   └─────────────────┘ │
   └───────────────────────┘
```

Properties:

- The session's `GlobalIndexCache` is namespaced by dataset URI, then segment
  UUID and optional fragment-reuse-index UUID, preventing cross-dataset or
  cross-segment collisions. Native RaBitQ raw payloads use a physical cache
  identity independent of FRI; translated row views remain tied to the current
  remapping plan, so compaction can reuse codes without reusing stale addresses.
- `IVFIndex::load_partition` lazily inserts the requested partition's
  sub-index and storage into the segment cache.
- `Dataset::prewarm_index(name)` opens **all** same-name segments and loads all
  of their partitions. Tests assert that a subsequent query performs no index
  I/O, including when the logical index has multiple delta segments.
- Prewarming groups adjacent partitions into parallel read windows with
  a default encoded-byte target of 64 MiB, configurable through
  `LANCE_IVF_PREWARM_WINDOW_SIZE_BYTES`. This is an I/O target, not a decoded
  memory limit; cache capacity still determines which partitions remain warm.
- `Dataset::prewarm_index_segments(name, segment_ids)` (or Python's
  `prewarm_index(..., index_segments=...)`) can warm only selected physical
  segments of a logical index.
- Per-query `ScanStatistics.index_cache_hits` and `index_cache_misses` expose
  cache reuse without inferring it from total I/O. A path that performs no
  instrumented index-cache lookup reports zero for both counters.
- ANN stage timings include index opening, partition loading/preparation,
  prefilter/CPU-queue waits, search, query preparation, distance/top-k, and
  result materialization (`IndexTiming` in `rust/lance-index-core/src/metrics.rs`).
  They accumulate wall time across nested/concurrent stages and must not be
  summed as query latency. Segment ownership masks are cached separately
  from partition payloads.
- Entries remain subject to the configured cache backend and capacity.

Query-visible index listings have their own memory-only cache keyed by store,
snapshot version, and manifest ETag. It caches inferred details and effective
coverage separately from raw manifest metadata; incomplete inference can retry.
Eligible unfiltered, fully covered quantized-refinement queries also cache
partition rankings within the snapshot. Those keys include query/options,
physical segment identity, and manifest generation (transaction identity or
ETag), so table recreation and version changes cannot reuse old rankings.
Filtered, externally masked, or explicitly scoped queries bypass that result
cache. These caches are distinct from reusable immutable RQ code payloads.

Rust and Python sessions expose `set_index_cache_capacity` and
`set_metadata_cache_capacity` for runtime resizing when supported by the
backend. Capacity is a cache budget, not a total process-memory limit.

The Rust session can install a `RabitScanFactory` for native RaBitQ through
`Session::with_rabit_scan_factory`. Strategies choose scheduling and partition
or plane cache granularity; the shared context retains query preparation,
filtering, and scoring semantics. Controlled partition streams preserve probe
order, candidate budgets, and stop/cancellation behavior. These are runtime
policies, not persisted index-format fields. Native residency and scan
diagnostics are exposed separately from ordinary query cache hit/miss counters.

For distributed inspection, `DatasetIndexExt::index_segment_statistics` returns
per-UUID payloads in request order. `index_statistics_from_segments` aggregates
exactly one payload per current segment from the same dataset version, without
reopening those segments. Coverage comes from manifest metadata; stale or
incomplete result sets are rejected. The implementation lives in
`rust/lance/src/index/statistics.rs`.

---

## 9. Concurrent index rebuilds & compaction

```
  Manifest(v=17)                    Manifest(v=18)
  ┌──────────────────────┐          ┌──────────────────────┐
  │ indices:             │          │ indices:             │
  │   { uuid=OLD, … }    │          │   { uuid=NEW, … }    │
  └──────────────────────┘          └──────────────────────┘

  _indices/OLD/...                  _indices/OLD/...       ← still exists
                                    _indices/NEW/...       ← added atomically

  Readers on v=17 → keep using OLD.
  Readers on v=18 → use NEW.
  GC eventually reclaims OLD once no referencing version remains.
```

- Commits are atomic on the manifest pointer — a new version does not
  invalidate readers on the old one.
- Every rewritten physical payload gets a **new UUID**. A full rebuild can
  replace the logical index's segment set, while append optimization can add
  a same-name delta segment and retain existing coverage. Queries fan out over
  whichever set the manifest records.
- `OptimizeOptions::append()` is segment-set-native: it builds only the
  unindexed fragments with the complete model of the deterministic last
  manifest segment and preserves older, query-compatible segments even when
  their models differ. An explicit merge validates only its selected suffix;
  explicit retrain is the operation that source-rebuilds all current coverage
  and unifies the model.
- Vector segments can be copied into one merged store only when they share
  compatible IVF and quantizer models. The creation merger checks IVF/PQ/RQ
  mismatches and SQ dimension, bit width, and learned bounds. Independently
  trained SQ segments generally stay separate because workers have no shared
  bounds input. Optimization also compares complete quantizer metadata, including SQ
  bounds (see `05-distributed-vector-index-creation.md` §3 and §9).
- Default optimization can adjust IVF partitions using the persisted
  `target_partition_size` (or the index-type default). Lance splits oversized
  partitions into multiple pieces toward that target, or joins undersized
  partitions when no split is performed. A pass that changes centroids
  consolidates the participating segments and re-encodes affected rows.
  `OptimizeOptions::append()` bypasses partition adjustment. Explicit merge
  counts do so by default, but Rust's `with_partition_rebalancing()` enables
  split/join within that selected suffix. It rejects a zero merge count and
  cannot be combined with distributed task/segment row bounds.
- Compaction of data fragments requires index remapping. The
  *fragment reuse index* (optional auxiliary index) accelerates this by
  tracking where each old row ended up after compaction. It also supports
  stable-row-ID tables; address-domain indices may still need eager remapping.
- Rust exposes `plan_optimize_indices`, task execution, merge execution, and
  `commit_optimize_indices` in `rust/lance/src/index/optimize.rs`. Bounded
  builds retain one reference model; retraining may train that model during
  planning. This optimizer uses a different vector merge path from
  `merge_existing_index_segments`, whose PQ/RQ inputs still have intermediate
  layout restrictions. See `05-distributed-vector-index-creation.md` §9.
- Python exposes the same stages through `lance.optimize.OptimizeIndices`,
  and Java through `org.lance.index.optimize.OptimizeIndices`. All three expose
  bounded consolidation (`max_rows_to_consolidate`, Rust
  `OptimizeOptions::consolidate`) for a suffix of existing vector
  segments: shared models merge directly, while differing models re-encode
  under the newest selected model without retraining. This mode requires
  complete selected coverage at commit and leaves unindexed fragments alone.
  Pass the planning options to commit in every binding. Rust also offers
  `consolidate_selected` for arbitrary UUIDs of one index: it re-encodes selected
  rows, drops deletions, and trains on those rows when models differ. That mode
  can split outputs by `max_rows_per_segment`; it is not bounded suffix consolidation.

---

## 10. A minimal debug recipe

If recall is low:

1. Is the right logical index being used? Inspect every same-name segment's
   effective coverage after any FRI translation. Uncovered fragments
   use the flat fallback.
2. `nprobes` too low? Start at 20, then sweep.
3. `ef` (for HNSW variants) too low? The default is
   `max(256, candidates + candidates/2)`, with `candidates = k * refine_factor`
   and an omitted factor treated as 1; sweep upward.
4. Add `refine_factor=10–30`. If recall jumps, the approximate distance is
   the bottleneck.
5. Check the requested distance metric. If no index on the column matches,
   Lance warns and falls back to brute-force search with the requested metric;
   it does not search a mismatched index. Confirm the metric matches the
   application's intended notion of similarity.

If latency is high:

1. Cold partitions? Re-run the same probe set or use `prewarm_index` when a
   fully warm measurement is required.
2. Prefilter with a very non-selective predicate is a tax — consider
   postfilter.
3. Too many fragments → many per-fragment scan tasks. Consider compaction.
4. Check `_versions/` size. Manifest reads become measurable if a dataset
   has thousands of versions without GC.

---

## 11. Quick reference — files to know

| Concern | Path |
|---|---|
| Scanner state / builder (`nearest`) | `rust/lance/src/dataset/scanner.rs`, `rust/lance/src/dataset/scanner/builder.rs` |
| Vector-search planner | `rust/lance/src/dataset/scanner/vector.rs` (`vector_search`) |
| Effective index listing | `rust/lance/src/index/api.rs` (`load_indices` declaration), `rust/lance/src/index/lifecycle.rs` (implementation) |
| Raw loading / opening | `rust/lance/src/index/loading.rs` (`open_vector_index`) |
| Segment freshness / lifecycle | `rust/lance/src/index/provenance.rs`, `rust/lance/src/index/lifecycle.rs` |
| Snapshot-local overlay exclusion / replay | `rust/lance/src/index/visibility.rs` |
| Segment statistics / cache diagnostics | `rust/lance/src/index/statistics.rs`, `rust/lance/src/index/prewarm.rs` |
| Index caching | `rust/lance/src/session/index_caches.rs` |
| ANN stage metrics | `rust/lance-index-core/src/metrics.rs` |
| FRI coverage / row translation | `rust/lance/src/index/frag_reuse_reader.rs` |
| Distributed optimization | `rust/lance/src/index/optimize.rs` |
| KNN exec nodes | `rust/lance/src/io/exec/knn.rs` |
| WARP multivector execution | `rust/lance/src/io/exec/knn/warp.rs` |
| Automatic IVF probe policy | `rust/lance/src/io/exec/knn/adaptive_probe.rs` |
| Shared batch partition search / prewarm | `rust/lance/src/index/vector/ivf/v2.rs` |
| Native RaBitQ runtime scan policies | `rust/lance/src/index/vector/ivf/v2/rabit_scan.rs` |
| Exact index-file format policy | `rust/lance/src/dataset/index.rs`, `rust/lance/src/dataset/versions/mod.rs` |
| Dataset directories | `rust/lance/src/dataset.rs` (`INDICES_DIR`, `indice_files_dir`) |
| `IndexMetadata` | `rust/lance-table/src/format/index.rs` |
| `Manifest` | `rust/lance-table/src/format/manifest.rs` |
| IVF on-disk meta keys | `rust/lance-index/src/vector/ivf/storage.rs` |
| PQ on-disk meta | `rust/lance-index/src/vector/pq/storage.rs` |
| Shuffler (build time) | `rust/lance-index/src/vector/v3/shuffler.rs` |
| Sub-index trait | `rust/lance-index/src/vector/v3/subindex.rs` |
| Unindexed-fragment helpers | `rust/lance/src/index/loading.rs` + scanner callsites |

These are `67faafe65` paths. The upstream crate-architecture proposal (not
implemented) would move scan planning into an operations layer and index
implementations behind an index API whose query capabilities take the
prefilter as an explicit argument, instead of the scanner wiring in §7. Public
`lance` items are to stay through re-exports; re-check file locations after
any such move ([07](07-ecosystem-evidence.md#upstream-crate-architecture-proposal)).

---

This concludes the storage-and-search core of the fourteen-document set
(00–13). The six vector-focused guides are:

1. **`00-overview.md`** — dataset layout + crate layering + lifecycle
2. **`01-vector-storage.md`** — how embedding columns are encoded
3. **`02-vector-indices.md`** — the index algorithms themselves
4. **`03-index-on-disk-and-search.md`** *(this file)* — storage, query, ops
5. **`04-lance-versions.md`** — library releases versus file-format versions
6. **`05-distributed-vector-index-creation.md`** — distributed build protocol

Chapters 06–13 cover distributed architecture and integration proposals,
starting with [the current-state audit](06-distributed-current-state.md).
