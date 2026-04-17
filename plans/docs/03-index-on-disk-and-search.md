# Lance — Vector Index On-Disk Format & Query Execution

**Source baseline:** Lance `12.0.0`, commit `cbeec97cb` (2026-09-17).

**Scope.** What a committed vector index looks like on disk, how it is
referenced from the manifest, and the exact sequence of calls that happens
when a user runs `scanner.nearest(...)`.

**Audience.** Contributors debugging index loading, query latency, recall,
prefilter behavior, or index lifecycle (rebuilds, compaction, deltas).

---

## 1. The link from manifest to physical index files

At commit time, each physical index segment contributes one `IndexMetadata`
entry and writes its payload to `_indices/<segment-uuid>/`. A logical index is
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
    pub fields: Vec<i32>,                         // keyed fields, then carried fields
    pub covering_fields: Vec<i32>,                // carried-only suffix of fields
    pub name: String,                             // human-readable
    pub dataset_version: u64,                     // version this index was built over
    pub fragment_bitmap: Option<RoaringBitmap>,   // fragments covered
    pub index_details: Option<Arc<prost_types::Any>>,  // type-specific proto
    pub index_version: i32,                       // format version of the index itself
    pub created_at: Option<DateTime<Utc>>,        // when the index was built (None for older indices)
    pub base_id: Option<u32>,                     // optional key into Manifest::base_paths
                                                  // (used when index files live outside the dataset root)
    pub files: Option<Vec<IndexFile>>,            // files stored by this segment + sizes
}
```

`covering_fields` describes extra payload dependencies of a covering index;
it is empty for the vector segments discussed here. Consumers that need the
search key must distinguish it from the carried suffix of `fields`.

Two key invariants:

1. **Name vs UUID.** The name identifies the logical index; each UUID identifies
   one immutable physical segment and its `_indices/<uuid>/` directory. Adding,
   merging, or rebuilding payloads creates new UUIDs. Old directories remain
   reachable from older manifests until garbage collection.

2. **Coverage is the union of segment bitmaps.** Each segment's
   `fragment_bitmap` is the source of truth for that segment. The union across
   compatible same-name segments is indexed; current fragments outside that
   union are uncovered and must be flat-scanned.

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
        ├─ storage metadata (`storage_metadata`)
        └─ quantizer metadata/codebooks (`lance:pq`, `lance:sq`,
           or `lance:rabit`)

   Both files use the Lance container: pages, global buffers, column
   metadata, offset tables, and footer.
```

For HNSW variants, `index.idx` also carries per-partition graph batches with
the adjacency lists, layers, and entry points. The generic trait that lets IVF
and its sub-indexes share this framework is
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
first input auxiliary file's exact format; upgrading to v12 does not rewrite
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

End-to-end Rust pipeline:

```
┌──────────────────────────────────────────────────────────────────────────┐
│ 1. Scanner builder                                                       │
│    rust/lance/src/dataset/scanner.rs :: Scanner::nearest(col, q, k)      │
│        stores Query{ column, key, k, minimum_nprobes,                   │
│                      maximum_nprobes, refine_factor, ... }             │
└──────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ 2. Plan                                                                  │
│    Scanner::vector_search(filter_plan, query)                            │
│      • resolve the named logical index and all compatible segments        │
│      • open each UUID segment (cached) → deserialize IVF + Q             │
│      • decide routing:                                                   │
│          no index or all-unindexed fragments → FLAT PATH                 │
│          index present                       → ANN PATH (+ delta merge) │
└──────────────────────────────────────────────────────────────────────────┘
                                 │
         ┌───────────────────────┴───────────────────────┐
         ▼                                               ▼
┌─────────────────────┐                         ┌──────────────────────────┐
│ FLAT PATH           │                         │ ANN PATH                 │
│ io/exec/knn.rs      │                         │ io/exec/knn.rs           │
│  KNNVectorDistance  │                         │  ANNIvfSubIndexExec      │
│                     │                         │    a. compare q vs IVF   │
│  brute-force scan   │                         │       centroids          │
│  over fragments     │                         │    b. top-`nprobes` parts│
│  using SIMD kernels │                         │    c. per partition:     │
│                     │                         │       load codes + graph │
│                     │                         │       walk (HNSW) or scan│
│                     │                         │       (flat) + dist table│
│                     │                         │    d. top-k per segment  │
└──────────┬──────────┘                         └─────────────┬────────────┘
           │                                                   │
           │                                                   ▼
           │                                    ┌──────────────────────────┐
           │                                    │ 3. MERGE DELTA           │
           │                                    │    scanner.rs :: knn_    │
           │                                    │    combined(...)         │
           │                                    │    if any fragment NOT   │
           │                                    │    in bitmap union:      │
           │                                    │      flat-scan those     │
           │                                    │      union w/ ANN top-k  │
           │                                    └─────────────┬────────────┘
           │                                                   │
           └────────────────────┬──────────────────────────────┘
                                │
                                ▼
           ┌────────────────────────────────────────────────┐
           │ 4. REFINE (optional; refine_factor is set)      │
           │    fetch top (k × factor) approximate hits     │
           │    take(row_ids) → load RAW vectors from data/ │
           │    recompute EXACT distances with SIMD kernel  │
           │    keep top-k                                  │
           └─────────────────────┬──────────────────────────┘
                                 │
                                 ▼
           ┌────────────────────────────────────────────────┐
           │ 5. FILTER BRANCHES                              │
           │    prefilter=true ran the scalar predicate      │
           │    before ANN and supplied its row-ID mask;     │
           │    prefilter=false filters ANN output here.     │
           └─────────────────────┬──────────────────────────┘
                                 │
                                 ▼
           ┌────────────────────────────────────────────────┐
           │ 6. RESULT                                      │
           │    RecordBatch stream with original columns    │
           │    + synthetic `_distance` column              │
           └────────────────────────────────────────────────┘
```

The ANN branch fans out across every compatible physical segment, merges
their candidates, and then merges the flat path for uncovered fragments. The
prefilter and postfilter cases are alternative plans; prefiltering is not a
late stage after ANN/refinement.

---

## 4. The ANN path in detail

For `IVF_PQ` the ANN phase inside step 2 expands as follows for each segment
(the sketch assumes the default PQ8; PQ4 uses 16-entry tables and packed
codes):

```
  q = query vector (f32, D-dim)
  IvfModel.centroids : [k × D] f32

  1. Compute dist(q, c_i) for all i in [0, k)     ── SIMD L2/Cosine/Dot
  2. Sort ascending → pick top `nprobes` centroid IDs: P1…Pn
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

The sketch above uses explicit `nprobes`. It sets both
`Query.minimum_nprobes` and `maximum_nprobes` to the same value. Without it,
the scanner starts with a minimum of 1 and no explicit maximum; centroid
distances select an initial budget, and late search can probe more partitions
when filtering leaves too few results.

In v12, `rust/lance/src/io/exec/knn/adaptive_probe.rs` selects a metric-aware
initial budget for current IVF_FLAT indices with finite Float32 queries,
L2/cosine distance, `k <= 100`, no explicit maximum, and no refinement factor
greater than 1. Other index types, Dot/Hamming, and explicitly bounded automatic
queries retain the existing heuristic. Fixed `nprobes` bypasses both heuristics.

`LANCE_AUTO_PROBE_MARGIN`, `LANCE_AUTO_MIN_INITIAL_NPROBES`, and
`LANCE_AUTO_MAX_INITIAL_NPROBES` tune that eligible profile. Its cap limits the
initial budget, not subsequent late search. These are empirical latency/recall
controls, not a recall guarantee.

### Batch queries

For fixed-size vector columns, Python accepts a 2-D query array in `q` and
returns up to `k` rows per query, tagged with an Int32 `query_index` column.
Eligible v12 queries use `ANNIvfBatchExec`: rank partitions for each query,
group queries that need the same partition, load/search that partition once
for the group, and merge candidates separately for each query.

The shared path requires flat sub-indices (IVF_FLAT/PQ/SQ/RQ), fixed positive
`nprobes`, no `refine_factor`, no external row-address mask, and no newer
overlay rows to reconcile. Requested fragments must be covered by the selected
segments unless `fast_search` permits indexed-only search. Unsupported shapes
use the per-query path, preserving adaptive probing, HNSW, refinement, and
delta/overlay fallback behavior.

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
      final top-k         = merge(ANN, flat) → dedup → sort → truncate
```

Code: `rust/lance/src/dataset/scanner.rs`:

- `Dataset::unindexed_fragments(index_name)` returns the complement of the
  same-name segment coverage union.
- `Scanner::vector_search` branches on whether the merge is needed.
- The ANN plan queries all compatible segments; `knn_combined` unions those
  candidates with a flat KNN plan over the delta before top-k truncation.

Overlay files introduce a second stale-row case. The format contract requires
rows updated by an overlay newer than a segment's `dataset_version` (and
covering the indexed field) to be excluded from that segment and re-evaluated
with their current values. The current planner does this at row granularity:
it blocks stale addresses from vector ANN and re-scores only those rows through
a targeted `take` plus flat-KNN path. The check is field-aware and version-gated,
so unrelated overlays and overlays already incorporated by the index do not
force fallback work.

Segment files may also retain stale rows after an in-place column rewrite has
removed their fragments from the segment bitmap. v12 applies each segment's
ownership mask inside partition search, **before** top-k selection, so stale
entries cannot consume candidate slots. Stable row IDs are resolved through
the current fragment row-ID sequences. Physical segment merge and optimization
also filter source rows before copying them into a new segment.

A `fast_search=true` flag lets the user opt **out** of the delta merge —
trading possible recall loss for latency if they know the delta is empty
or irrelevant.

---

## 6. Refine (exact re-rank)

Approximate distances from PQ/SQ/RQ are noisy. `refine_factor` fixes this:

```
  Without refine:                       With refine (factor = 10):
  ─────────────────                     ──────────────────────────
   ANN → top-k    → return              ANN → top (k·10) candidates
                                        take() → raw vectors from data/
                                        SIMD exact distance
                                        → top-k → return
```

- Implemented in `rust/lance/src/dataset/scanner.rs` during plan building.
- `refine_factor=None` disables it; a positive factor enables exact re-ranking,
  including factor 1. Typical values are 5–30; zero is rejected.
- Cost: one `take(row_ids)` over the dataset + one exact-distance pass over
  `k·factor` vectors.

Because Lance is optimized for random access, `take(...)` on `k·10` rows is
typically a small, well-batched sequence of page-level IOVs.

---

## 7. Prefilter vs postfilter

Example query: `WHERE category = 'cats'` combined with vector search.

- **Postfilter** (default, `scanner.prefilter = false`): run ANN first,
  filter the resulting `RecordBatch`. Can lose recall badly if the filter
  is selective and most top-k neighbours get filtered out. Good for
  non-selective filters.
- **Prefilter** (`scanner.prefilter = true`): evaluate the scalar filter
  first (using scalar indexes if present), materialize the surviving row-id
  set, then push that bitmap into the ANN path so only those IDs are
  considered during partition scanning. More accurate but costs a filter
  pass up front.

On HNSW composites, setting `approx_mode=Fast` uses the ACORN-1 traversal for
this prefiltered graph search. It can reduce filter-aware graph-search latency
at a possible recall cost; `Normal` remains the default.

The filter is represented as `Scanner::filter: LanceFilter`. Push-down wiring
happens in the plan builder (`vector_search` + KNN execution nodes), which lowers
`LanceFilter` into an `ExprFilterPlan` before execution.

External callers can also supply a row allow/block mask via
Rust `Scanner::with_row_addr_prefilter` or Python's serialized
`row_addr_allowlist` / `row_addr_blocklist` scanner arguments. This mask combines
with scalar predicates and deletion/segment masks before candidate selection;
despite the API names, it uses the dataset's `_rowid` domain: physical row
addresses without stable row IDs, and stable row IDs when that feature is
enabled. Construct the mask from the same dataset snapshot being queried.

---

## 8. Caching

Opening a segment loads its readers and shared metadata. Queries then load and
cache only the IVF partitions selected by `nprobes`; later queries can reuse
those partition entries.

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
  cross-segment collisions.
- `IVFIndex::load_partition` lazily inserts the requested partition's
  sub-index and storage into the segment cache.
- `Dataset::prewarm_index(name)` opens **all** same-name segments and loads all
  of their partitions. Tests assert that a subsequent query performs no index
  I/O, including when the logical index has multiple delta segments.
- v12 prewarming groups adjacent partitions into parallel read windows with
  a default encoded-byte target of 64 MiB, configurable through
  `LANCE_IVF_PREWARM_WINDOW_SIZE_BYTES`. This is an I/O target, not a decoded
  memory limit; cache capacity still determines which partitions remain warm.
- `Dataset::prewarm_index_segments(name, segment_ids)` (or Python's
  `prewarm_index(..., index_segments=...)`) can warm only selected physical
  segments of a logical index.
- Per-query `ScanStatistics.index_cache_hits` and `index_cache_misses` expose
  cache reuse without inferring it from total I/O. A path that performs no
  instrumented index-cache lookup reports zero for both counters.
- Entries remain subject to the configured cache backend and capacity.

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
- Vector segments can be merged only when they share compatible IVF and
  quantizer models. IVF/PQ/RQ mismatches are checked; the SQ learned-bounds
  validation gap remains, so keep independently trained SQ segments separate
  (see `05-distributed-vector-index-creation.md` §3).
- Default optimization can adjust IVF partitions using the persisted
  `target_partition_size` (or the index-type default). v12 splits oversized
  partitions into multiple pieces toward that target, or joins undersized
  partitions when no split is performed. A pass that changes centroids
  consolidates the participating segments and re-encodes affected rows.
  `OptimizeOptions::append()` and explicit merge counts bypass this automatic
  partition adjustment.
- Compaction of data fragments requires index remapping. The
  *fragment reuse index* (optional auxiliary index) accelerates this by
  tracking where each old row ended up after compaction.

---

## 10. A minimal debug recipe

If recall is low:

1. Is the right logical index being used? Inspect every same-name segment and
   union their `fragment_bitmap` values. Uncovered fragments use the flat
   fallback.
2. `nprobes` too low? Start at 20, then sweep.
3. `ef_search` (for HNSW variants) too low? The default is `k + k/2`;
   sweep upward.
4. Add `refine_factor=10–30`. If recall jumps, the approximate distance is
   the bottleneck.
5. Check distance type matches training. `Cosine` vs `L2` on
   un-normalized vectors silently ruins recall.

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
| Scanner entry (`nearest`) | `rust/lance/src/dataset/scanner.rs` |
| Vector-search planner | same file; `vector_search()` |
| Index loading | `rust/lance/src/index.rs` (`load_indices`, `open_vector_index`) |
| Index caching | `rust/lance/src/session/index_caches.rs` |
| KNN exec nodes | `rust/lance/src/io/exec/knn.rs` |
| Automatic IVF probe policy | `rust/lance/src/io/exec/knn/adaptive_probe.rs` |
| Shared batch partition search / prewarm | `rust/lance/src/index/vector/ivf/v2.rs` |
| Exact index-file format policy | `rust/lance/src/dataset/index.rs`, `rust/lance/src/dataset/versions/mod.rs` |
| Dataset directories | `rust/lance/src/dataset.rs` (`INDICES_DIR`, `indice_files_dir`) |
| `IndexMetadata` | `rust/lance-table/src/format/index.rs` |
| `Manifest` | `rust/lance-table/src/format/manifest.rs` |
| IVF on-disk meta keys | `rust/lance-index/src/vector/ivf/storage.rs` |
| PQ on-disk meta | `rust/lance-index/src/vector/pq/storage.rs` |
| Shuffler (build time) | `rust/lance-index/src/vector/v3/shuffler.rs` |
| Sub-index trait | `rust/lance-index/src/vector/v3/subindex.rs` |
| Unindexed-fragment helpers | `rust/lance/src/index.rs` + scanner callsites |

---

This concludes the storage-and-search core of the six-part reference. Recap:

1. **`00-overview.md`** — dataset layout + crate layering + lifecycle
2. **`01-vector-storage.md`** — how embedding columns are encoded
3. **`02-vector-indexes.md`** — the index algorithms themselves
4. **`03-index-on-disk-and-search.md`** *(this file)* — storage, query, ops
5. **`04-lance-versions.md`** — library releases versus file-format versions
6. **`05-distributed-vector-index-creation.md`** — distributed build protocol
