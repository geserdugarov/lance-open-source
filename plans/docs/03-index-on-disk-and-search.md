# Lance — Vector Index On-Disk Format & Query Execution

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
    pub fields: Vec<i32>,                         // indexed field IDs
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

---

## 3. The query execution pipeline

User calls (Python):

```python
results = (
    ds.scanner()
      .nearest("embedding", q, k=10, nprobes=20, refine_factor=10)
      .filter("category = 'cats'")
      .limit(10)
      .to_table()
)
```

End-to-end Rust pipeline:

```
┌──────────────────────────────────────────────────────────────────────────┐
│ 1. Scanner builder                                                       │
│    rust/lance/src/dataset/scanner.rs :: Scanner::nearest(col, q, k)      │
│        stores a Query{ column, query_vec, k, nprobes, refine_factor }    │
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
           │ 4. REFINE (optional; refine_factor > 1)        │
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
on the flat path with their current values. Overlay/index integration is
experimental, so confirm that planner path before relying on it in a released
client.

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
- `refine_factor=None` disables it; typical values are 5–30.
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

The filter is represented as `Scanner::filter: LanceFilter`. Push-down wiring
happens in the plan builder (`vector_search` + KNN execution nodes), which lowers
`LanceFilter` into an `ExprFilterPlan` before execution.

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
- Vector segments can be merged only when they share compatible IVF and
  quantizer models; independently trained subset segments are rejected.
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
| Dataset directories | `rust/lance/src/dataset.rs` (`INDICES_DIR`, `indice_files_dir`) |
| `IndexMetadata` | `rust/lance-table/src/format/index.rs` |
| `Manifest` | `rust/lance-table/src/format/manifest.rs` |
| IVF on-disk meta keys | `rust/lance-index/src/vector/ivf/storage.rs` |
| PQ on-disk meta | `rust/lance-index/src/vector/pq/storage.rs` |
| Shuffler (build time) | `rust/lance-index/src/vector/v3/shuffler.rs` |
| Sub-index trait | `rust/lance-index/src/vector/v3/subindex.rs` |
| Unindexed-fragment helpers | `rust/lance/src/index.rs` + scanner callsites |

---

This concludes the four-part reference. Recap:

1. **`00-overview.md`** — dataset layout + crate layering + lifecycle
2. **`01-vector-storage.md`** — how embedding columns are encoded
3. **`02-vector-indexes.md`** — the index algorithms themselves
4. **`03-index-on-disk-and-search.md`** *(this file)* — storage, query, ops
