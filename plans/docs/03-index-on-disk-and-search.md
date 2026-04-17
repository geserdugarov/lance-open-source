# Lance — Vector Index On-Disk Format & Query Execution

**Scope.** What a committed vector index looks like on disk, how it is
referenced from the manifest, and the exact sequence of calls that happens
when a user runs `scanner.nearest(...)`.

**Audience.** Contributors debugging index loading, query latency, recall,
prefilter behavior, or index lifecycle (rebuilds, compaction, deltas).

---

## 1. The link from manifest to physical index files

At commit time, creating an index adds one `IndexMetadata` entry to the new
manifest and writes the index payload to `_indices/<uuid>/` under the
dataset root.

```
  _versions/17.manifest                  <- the committed manifest
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

`IndexMetadata` — `rust/lance-table/src/format/index.rs` (≈ line 31):

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
    pub files: Option<Vec<IndexFile>>,            // physical segments + sizes
}
```

Two key invariants:

1. **UUID identity.** The `_indices/<uuid>/` directory is addressed by the
   index's UUID, not its name. Renaming an index (if supported) leaves the
   directory alone. Rebuilding an index produces a **new** UUID and a new
   directory; the old one is retained until the next garbage collection so
   older dataset versions remain readable.

2. **`fragment_bitmap` is the source of truth for "indexed" rows.** Any
   fragment added *after* the index was built is not in the bitmap and is
   said to be **unindexed**. At query time the scanner reads this bitmap to
   decide whether to flat-scan the delta.

Directory-resolution helpers:

- `Dataset::indices_dir()` → `<dataset_root>/_indices/` (`rust/lance/src/dataset.rs:1922`)
- `Dataset::indice_files_dir(idx)` → the indices **base** directory for that index
  (typically `<dataset_root>/_indices/`, but redirected via `IndexMetadata::base_id`
  when the index lives outside the dataset root). Callers append `<index.uuid>/`
  themselves to reach the segment files. (`rust/lance/src/dataset.rs:2135`)

---

## 2. Physical layout of an IVF_PQ (or IVF_HNSW_PQ) index

The index segments are Lance files (same `.lance` container as data files),
with special schema-metadata keys that tell readers how to interpret them.

```
   _indices/<uuid>/
   ├── <segment-a>.lance
   │    │
   │    │  Schema metadata:
   │    │     lance:ivf           → serialized IvfModel (centroids + per-partition offsets/lengths)
   │    │     lance:ivf:partition → per-partition auxiliary data layout
   │    │     lance:pq            → serialized ProductQuantizationMetadata
   │    │       • num_sub_vectors, num_bits, distance_type
   │    │       • codebook_position   (index into global buffers)
   │    │
   │    │  Columns:
   │    │     __ivf_part_id : UInt32     (partition each vector belongs to)
   │    │     __pq_code     : FixedSizeBinary(M)   (or UInt8 × M)
   │    │     (row-id  →  implicit via partition offset + position)
   │    │
   │    │  Global buffers:
   │    │     PQ codebook (f32 flattened)  at position pq_codebook_position
   │    │     IVF centroids (f32 flattened) at position ivf_centroids_position
   │    │
   │    │  Pages / columns / column meta / footer
   │    │  (same v2 file layout as data files)
   │    ↓
   ├── <segment-b>.lance         <- optional additional segments, e.g. auxiliary data
   └── ...
```

For the HNSW variants the layout adds per-partition graph segments, each
holding the adjacency lists per layer plus the partition's entry point. The
generic trait that lets IVF + various sub-indexes share this framework is
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
│      • load_indices() → pick IndexMetadata whose fields match column     │
│      • open_vector_index(idx) (cached) → deserialize IvfModel + Q        │
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
│                     │                         │    d. top-k row IDs      │
└──────────┬──────────┘                         └─────────────┬────────────┘
           │                                                   │
           │                                                   ▼
           │                                    ┌──────────────────────────┐
           │                                    │ 3. MERGE DELTA           │
           │                                    │    scanner.rs :: knn_    │
           │                                    │    combined(...)         │
           │                                    │    if any fragment NOT   │
           │                                    │    in fragment_bitmap:   │
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
           │ 5. PREFILTER / POSTFILTER                      │
           │    prefilter = true:                           │
           │       scalar predicates pushed down — ANN      │
           │       search sees only surviving row IDs       │
           │    prefilter = false (default):                │
           │       ANN first, then filter the output batch  │
           └─────────────────────┬──────────────────────────┘
                                 │
                                 ▼
           ┌────────────────────────────────────────────────┐
           │ 6. RESULT                                      │
           │    RecordBatch stream with original columns    │
           │    + synthetic `_distance` column              │
           └────────────────────────────────────────────────┘
```

---

## 4. The ANN path in detail

For `IVF_PQ` the ANN phase inside step 2 expands to:

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
  4. Output: top-k (row_id, approx_dist).
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
      indexed_fragments   = {0, 1, 2, 3, 4, 5}   → ANN via index X
      unindexed_fragments = {6, 7}               → flat scan these
      final top-k         = merge(ANN, flat) → dedup → sort → truncate
```

Code: `rust/lance/src/dataset/scanner.rs`:

- `Dataset::unindexed_fragments(index_name)` returns the complement.
- `Scanner::vector_search` branches on whether the merge is needed.
- `knn_combined` unions the ANN output with a flat KNN plan over the delta
  before top-k truncation.

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

The filter is represented as `Scanner::filter: LanceFilter`
(`rust/lance/src/dataset/scanner.rs` ≈ line 739). Push-down wiring happens
in the plan builder (`vector_search` + KNN execution nodes), which lowers
`LanceFilter` into an `ExprFilterPlan` before execution.

---

## 8. Caching

First query loads and deserializes the index. Subsequent queries reuse the
in-memory structures via a nested cache.

```
   GlobalIndexCache                           rust/lance/src/session/
   ┌───────────────────────┐                  index_caches.rs
   │                       │
   │   DSIndexCache(dsURI) │
   │   ┌─────────────────┐ │
   │   │                 │ │
   │   │ IndexCache      │ │
   │   │  keyed by       │ │
   │   │  (idx UUID,     │ │
   │   │   maybe FRI)    │ │
   │   │                 │ │
   │   │ entries:        │ │
   │   │  • IvfModel     │ │
   │   │  • Quantizer    │ │
   │   │  • HNSW graph   │ │
   │   │  • PQ codes     │ │
   │   │    (per part.)  │ │
   │   └─────────────────┘ │
   └───────────────────────┘
```

Properties:

- Process-wide by default (via `GlobalIndexCache`), scoped by session.
- **Whole-index** load — there is no lazy per-partition loading at the cache
  layer; the first touch hydrates everything. Partition codes themselves can
  be streamed from disk inside the query, but the metadata and codebooks
  land in memory up front.
- Eviction is session-driven; persistent sessions keep hot indexes
  essentially forever.

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
- Index rewriting produces a **new UUID**. This is why the rebuild does not
  risk corrupting the live index.
- Compaction of data fragments requires index remapping. The
  *fragment reuse index* (optional auxiliary index) accelerates this by
  tracking where each old row ended up after compaction.

---

## 10. A minimal debug recipe

If recall is low:

1. Is the right index being used? Call `ds.list_indices()`; confirm UUIDs
   and `fragment_bitmap` coverage. Unindexed fragments mean flat-scan
   fallback.
2. `nprobes` too low? Start at 20, then sweep.
3. `ef_search` (for HNSW variants) too low? Start at `2·k`, sweep up.
4. Add `refine_factor=10–30`. If recall jumps, the approximate distance is
   the bottleneck.
5. Check distance type matches training. `Cosine` vs `L2` on
   un-normalized vectors silently ruins recall.

If latency is high:

1. Cold cache? First query is always slow. Re-run and measure warm.
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
