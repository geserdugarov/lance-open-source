# Lance — Architecture Overview (Vector-Focused)

A field guide for contributors whose primary interest is **storing and searching
embedding vectors** with Lance. This document frames the big picture; the
sibling documents drill into each subsystem:

| File | Subject |
|---|---|
| `00-overview.md` *(this)* | Dataset directory layout, crate layering, end-to-end vector lifecycle |
| `01-vector-storage.md` | How embedding columns are physically encoded and written |
| `02-vector-indexes.md` | Vector index types (IVF, PQ, SQ, HNSW, RaBitQ), composition, build flow |
| `03-index-on-disk-and-search.md` | Index file format, query execution, prefilter + refinement |
| `04-lance-versions.md` | `Lance v1`, `v2`, … major release lines; which version to anchor to for compatibility |
| `05-distributed-vector-index-creation.md` | Distributed build protocol, model scope, segment merge/commit invariants, operations, and progress tracker |

---

## 1. What a Lance dataset looks like on disk

```
my_dataset.lance/
├── data/                              <- Base and overlay data files
│   ├── <uuid-1>.lance                 <- Base file (one per fragment/column group in v2)
│   ├── <uuid-2>.lance                 <- Each file holds many pages across many columns
│   └── ...
├── _versions/                         <- Immutable manifests, one per committed version
│   ├── <version-key>.manifest         <- Schema, fragments, index segments, flags
│   ├── <version-key>.manifest         <- V1 and V2 use different naming schemes
│   └── ...
├── _indices/                          <- All secondary indexes (vector, scalar, FTS)
│   ├── <segment-uuid>/                <- One directory per physical index segment
│   │   ├── index.idx                  <- Index-specific primary structures
│   │   └── auxiliary.idx              <- Codes, row IDs, or auxiliary structures
│   └── ...                            <- A named logical index may have many segments
├── _deletions/                        <- Per-fragment deletion vectors
│   └── <frag-id>-<version>.arrow      <- Tombstone bitmap, added lazily
└── _transactions/                     <- Transaction log (used for conflict resolution)
    └── <timestamp>-<uuid>.txn
```

Constants authoritatively defined in the source:

| Dir | Constant | Location |
|---|---|---|
| `data/` | `DATA_DIR` | `rust/lance/src/dataset.rs` |
| `_versions/` | `VERSIONS_DIR` | `rust/lance-table/src/io/commit.rs` |
| `_indices/` | `INDICES_DIR` | `rust/lance/src/dataset.rs` |
| `_deletions/` | `DELETIONS_DIR` | `rust/lance-table/src/io/deletion.rs` |
| `_transactions/` | `TRANSACTIONS_DIR` | `rust/lance/src/dataset.rs` |

**Key property:** data and index files are **append-only and immutable**. An
`UPDATE` may append replacement fragments or sparse overlay files, while a
`DELETE` appends a deletion vector. The next manifest references the new
objects; older manifests retain their original snapshot. This is what makes
Lance zero-copy-versioned.

---

## 2. Crate layering (relevant to vector workflows)

```
 Bindings       python/src (PyO3)       java/lance-jni (JNI)
                              │
 Execution            lance (main crate)
                 Dataset / Fragment / Scanner
                    ┌─────┴─────┐
                    │           │
             lance-datafusion   lance-index
               planner glue     vector + scalar implementations
                    │           │
                    │      lance-index-core
                    │       traits + types
                    │           │
                 lance-table   lance-linalg
            manifest / fragments  distance / kmeans / SIMD
                    │           │
       ┌───────────┼───────────┴──────┐
       │            │                  │
   lance-file    lance-encoding       lance-io
 reader/writer   logical + physical   object store + scheduler
       └───────────┴───────────┬──────────┘
                            │
                 lance-core / lance-arrow
```

For vector workloads, the hot path touches:

- **Write:** `lance` → `lance-encoding` (primitive encoder for `FixedSizeList<f32>`) → `lance-file` (writer) → `lance-io` (object store).
- **Index build:** `lance` → contracts from `lance-index-core` → implementations in `lance-index::vector::*` → `lance-linalg` (kmeans, distance) → `lance-file` (segment files).
- **Query:** `lance::dataset::scanner` → `lance-index-core` + `lance-index::vector` + `lance::io::exec::knn` → `lance-linalg::distance` (SIMD kernels).

---

## 3. The five load-bearing abstractions

These are the types every contributor must know before the code becomes
legible. All five are tied together by the `Manifest`.

```
             ┌──────────────────────────────┐
             │        Manifest (vN)         │
             │  • schema                    │
             │  • fragments: [Fragment]     │
             │  • indices:   [IndexMeta]    │
             │  • feature_flags             │
             └──────────────┬───────────────┘
                 references │ references
           ┌───────────────┘ └──────────────┐
           ▼                                 ▼
  ┌────────────────┐                ┌──────────────────┐
  │   Fragment     │                │  IndexMetadata   │
  │ • id           │                │ • name + uuid    │
  │ • data_files[] │                │ • fields[]       │
  │ • overlays[]   │                │ • fragment_bitmap│
  │ • deletion_vec │                │                  │
  └──────┬─────────┘                │ • dataset_version│
         │ points to                │ • files[]        │
         ▼                          └──────────┬───────┘
  ┌──────────────┐                             │
  │  DataFile    │                             │ one physical segment
  │ (.lance file)│                             ▼
  │  • path      │                   ┌───────────────────┐
  │  • fields[]  │                   │ _indices/<uuid>/  │
  └──────┬───────┘                   │ index.idx + aux   │
         │                           └───────────────────┘
         ▼
  ┌────────────────────────────────────┐
  │   Pages (encoded column data)      │
  │  [page0][page1]…[col-meta][footer] │
  └────────────────────────────────────┘
```

| Concept | Lives in | Role for vector workloads |
|---|---|---|
| **Dataset** | `rust/lance/src/dataset.rs` | User-facing handle; owns object store, commit handler, current manifest |
| **Fragment** | `rust/lance-table/src/format/fragment.rs` + `rust/lance/src/dataset/fragment.rs` | Immutable row-address range; tracks base files, sparse overlays, and a deletion vector |
| **DataFile** | `rust/lance-table/src/format/fragment.rs` | One physical `.lance` file; knows which field IDs it stores |
| **Manifest** | `rust/lance-table/src/format/manifest.rs` | Versioned snapshot — the atomic unit of a commit |
| **IndexMetadata** | `rust/lance-table/src/format/index.rs` | Describes one physical segment of a named logical index; its `fragment_bitmap` contributes to aggregate coverage |

A logical index is identified by `name`, not UUID. All same-name segments
with compatible details are queried, and the union of their fragment bitmaps
defines the covered portion of the dataset.

A `Scanner` (in `rust/lance/src/dataset/scanner.rs`) is the query-builder that
stitches these together at read time; the full query path is covered in
`03-index-on-disk-and-search.md`.

---

## 4. End-to-end lifecycle of an embedding column

The following shows what happens from `write_dataset(...)` with a `vector`
column of type `FixedSizeList<Float32, 768>`, through index build, to a
`nearest(...)` query.

```
                   ┌──────────────────────────────────────────────┐
                   │  User: RecordBatch with vector col (768-dim) │
                   └────────────────────┬─────────────────────────┘
                                        │  write_dataset / add
                                        ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │ 1. INGEST                                                        │
  │    lance/src/dataset/write/insert.rs                             │
  │    → FragmentCreateBuilder                                       │
  │    → FileWriter (lance-file/src/writer.rs)                       │
  │    → PrimitiveStructuralEncoder for FixedSizeList<f32>           │
  │       (lance-encoding/src/encodings/logical/primitive.rs)        │
  │    → Optional Byte-Stream-Split + LZ4/Zstd compression           │
  │    → Bytes land in data/<uuid>.lance (pages + col meta + footer) │
  └──────────────────────────────────────────────────────────────────┘
                                        │  commit
                                        ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │ 2. COMMIT                                                        │
  │    Transaction{ op: Add(fragments) }                             │
  │    → CommitHandler atomically writes the next versioned manifest │
  │    → New Manifest lists the new Fragment + its DataFile          │
  └──────────────────────────────────────────────────────────────────┘
                                        │  create_index(column="vector", …)
                                        ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │ 3. BUILD INDEX                                                   │
  │    lance/src/index/vector/builder.rs::IvfIndexBuilder<S, Q>      │
  │       S = sub-index (Flat | HNSW)                                │
  │       Q = quantizer  (PQ | SQ | RaBitQ | Flat)                   │
  │    ─────────────────────────────────────────────────────────     │
  │    (a) Sample vectors → kmeans (lance-linalg) → IVF centroids    │
  │    (b) Assign every vector to nearest centroid (partition)       │
  │    (c) Shuffle rows into per-partition Lance files               │
  │    (d) Train quantizer (PQ/SQ/RQ) on sampled data                │
  │    (e) Build sub-index per partition (flat list or HNSW graph)   │
  │    (f) Write one or more UUID segments under _indices/            │
  │    (g) Record one IndexMetadata per segment + fragment coverage   │
  └──────────────────────────────────────────────────────────────────┘
                                        │  scanner.nearest("vector", q, k)
                                        ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │ 4. QUERY                                                         │
  │    lance/src/dataset/scanner.rs::vector_search                   │
  │    → open every compatible segment for the named logical index   │
  │    → IVF: top-nprobes partitions vs centroids                    │
  │    → Sub-index search per partition (flat scan / HNSW traversal) │
  │    → Quantized distance approximation (PQ table / SQ / RQ)       │
  │    → Optional refine: exact re-rank with raw vectors             │
  │    → Merge segment results with flat-scanned uncovered fragments │
  │    → Prefilter: scalar predicates pushed into / applied around   │
  │    → Top-k RecordBatch stream to user                            │
  └──────────────────────────────────────────────────────────────────┘
```

The rest of this document set expands each box.

---

## 5. Recommended reading order

1. **Start here** (`00-overview.md`) — you are done with it.
2. Read `01-vector-storage.md` before touching any encoding/write code.
3. Read `02-vector-indexes.md` before proposing a new index type or tuning
   an existing one.
4. Read `03-index-on-disk-and-search.md` before debugging query latency,
   recall, or index rebuild behavior.
5. Use `04-lance-versions.md` when deciding which released compatibility line
   a change or investigation should target.
6. Read `05-distributed-vector-index-creation.md` before integrating vector
   index builds with an external scheduler or designing delta consolidation.

All six files are cross-referenced and can be read in isolation, but the
order above minimizes backtracking.
