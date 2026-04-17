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

---

## 1. What a Lance dataset looks like on disk

```
my_dataset.lance/
├── data/                              <- All user-visible data lives here
│   ├── <uuid-1>.lance                 <- Data file (one per fragment-per-column-group in v2)
│   ├── <uuid-2>.lance                 <- Each file holds many pages across many columns
│   └── ...
├── _versions/                         <- Immutable manifests, one per committed version
│   ├── 1.manifest                     <- Protobuf: schema, fragments[], indices[], flags
│   ├── 2.manifest
│   └── ...
├── _indices/                          <- All secondary indexes (vector, scalar, FTS)
│   └── <index-uuid>/                  <- One directory per logical index (keyed by UUID)
│       ├── <segment>.idx              <- Index segments (layout depends on index type)
│       └── ...
├── _deletions/                        <- Per-fragment deletion vectors
│   └── <frag-id>-<version>.arrow      <- Tombstone bitmap, added lazily
└── _transactions/                     <- Transaction log (used for conflict resolution)
    └── <timestamp>-<uuid>.txn
```

Constants authoritatively defined in the source:

| Dir | Constant | Location |
|---|---|---|
| `data/` | `DATA_DIR` | `rust/lance/src/dataset.rs:146` |
| `_versions/` | `VERSIONS_DIR` | `rust/lance-table/src/io/commit.rs:70` |
| `_indices/` | `INDICES_DIR` | `rust/lance/src/dataset.rs:145` |
| `_deletions/` | `DELETIONS_DIR` | `rust/lance-table/src/io/deletion.rs:25` |
| `_transactions/` | `TRANSACTIONS_DIR` | `rust/lance/src/dataset.rs:147` |

**Key property:** data files are **append-only and immutable**. An `UPDATE` or
`DELETE` does not rewrite data files — it writes new fragments or adds a
deletion vector, and produces a new manifest. This is what makes Lance
zero-copy-versioned.

---

## 2. Crate layering (relevant to vector workflows)

```
                ┌──────────────────────────────────────┐
 Bindings       │  python/src (PyO3)  │  java/lance-jni │
                └──────────────────────────────────────┘
                                  ▲
                                  │
                ┌──────────────────────────────────────┐
 Execution     │           lance (main crate)           │
                │  • Dataset, Fragment, Scanner          │
                │  • write/  index/  io/commit/  io/exec│
                └──────────────────────────────────────┘
                          ▲              ▲
                          │              │
   ┌──────────────────────┴─┐   ┌────────┴────────────┐
   │  lance-datafusion      │   │  lance-index        │
   │  (SQL / planner glue)  │   │  • vector/ivf, pq,  │
   └────────────────────────┘   │    sq, hnsw, bq     │
                          ▲      └─────────────────────┘
                          │               ▲
                ┌─────────┴──────┐         │
                │  lance-table   │   ┌─────┴─────────┐
                │  • Manifest    │   │ lance-linalg  │
                │  • Fragment    │   │ • distance/   │
                │  • DataFile    │   │ • kmeans      │
                │  • IndexMeta   │   │ • simd/       │
                └────────────────┘   └───────────────┘
                          ▲
                          │
    ┌────────────┬────────┴─────────┬──────────────┐
    │ lance-file │  lance-encoding  │   lance-io   │
    │ reader/    │  logical +       │  ObjectStore │
    │ writer +   │  physical        │  scheduler   │
    │ footer     │  encoders        │  blob cache  │
    └────────────┴──────────────────┴──────────────┘
                          ▲
                          │
                ┌─────────┴──────┐
                │  lance-core    │   Schema, Field,
                │  lance-arrow   │   error, Arrow utils
                └────────────────┘
```

For vector workloads, the hot path touches:

- **Write:** `lance` → `lance-encoding` (primitive encoder for `FixedSizeList<f32>`) → `lance-file` (writer) → `lance-io` (object store).
- **Index build:** `lance` → `lance-index::vector::*` → `lance-linalg` (kmeans, distance) → `lance-file` (writer for index segments).
- **Query:** `lance::dataset::scanner` → `lance-index::vector` + `lance::io::exec::knn` → `lance-linalg::distance` (SIMD kernels).

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
  │ • id           │                │ • uuid           │
  │ • data_files[] │                │ • fields[]       │
  │ • deletion_vec │                │ • fragment_bitmap│
  └──────┬─────────┘                │ • dataset_version│
         │ points to                │ • files[]        │
         ▼                          └──────────┬───────┘
  ┌──────────────┐                             │
  │  DataFile    │                             │ physical segments
  │ (.lance file)│                             ▼
  │  • path      │                   ┌───────────────────┐
  │  • fields[]  │                   │ _indices/<uuid>/  │
  └──────┬───────┘                   │   <segment>.idx   │
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
| **Fragment** | `rust/lance-table/src/format/fragment.rs` + `rust/lance/src/dataset/fragment.rs` | Immutable slice of rows; tracks which `DataFile`s hold its columns + deletion vector |
| **DataFile** | `rust/lance-table/src/format/fragment.rs` | One physical `.lance` file; knows which field IDs it stores |
| **Manifest** | `rust/lance-table/src/format/manifest.rs` | Versioned snapshot — the atomic unit of a commit |
| **IndexMetadata** | `rust/lance-table/src/format/index.rs` | Pointer from manifest to index directory; carries `fragment_bitmap` so queries know which fragments are "indexed" vs "unindexed" |

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
  │    → Byte-Stream-Split + LZ4/Zstd compression                    │
  │    → Bytes land in data/<uuid>.lance (pages + col meta + footer) │
  └──────────────────────────────────────────────────────────────────┘
                                        │  commit
                                        ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │ 2. COMMIT                                                        │
  │    Transaction{ op: Add(fragments) }                             │
  │    → CommitHandler atomically writes _versions/<N>.manifest      │
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
  │    (f) Write segments under _indices/<index-uuid>/               │
  │    (g) New Manifest records IndexMetadata w/ fragment_bitmap     │
  └──────────────────────────────────────────────────────────────────┘
                                        │  scanner.nearest("vector", q, k)
                                        ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │ 4. QUERY                                                         │
  │    lance/src/dataset/scanner.rs::vector_search                   │
  │    → open_vector_index (deserialize IVF + quantizer from disk)   │
  │    → IVF: top-nprobes partitions vs centroids                    │
  │    → Sub-index search per partition (flat scan / HNSW traversal) │
  │    → Quantized distance approximation (PQ table / SQ / RQ)       │
  │    → Optional refine: exact re-rank with raw vectors             │
  │    → Merge with flat-scanned unindexed fragments                 │
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

All four files are cross-referenced and can be read in isolation, but the
order above minimizes backtracking.
