# Lance — Architecture Overview (Vector-Focused)

**Source baseline:** `main` at `67faafe65` (2026-11-16), workspace
`15.0.0-beta.22`. This is a development snapshot; the latest stable tag in
this checkout is `v14.0.3`. See `04-lance-versions.md` for release boundaries.

A field guide for contributors whose primary interest is **storing and searching
embedding vectors** with Lance. This document frames the big picture; the
sibling documents drill into each subsystem:

| File | Subject |
|---|---|
| `00-overview.md` *(this)* | Dataset directory layout, crate layering, end-to-end vector lifecycle |
| `01-vector-storage.md` | How embedding columns are physically encoded and written |
| `02-vector-indices.md` | Vector index types (IVF, PQ, SQ, HNSW, RaBitQ), composition, build flow |
| `03-index-on-disk-and-search.md` | Index file format, query execution, prefilter + refinement |
| `04-lance-versions.md` | `Lance v1`, `v2`, … major release lines; which version to anchor to for compatibility |
| `05-distributed-vector-index-creation.md` | Distributed build and optimization protocols across Rust/Python/Java, model scope, segment merge/commit invariants, and progress tracker |

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
├── _indices/                          <- All secondary indices (vector, scalar, FTS)
│   ├── <segment-uuid>/                <- One directory per physical index segment
│   │   ├── index.idx                  <- Index-specific primary structures
│   │   └── auxiliary.idx              <- Codes, row IDs, or auxiliary structures
│   └── ...                            <- A named logical index may have many segments
├── _deletions/                        <- Per-fragment deletion vectors
│   └── <frag>-<read_version>-<id>.{arrow|bin} <- Deletion vector, added lazily
└── _transactions/                     <- Transaction log (used for conflict resolution)
    └── <read_version>-<uuid>.txn
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
`DELETE` records a deletion vector, either in `_deletions/` or inline in the
manifest when the configured byte budget permits it. Inline deletions set a
table feature flag, so they require a reader that supports that feature.
The next manifest references the new objects; older manifests retain their
original snapshot. This is what makes
Lance zero-copy-versioned.

---

## 2. Crate layering (relevant to vector workflows)

Selected direct Cargo dependencies are shown below. Repeated crates represent
shared dependencies; this is not the complete dependency graph.

```text
python/src (PyO3) / java/lance-jni (JNI)
`-- lance: Dataset / Fragment / Scanner
    |-- lance-datafusion: planner glue
    |   `-- lance-datafusion-exec: execution contracts
    |-- lance-index: vector + scalar implementations (including vector/kmeans.rs)
    |   |-- lance-index-core: traits + types
    |   |   |-- lance-core
    |   |   |-- lance-io
    |   |   `-- lance-select
    |   |-- lance-linalg: distance / SIMD
    |   |-- lance-table: manifest / fragments
    |   `-- lance-file: reader / writer
    |-- lance-table
    |-- lance-geo (optional): RTree implementation + plugin registration
    |-- lance-file
    |   |-- lance-encoding: logical + physical encodings
    |   |-- lance-io: object store + scheduler
    |   |-- lance-core
    |   `-- lance-arrow
    |-- lance-encoding
    |-- lance-io
    |-- lance-core
    `-- lance-arrow
```

For vector workloads, the hot path touches:

- **Write:** `lance` → `lance-file::versions` (exact-format writer and encoding policy) → `lance-encoding` (primitive encoder for `FixedSizeList<f32>`) → `lance-io` (object store).
- **Index build:** `lance` → contracts from `lance-index-core` → implementations in `lance-index::vector::*` (including `vector/kmeans.rs`) → `lance-linalg` (distance kernels) → `lance-file` (segment files).
- **Query:** `lance::dataset::scanner` → `lance-index-core` + `lance-index::vector` + `lance::io::exec::knn` → `lance-linalg::distance` (SIMD kernels).

`lance-datafusion-exec` owns shared execution/stream contracts so index
implementations do not need the full DataFusion crate. `lance-select` owns row
address masks and selection algebra. The workspace uses Arrow 59.2 and
DataFusion 55. For the full ownership map, read `ARCHITECTURE.md`: scanner
configuration is in `dataset/scanner/builder.rs`, vector planning in
`dataset/scanner/vector.rs`, and index coordination in `index/lifecycle.rs`
with separate loading, provenance, statistics, and prewarm modules.

File-version selectors and exact persisted identities live in
`rust/lance-file/src/version.rs`. New datasets resolve `stable` to `2.2`;
`next` selects unstable `2.3`. Each exact version composes its readers, writers,
and encoding policy under `rust/lance-file/src/versions/`; `lance-encoding`
supplies the shared encoding mechanisms. Existing datasets retain their
manifest's write format when the library is upgraded.

Unstable 2.3 uses independent structure and value planes
(`PlanarLayout` in `protos/encodings_v2_3.proto`). It can dictionary-encode
repeated fixed-size vectors and apply opt-in pcodec compression to float
mini-blocks, including fixed-size vectors. Stable 2.1/2.2 retain their dense
structural layouts; the experimental 2.3 changes do not alter their wire contracts.

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

A logical index is identified by `name`, not UUID. Same-name segments with
compatible details are queried together. Their effective fragment coverage
defines the indexed portion of the dataset; after deferred compaction, the
fragment reuse index (FRI) may translate stored coverage and row addresses.
A definition-only vector index records intent without trained files or indexed
coverage, allowing small tables to flat-scan until optimization can train it.

Stable row IDs and physical row addresses remain separate domains. Lance can
store row-ID/version lineage in hidden columns and load fragment row-ID
sequences on demand. FRI also supports stable-row-ID tables; an index's row
domain determines whether address remapping is still needed.

`IndexMetadata::keyed_fields()` and `dependency_fields()` interpret both
suffix-style and independent covering fields. Do not infer search keys by
subtracting a presumed carried suffix. `index/visibility.rs` owns snapshot-local
overlay selections; query-visible index listings are cached separately from
raw manifest metadata.

Transaction schemas live in `protos/transaction/`. The action-based Transaction
V2 specification and protobuf messages are present, but the runtime decoder
rejects `CompositeOperation`; the ordinary named operations remain the
implemented commit path. A schema declaration is not a new transaction API.

A `Scanner` (state in `rust/lance/src/dataset/scanner.rs`, planning in its
`scanner/` submodules) stitches these together at read time; the query path is
covered in `03-index-on-disk-and-search.md`.

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
  │    → write_fragments_internal_impl (dataset/write.rs)             │
  │    → dataset/versions/mod.rs → lance_file::versions::create_writer │
  │    → versions/v2_2::Writer for the current stable format          │
  │    → PrimitiveStructuralEncoder for FixedSizeList<f32>           │
  │       (encodings/logical/primitive/structural_encoder.rs)        │
  │    → Raw FixedSizeList values in stable 2.1/2.2 pages           │
  │    → Bytes land in data/<uuid>.lance (pages + col meta + footer) │
  └──────────────────────────────────────────────────────────────────┘
                                        │  commit
                                        ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │ 2. COMMIT                                                        │
  │    Transaction{ operation: Append { fragments } }                │
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
  │    (a) Sample vectors → lance-index::vector::kmeans → centroids    │
  │    (b) Train/load quantizer (PQ/SQ/RQ) on sampled data            │
  │    (c) Assign vectors to partitions and encode quantized codes   │
  │    (d) Shuffle encoded rows into per-partition Lance files       │
  │    (e) Build sub-index per partition (flat list or HNSW graph)   │
  │    (f) Write one or more UUID segments under _indices/            │
  │    (g) Record one IndexMetadata per segment + fragment coverage   │
  └──────────────────────────────────────────────────────────────────┘
                                        │  scanner.nearest("vector", q, k)
                                        ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │ 4. QUERY                                                         │
  │    lance/src/dataset/scanner/vector.rs::vector_search            │
  │    → open every compatible segment for the named logical index   │
  │    → IVF: rank partitions; fixed or automatic probe budget       │
  │    → Sub-index search per partition (flat scan / HNSW traversal) │
  │    → Quantized distance approximation (PQ table / SQ / RQ)       │
  │    → Optional refine: exact re-rank with raw vectors             │
  │    → Fallback: uncovered fragments + stale overlay rows          │
  │    → Exact rescore of union (RQ code-only exception: doc 03 §6)   │
  │    → Prefilter: scalar predicates pushed into / applied around   │
  │    → Top-k RecordBatch stream to user                            │
  └──────────────────────────────────────────────────────────────────┘
```

The rest of this document set expands each box.

IVF_RQ and IVF_HNSW_RQ default to five bits per dimension. Eligible batch queries
share partition scans with fixed or adaptive probes; shared exact refinement
requires fixed probes. Automatic probing has L2, cosine, and Dot profiles
through `k=100000`, across IVF index and vector types, including refinement;
`search_effort` adjusts the initial budget.
Rust, Python, and Java expose plan/build/merge/commit stages for distributed
index optimization and bounded consolidation. Native flat RaBitQ additionally
supports quantized refinement, plane reads, and runtime scan policies.
Cross-column refinement reranks coarse candidates in another vector space;
WARP provides an indexed multivector search path over supported flat IVF
sub-indices. See `02-vector-indices.md`,
`03-index-on-disk-and-search.md`, and `05-distributed-vector-index-creation.md`
for the API boundaries.

---

## 5. Recommended reading order

1. **Start here** (`00-overview.md`) — you are done with it.
2. Read `01-vector-storage.md` before touching any encoding/write code.
3. Read `02-vector-indices.md` before proposing a new index type or tuning
   an existing one.
4. Read `03-index-on-disk-and-search.md` before debugging query latency,
   recall, or index rebuild behavior.
5. Use `04-lance-versions.md` when deciding which released compatibility line
   a change or investigation should target.
6. Read `05-distributed-vector-index-creation.md` before integrating vector
   index builds with an external scheduler or designing delta consolidation.

These six vector guides form the foundation of the fourteen-document set
(00–13). Continue with [06](06-distributed-current-state.md) for the distributed
architecture audit and chapters 07–13 for the integration designs and contracts.
The order above minimizes backtracking.
