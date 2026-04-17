# Lance — Vector Storage Deep Dive

**Source baseline:** Lance `12.0.0`, commit `cbeec97cb` (2026-09-17).

**Scope.** How embedding vectors become bytes on disk — from Arrow schema
through to the pages of a `.lance` file. Index storage is covered separately
in `03-index-on-disk-and-search.md`.

**Audience.** Contributors who touch write paths, encoders, or schema handling.

---

## 1. The Arrow type of a vector column

Lance represents an embedding as `FixedSizeList<T>` where `T` is a numeric
primitive. There is no bespoke Lance-level "vector type" — Lance leans on
Arrow.

```
             FixedSizeList<T>
             ┌────────────────────────────┐
             │ list_size: i32  (= D)      │   dimension of the embedding
             │ child: Field {             │
             │   name: "item",            │   <-- conventional child name
             │   data_type: T,            │   <-- usually Float32
             │   nullable: bool           │
             │ }                          │
             └────────────────────────────┘
```

**Supported `T` for vector columns** (as exercised by encoders and distance
kernels):

| Element type | Bytes / value | Notes |
|---|---|---|
| `Float32` (`f32`) | 4 | **Most common.** All SIMD kernels support it. |
| `Float16` (`f16`) | 2 | Feature-gated (`fp16kernels`); recent commits added kernels. |
| `BFloat16` (`bf16`) | 2 | Recent SIMD distance kernels added (commit `d0124edf`). |
| `UInt8` (`u8`) | 1 | Used for binary / quantized vectors (Hamming / L2 only). |
| `Float64` (`f64`) | 8 | Rare for embeddings; has SQ/distance kernels (commit `c913ff8f`). |

**Important:** storing vectors as `Float16` / `BFloat16` / `UInt8` is a
*user choice at write time*, not an automatic quantization. Lance does not
down-convert your `Float32` vectors. Quantization that happens automatically
only lives in **indexes** (PQ / SQ / RaBitQ — see `02-vector-indexes.md`).

**Primitive-type validation.** `FixedSizeList<primitive>` is detected as
"primitive" (and therefore takes the fast encoder path) by
`PrimitiveFieldEncoding::is_primitive_type` in
`rust/lance-encoding/src/encoder/structural.rs`. The exact-format strategy in
`rust/lance-file/src/versions/v2_2/mod.rs` composes this mechanism. For complex
children such as structs or variable-size lists, Lance uses
`FixedSizeListStructuralEncoder` in
`rust/lance-encoding/src/encodings/logical/fixed_size_list.rs`.

---

## 2. On-disk layout of a single `.lance` data file (v2.x)

The Lance v2 file format writes data pages and out-of-line buffers first, then
global buffers, per-column metadata, two offset tables, and a 40-byte footer.

```
  offset 0 ─▶ ┌────────────────────────────────────────────────────┐
              │                                                    │
              │                   DATA PAGES                       │
              │  ┌──────────────────────────────────────────────┐  │
              │  │ page 0 (col A)                               │  │
              │  │  ├─ rep/def levels (null metadata)           │  │
              │  │  └─ encoded values (Value/BSS/codec as selected) │  │
              │  ├──────────────────────────────────────────────┤  │
              │  │ page 1 (col B)                               │  │
              │  │  ...                                         │  │
              │  ├──────────────────────────────────────────────┤  │
              │  │ page N (col A, chunk 2)                      │  │
              │  └──────────────────────────────────────────────┘  │
              │  Each page is 64-byte aligned (PAGE_BUFFER_ALIGNMENT│
              │  in lance-file/src/writer.rs).                     │
              │                                                    │
              ├────────────────────────────────────────────────────┤
              │              GLOBAL BUFFERS                        │
              │   shared artifacts (schema, statistics, or index   │
              │   metadata/codebooks in index files)               │
              │                                                    │
              ├────────────────────────────────────────────────────┤
              │            COLUMN METADATA (protobuf)              │
              │   page locations, encodings, and buffer refs       │
              │                                                    │
              ├────────────────────────────────────────────────────┤
              │  CMO TABLE   (column-metadata offsets)             │
              │  GBO TABLE   (global-buffer offsets)               │
              ├────────────────────────────────────────────────────┤
              │  FOOTER (fixed size, 40 bytes)                     │
              │  [meta_start | CMO | GBO | counts | ver | "LANC"] │
              └────────────────────────────────────────────────────┘
```

- **Footer magic** is `LANC` (`MAGIC` in `rust/lance-file/src/format.rs`). The
  preceding fields are three `u64` offsets, two `u32` counts, and two `u16`
  version numbers.
- **Version discrimination** (`rust/lance-file/src/version.rs`):

  | Footer `(major, minor)` | Exact persisted identity |
  |---|---|
  | `(0, 0..=2)` | `ConcreteFileVersion::V1` |
  | `(0, 3)` or `(2, 0)` | `ConcreteFileVersion::V2_0` |
  | `(2, 1)` | `ConcreteFileVersion::V2_1` |
  | `(2, 2)` | `ConcreteFileVersion::V2_2` *(current default)* |
  | `(2, 3)` | `ConcreteFileVersion::V2_3` *(unstable)* |

  `ConcreteFileVersion` represents only an exact identity read from a manifest,
  `DataFile`, or footer; it deliberately has no ordering and cannot represent
  selector aliases. Each wire location has its own codec. In particular, V2.0
  standard footers encode `(0, 3)`, embedded/mini-Lance footers encode `(2, 0)`,
  and new `DataFile` metadata encodes `(2, 0)`; readers retain both historical
  numeric forms. Manifest strings accept only canonical exact values such as
  `"2.0"`. The public `LanceFileVersion` selector
  (also in `rust/lance-file/src/version.rs`) additionally accepts `legacy`, `0.3`,
  `stable`, and `next`. `Stable` resolves to `V2_2`; `Next` resolves to `V2_3`.
  New datasets use `Stable`. `V2_3` is unstable, so its sparse structural-page
  encoding may change without migrations or compatibility fallbacks. Exact
  persisted metadata always uses canonical values rather than selector aliases.

  Upgrading the library does not change an existing dataset's write format.
  In v12, compaction can request an exact V2 output version with
  `data_storage_version`; that rewrites selected files without changing the
  manifest's default write version.

- `V2_1+` uses the **structural encoding** machinery — this is the path a
  vector column takes today. `V2_0` uses its older, non-structural strategy
  under `rust/lance-file/src/versions/v2_0/`. The frozen legacy-v1 reader and
  wire codecs live separately under `rust/lance-file/src/versions/v1/`; current
  dataset writers do not emit v1 files.

---

## 3. How a `FixedSizeList<Float32, D>` actually encodes

The encoder chosen at write time depends on the child type. For a primitive
child like `Float32`, Lance uses `PrimitiveStructuralEncoder` — *not* the
list-oriented encoder — because the whole `[f32; D]` sequence is just a flat
run of `f32` values with a known stride.

```
Input column:  [[x00, x01, …, x0(D-1)], [x10, x11, …, x1(D-1)], …]     (N rows × D dims)

Flatten (stride=D):
               [ x00  x01  …  x0(D-1)  x10  x11  …  x1(D-1)  … ]       (N·D f32 values)

Current stable format (`Stable` = `V2_2`):

     PrimitiveStructuralEncoder → dense structural pages
       (miniblock or full-zip layout, according to data and encoding metadata)

When general compression is configured for `f32` or `f64`, the strategy may
select Byte-Stream-Split (BSS) before the requested LZ4/Zstd codec:

     f32 bytes:  [b0 b1 b2 b3][b0 b1 b2 b3]…    (interleaved, little-endian)
     BSS:        [b0 b0 b0 …][b1 b1 b1 …][b2 b2 b2 …][b3 b3 b3 …]

     f32 chunks contain at most 1024 values (4 KiB); f64 chunks contain
     at most 512. A page can contain many chunks.

`V2_2` also permits automatic general compression for blocks larger than
32 KiB on its block-compression path. This does not mean every vector page
automatically receives BSS or a general-purpose codec.

     NOT applied to f32 vectors:
       • bitpacking  (integer-only; InlineBitpacking)
       • FSST        (variable-length strings)
       • dictionary  (high-cardinality floats don't benefit)
```

Relevant code:

- Logical encoder: `rust/lance-encoding/src/encodings/logical/primitive.rs`
- BSS physical codec: `rust/lance-encoding/src/encodings/physical/byte_stream_split.rs`
- Exact-format strategy / dispatch: `rust/lance-file/src/versions/v2_2/mod.rs`
  (`FieldStrategy`); shared field builders:
  `rust/lance-encoding/src/encoder/structural.rs` (`PrimitiveFieldEncoding`).
- Compression selection: `rust/lance-file/src/versions/v2_2/compression.rs`;
  shared compressors: `rust/lance-encoding/src/compression.rs`.

**Nulls.** Even though most embedding columns are non-nullable, Lance still
emits **repetition and definition levels** (Dremel-style) when the schema
permits nulls. For non-nullable vector columns the rep/def preamble is
trivial and contributes negligible space.

**All-null inner values.** The v12 encoder fixes the full-zip case where every
inner element of a fixed-size list is null. The format versioning guide records
an exception to forward compatibility for this pattern: readers predating the
fix cannot read the corrected encoding. It labels the fix `11.1.0`, but the
repository's release tags place its first stable release in v12.
See `docs/src/format/file/versioning.md` and `04-lance-versions.md` before mixing
reader versions for nullable vectors.

**Size estimate.** Before any compression:

```
  raw_bytes_per_vector = D × sizeof(T)
  raw_dataset_bytes    = N × D × sizeof(T)
```

Random embeddings have high entropy, so even a configured BSS + LZ4/Zstd
pipeline usually yields modest savings. Do not assume the default `V2_2`
writer applies general compression to every f32 vector page, and do not rely on
aggressive compression for storage cost. Use **indexing** (PQ / SQ / RQ) if the
search structure is too large, or consider writing raw values as `f16` / `bf16`
for a ~2× cut.

---

## 4. Where a vector column sits in the fragment/file hierarchy

```
 Dataset
  └─ Fragment fragId=7
      ├─ DataFile  path="data/a1b2…​.lance"   fields=[0, 1, 2]     ◀── e.g. id, text, vector
      │   ├─ column 0  (id: int64)     pages …
      │   ├─ column 1  (text: string)  pages …
      │   └─ column 2  (vector: FSL<f32,768>)  pages …            ◀── vectors live INLINE
      ├─ Overlay DataFile + coverage bitmap (optional sparse updates)
      └─ DeletionVector    (absent unless rows have been deleted)
```

Notes:

- In v2, **all columns of a fragment are typically stored in a single `.lance`
  file** (one `DataFile`). Multi-file fragments are supported (for wide schemas
  or late-added columns), but a freshly-written dataset usually has one.
- **Late column addition** (a common ML pattern: add an `embedding` column to
  an existing dataset) does produce *additional* `DataFile`s per fragment.
  Each new column group is another `(uuid).lance` file referenced by the same
  `Fragment`.
- Sparse updates may append a `DataOverlayFile`, whose coverage bitmap maps
  physical row offsets to rank-ordered values. V2 files support unequal
  column lengths so different overlay fields can cover different row sets.
- Vectors live in ordinary fixed-width numeric data pages, whether supplied by
  a base file or an overlay. The blob encoding is for variable-length binary
  such as images and audio, not fixed-size numeric lists.

`DataFile` struct — `rust/lance-table/src/format/fragment.rs`:

```
pub struct DataFile {
    pub path: String,                         // "data/<uuid>.lance"
    pub fields: Arc<[i32]>,                   // global field IDs this file holds (Arc-shared across fragments)
    pub column_indices: Arc<[i32]>,           // mapping field_id → column idx in file
    pub file_major_version: u32,              // canonical exact-version major
    pub file_minor_version: u32,              // canonical exact-version minor
    pub file_size_bytes: CachedFileSize,      // cached file size in bytes, if known
    pub base_id: Option<u32>,                 // set when the file lives outside the dataset root
}
```

`DataFile::file_version()` validates this numeric pair and returns a
`ConcreteFileVersion`; callers should not infer capabilities from numeric
ordering.

`Fragment` (same file) holds base `Vec<DataFile>`, overlay
`Vec<DataOverlayFile>`, and an optional deletion vector.

---

## 5. Walking the write path for a vector column

User code, Python:

```python
import pyarrow as pa, lance
schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("embedding", pa.list_(pa.float32(), 768)),
])
lance.write_dataset(batches, "s3://bucket/ds.lance", schema=schema)
```

Call chain (Rust side):

```
  python/src/dataset.rs (PyO3 wrapper)
        │
        ▼
  rust/lance/src/dataset/write/insert.rs
        │   InsertBuilder::execute_uncommitted
        ▼
  rust/lance/src/dataset/write.rs
        │   write_fragments_internal
        ▼
  rust/lance/src/dataset/fragment/write.rs
        │   FragmentCreateBuilder
        ▼
  rust/lance-file/src/writer.rs
        │   FileWriter::write_batch → exact Writer variant
        ▼
  rust/lance-file/src/versions/v2_2/writer.rs
        │   Writer::write_batch → ensure_initialized
        │   → shared structural writer / BatchEncoder
        ▼
  rust/lance-file/src/versions/v2_2/mod.rs
        │   FieldStrategy::create_field_encoder
        │   → PrimitiveFieldEncoding::try_create
        │     (lance-encoding/src/encoder/structural.rs)
        │   FixedSizeList<primitive> → PrimitiveStructuralEncoder
        ▼
  rust/lance-encoding/src/encodings/logical/primitive.rs
        │   Structural page pipeline:
        │     1. collect flattened values
        │     2. select page layout and value compression
        │     3. emit EncodedPage (buffers + PageEncoding protobuf)
        ▼
  rust/lance-file/src/writer/structural.rs + exact Writer
        │   write each page buffer
        │   append global buffers, column metadata, CMO/GBO tables
        │   write 40-byte footer
        ▼
  rust/lance-io/src/object_store.rs
        │   ObjectWriter / object-store abstraction (S3/GCS/Azure/local)
        ▼
  bytes on disk
```

Once the file is closed, the writer returns a `Fragment { id, files:
[DataFile{...}], ... }` to the caller. The caller (usually the `Dataset` commit
path) bundles these into a `Transaction` and writes a new manifest — see
`00-overview.md` §4 for the commit path.

---

## 6. Reading a vector column back

```
Dataset::scanner()
   .project(["embedding"])          -> FilterPlan + projection
   .limit(...)
   .try_into_stream()
```

Reader dispatch (simplified):

```
  rust/lance/src/dataset/scanner.rs
        │   plans a LanceScan execution node
        ▼
  rust/lance/src/io/exec/scan.rs
        │   per-fragment read task
        ▼
  rust/lance-file/src/reader.rs
        │   FileReader / ProjectedFileReader hold a DecodeEngine
        │   → exact-version ReadProjection prepares column metadata
        │   → DecodeEngine::read_range
        ▼
  rust/lance-encoding/src/decoder.rs
        │   schedule_and_decode
        │   → StructuralPrimitiveFieldScheduler
        │   → StructuralPrimitiveFieldDecoder
        │     (encodings/logical/primitive.rs, for FSL<primitive>)
        │   → decode the selected value/BSS/compression pipeline
        ▼
  Arrow FixedSizeListArray → emitted in the RecordBatch stream
```

Two properties worth remembering when benchmarking vector reads:

1. **Takes vs scans.** `take(row_ids)` is Lance's signature strength — a
   random-access read of N specific rows plans per-fragment IOVs directly
   against page offsets, bypassing the full-scan state machine. A full scan
   still pays the decompression cost; random access often decompresses only
   one or two pages per target row.
2. **IO scheduler.** `lance-io` has a priority/merge scheduler
   (`rust/lance-io/src/scheduler.rs`) that coalesces adjacent byte ranges
   before dispatch, avoiding many small adjacent reads against object storage.

---

## 7. Quick reference — files to know

| Concern | Path |
|---|---|
| Writer entry | `rust/lance-file/src/writer.rs` |
| Reader entry | `rust/lance-file/src/reader.rs` |
| Exact reader/writer composition | `rust/lance-file/src/versions/mod.rs` (+ `versions/v2_2/`) |
| Shared structural writer / reader | `rust/lance-file/src/writer/structural.rs`, `rust/lance-file/src/reader/structural.rs` |
| Field encoder mechanisms | `rust/lance-encoding/src/encoder/structural.rs` |
| Primitive encoder (vectors) | `rust/lance-encoding/src/encodings/logical/primitive.rs` |
| FixedSizeList encoder (complex children) | `rust/lance-encoding/src/encodings/logical/fixed_size_list.rs` |
| BSS codec | `rust/lance-encoding/src/encodings/physical/byte_stream_split.rs` |
| Bitpacking codec (integers, **not** vectors) | `rust/lance-encoding/src/encodings/physical/bitpacking.rs` |
| Compression policy / mechanisms | `rust/lance-file/src/versions/v2_2/compression.rs`, `rust/lance-encoding/src/compression.rs` |
| Public file-version selectors | `rust/lance-file/src/version.rs` |
| Exact persisted file identities | `rust/lance-file/src/version.rs` |
| Footer and magic layout | `rust/lance-file/src/format.rs` |
| DataFile / Fragment structs | `rust/lance-table/src/format/fragment.rs` |
| Manifest struct | `rust/lance-table/src/format/manifest.rs` |
| Write orchestration | `rust/lance/src/dataset/write/insert.rs` |
| Fragment builder | `rust/lance/src/dataset/fragment/write.rs` |

---

Continue to **`02-vector-indexes.md`** for how those stored vectors become
searchable at scale.
