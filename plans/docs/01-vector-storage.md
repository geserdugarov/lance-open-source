# Lance — Vector Storage Deep Dive

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
"primitive" (and therefore takes the fast encoder path) by `is_primitive_type`
in `rust/lance-encoding/src/encoder.rs`. If the child is a struct or list
(not our case), Lance falls back to `FixedSizeListStructuralEncoder` in
`rust/lance-encoding/src/encodings/logical/fixed_size_list.rs`.

---

## 2. On-disk layout of a single `.lance` data file (v2.x)

The Lance v2 file format writes data pages first, then per-column metadata,
then global buffers, then two offset tables, and finally a 16-byte footer.

```
  offset 0 ─▶ ┌────────────────────────────────────────────────────┐
              │                                                    │
              │                   DATA PAGES                       │
              │  ┌──────────────────────────────────────────────┐  │
              │  │ page 0 (col A)                               │  │
              │  │  ├─ rep/def levels (null metadata)           │  │
              │  │  └─ encoded values (BSS → LZ4 / Zstd …)      │  │
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
              │            COLUMN METADATA (protobuf)              │
              │   per column:                                      │
              │     page_locations: [(offset, length, …)]          │
              │     encoding:        PageEncoding (BSS, codec, …)  │
              │     buffer refs  →   GLOBAL BUFFERS                │
              │                                                    │
              ├────────────────────────────────────────────────────┤
              │              GLOBAL BUFFERS                        │
              │   shared artefacts (e.g. PQ codebooks in index     │
              │   files, statistics, schema metadata blobs)        │
              │                                                    │
              ├────────────────────────────────────────────────────┤
              │  CMO TABLE   (column-metadata offsets)             │
              │  GBO TABLE   (global-buffer offsets)               │
              ├────────────────────────────────────────────────────┤
              │  FOOTER (fixed size, 16 bytes)                     │
              │    [cmo_ptr | gbo_ptr | major | minor | "LANC"]    │
              └────────────────────────────────────────────────────┘
```

- **Footer magic** is `LANC` (`MAGIC` in `rust/lance-file/src/format.rs:33`).
- **Version discrimination** (`rust/lance-file/src/reader.rs:234-238`):

  | `(major, minor)` | Version enum |
  |---|---|
  | `(0, 3)` or `(2, 0)` | `LanceFileVersion::V2_0` |
  | `(2, 1)` | `V2_1` *(current default / `Stable`)* |
  | `(2, 2)` | `V2_2` |
  | `(2, 3)` | `V2_3` *(resolves from `Next`; marked unstable)* |

  Stability in `LanceFileVersion` (`rust/lance-encoding/src/version.rs`) is
  decided by `is_unstable()`, which returns `self >= Next` — so `Next` itself
  and every version above it (today `V2_3`) are unstable, while everything
  below `Next` (`Legacy`, `V2_0`, `V2_1`, `Stable`, `V2_2`) is stable. `Stable`
  resolves to the enum default (`V2_1`); `Next` resolves to `V2_3`. New
  datasets are written at the `Stable` version.

- `V2_1+` uses the **structural encoding** machinery — this is the path a
  vector column takes today. Older files (`V2_0`) use a legacy layout under
  `rust/lance-file/src/previous/`.

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

Per encoding chunk (≤ 1024 values for f32 → 4 KiB):

     Byte-Stream-Split (BSS)
     ───────────────────────
     f32 bytes:  [b0 b1 b2 b3][b0 b1 b2 b3]…    (interleaved, little-endian)
     BSS:        [b0 b0 b0 …][b1 b1 b1 …][b2 b2 b2 …][b3 b3 b3 …]

     Rationale: each byte stream has lower entropy than raw floats,
     so a block codec (LZ4 / Zstd) compresses it better.

     Block codec (LZ4 / Zstd)
     ────────────────────────
     Each of the 4 byte streams is compressed independently.

     Result: one MiniBlock → one page buffer.

     NOT applied to f32 vectors:
       • bitpacking  (integer-only; InlineBitpacking)
       • FSST        (variable-length strings)
       • dictionary  (high-cardinality floats don't benefit)
```

Relevant code:

- Logical encoder: `rust/lance-encoding/src/encodings/logical/primitive.rs`
- BSS physical codec: `rust/lance-encoding/src/encodings/physical/byte_stream_split.rs`
- Encoder strategy / dispatch: `rust/lance-encoding/src/encoder.rs` (look for
  `StructuralEncodingStrategy` and `is_primitive_type`).
- Block compressors: `rust/lance-encoding/src/compression.rs`

**Nulls.** Even though most embedding columns are non-nullable, Lance still
emits **repetition and definition levels** (Dremel-style) when the schema
permits nulls. For non-nullable vector columns the rep/def preamble is
trivial and contributes negligible space.

**Size estimate.** Before any compression:

```
  raw_bytes_per_vector = D × sizeof(T)
  raw_dataset_bytes    = N × D × sizeof(T)
```

Typical end-to-end compression for random embeddings is modest (5–15 %
reduction). Embeddings have high entropy; BSS + LZ4 mostly recoup the
sign-bit and exponent regularities of `f32`. Do not rely on aggressive
compression for storage cost — rely on **indexing** (PQ / SQ / RQ) if the raw
column is too large, or consider writing as `f16` / `bf16` for a ~2× cut.

---

## 4. Where a vector column sits in the fragment/file hierarchy

```
 Dataset
  └─ Fragment fragId=7
      ├─ DataFile  path="data/a1b2…​.lance"   fields=[0, 1, 2]     ◀── e.g. id, text, vector
      │   ├─ column 0  (id: int64)     pages …
      │   ├─ column 1  (text: string)  pages …
      │   └─ column 2  (vector: FSL<f32,768>)  pages …            ◀── vectors live INLINE
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
- Vectors are **always inline in data pages**. There is no special "blob"
  encoding for vectors; blob encoding exists in `rust/lance-encoding` but is
  reserved for *variable-length* binary (images, audio, etc.), not
  fixed-size numeric lists.

`DataFile` struct — `rust/lance-table/src/format/fragment.rs`:

```
pub struct DataFile {
    pub path: String,                         // "data/<uuid>.lance"
    pub fields: Arc<[i32]>,                   // global field IDs this file holds (Arc-shared across fragments)
    pub column_indices: Arc<[i32]>,           // mapping field_id → column idx in file
    pub file_major_version: u32,              // e.g. 2
    pub file_minor_version: u32,              // e.g. 3
    pub file_size_bytes: CachedFileSize,      // cached file size in bytes, if known
    pub base_id: Option<u32>,                 // set when the file lives outside the dataset root
}
```

`Fragment` (same file) holds a `Vec<DataFile>` plus optional deletion vector.

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
        │   FileWriter::write_batch
        │   → ensure_initialized (field encoders)
        │   → BatchEncoder::encode_batch
        ▼
  rust/lance-encoding/src/encoder.rs
        │   StructuralEncodingStrategy::create_field_encoder
        │     match field.data_type():
        │       FixedSizeList<primitive>  →  PrimitiveStructuralEncoder
        ▼
  rust/lance-encoding/src/encodings/logical/primitive.rs
        │   MiniBlockCompressor pipeline:
        │     1. collect values into a chunk (≤1024 f32)
        │     2. ByteStreamSplitEncoder
        │     3. block codec (LZ4/Zstd)
        │     4. emit EncodedPage (buffers + PageEncoding protobuf)
        ▼
  back in FileWriter
        │   write_buffer() for each page buffer
        │   append column metadata, global buffers, CMO/GBO tables
        │   write 16-byte footer
        ▼
  rust/lance-io/src/object_store.rs
        │   ObjectStore::put_opts (S3/GCS/Azure/local)
        ▼
  bytes on disk
```

Once the file is closed, the writer returns a `Fragment { id, data_files:
[DataFile{...}] }` to the caller. The caller (usually the `Dataset` commit
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
        │   FileReadCore::read_range  (held by FileReader as its `core`)
        │   → FileDecoder (structural path since V2_1)
        ▼
  rust/lance-encoding/src/decoder.rs
        │   per-column PageDecoder
        │   → PrimitiveStructuralDecoder (for FSL<primitive>)
        │   → BSS decoder + codec decompression
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
   before dispatch. For cold queries against object storage, the scheduler is
   often the difference between 10 ms and 100 ms per page.

---

## 7. Quick reference — files to know

| Concern | Path |
|---|---|
| Writer entry | `rust/lance-file/src/writer.rs` |
| Reader entry | `rust/lance-file/src/reader.rs` |
| Encoder strategy / dispatch | `rust/lance-encoding/src/encoder.rs` |
| Primitive encoder (vectors) | `rust/lance-encoding/src/encodings/logical/primitive.rs` |
| FixedSizeList encoder (complex children) | `rust/lance-encoding/src/encodings/logical/fixed_size_list.rs` |
| BSS codec | `rust/lance-encoding/src/encodings/physical/byte_stream_split.rs` |
| Bitpacking codec (integers, **not** vectors) | `rust/lance-encoding/src/encodings/physical/bitpacking.rs` |
| Block compressors (LZ4/Zstd) | `rust/lance-encoding/src/compression.rs` |
| File version constants | `rust/lance-file/src/format.rs` |
| DataFile / Fragment structs | `rust/lance-table/src/format/fragment.rs` |
| Manifest struct | `rust/lance-table/src/format/manifest.rs` |
| Write orchestration | `rust/lance/src/dataset/write/insert.rs` |
| Fragment builder | `rust/lance/src/dataset/fragment/write.rs` |

---

Continue to **`02-vector-indexes.md`** for how those stored vectors become
searchable at scale.
