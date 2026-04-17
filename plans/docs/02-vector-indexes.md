# Lance — Vector Indexes: Algorithms & Composition

**Scope.** The algorithmic content of Lance's vector index family. For how
the resulting index is laid out on disk and how a query executes against it,
see `03-index-on-disk-and-search.md`.

**Audience.** Contributors adding new index types, tuning defaults,
implementing new distance kernels, or diagnosing recall / latency issues.

---

## 1. The index-type taxonomy

Authoritative enum: `IndexType` in `rust/lance-index/src/lib.rs` (lines ~104–138).

```
                        VECTOR INDEX TYPES
                        ──────────────────

              ┌───────────────────────────────────┐
              │         All are IVF-based         │
              │  (no pure HNSW without IVF today) │
              └──────────────┬────────────────────┘
                             │
      ┌──────────────────────┼────────────────────────────────┐
      │                      │                                │
   IVF_FLAT              IVF_<Q>                       IVF_HNSW_<Q>
   (exact within        (quantized within            (graph within
    partition)           partition, flat)              partition,
                                                       quantized)

   IVF_FLAT         IVF_SQ   IVF_PQ   IVF_RQ      IVF_HNSW_FLAT
                                                   IVF_HNSW_SQ
                                                   IVF_HNSW_PQ
```

Full list with numeric codes and strings:

| Code | `IndexType` variant | String | Composition |
|---:|---|---|---|
| 101 | `IvfFlat` | `"IVF_FLAT"` | IVF + brute-force within partition |
| 102 | `IvfSq` | `"IVF_SQ"` | IVF + scalar quantization (default 8-bit) |
| 103 | `IvfPq` | `"IVF_PQ"` | IVF + product quantization (default 16 subvectors × 8 bits) |
| 104 | `IvfHnswSq` | `"IVF_HNSW_SQ"` | IVF + HNSW graph + SQ |
| 105 | `IvfHnswPq` | `"IVF_HNSW_PQ"` | IVF + HNSW graph + PQ |
| 106 | `IvfHnswFlat` | `"IVF_HNSW_FLAT"` | IVF + HNSW graph + exact distances |
| 107 | `IvfRq` | `"IVF_RQ"` | IVF + RaBitQ (rotated bit quantization) |

Mental model: every index has an **outer IVF** (a top-level k-means
partitioning) and then **two orthogonal choices inside each partition**:

```
    IVF  ×  { Flat | HNSW }  ×  { Flat | SQ | PQ | RaBitQ }
    ───      ───────────────     ─────────────────────────
    outer     sub-index            quantizer
    partitioning
```

Not every combination is exposed (`HNSW + Flat` = exact; `HNSW + Flat`
without IVF is not currently supported). The concrete seven variants in the
table above are the ones the enum recognizes.

---

## 2. IVF — the outer partitioning

```
   High-dim space (schematic 2-D view)

     ┌─────────────────────────────────────────────────┐
     │   ·  ·   · C0·               · · · C1 ·· ·      │
     │    · ·  ·   ·  ·               · ·  ·   ·       │
     │  · ·     ·                   · · · ·· ·  ·      │
     │         · ·                   ·  · ·            │
     │                                                 │
     │       · · ·  · C2 · ·      ·  C3  ·  ·          │
     │       ·  ·  ·    · ·        ·  ·  ·  ·  ·       │
     │        ·  ·    · ·           ·    · ·           │
     │                                                 │
     │   ·  ·   · · C4 ·    ·    · · C5 · ·  ·         │
     │       ·                     · ·  ·  · ·         │
     └─────────────────────────────────────────────────┘

   Centroids C0 … C(k-1) are computed by k-means on a training sample.
   Every vector is assigned to the partition of its NEAREST centroid.
```

**Struct:** `IvfModel` — `rust/lance-index/src/vector/ivf/storage.rs` (≈ line 27)

```
pub struct IvfModel {
    pub centroids: Option<FixedSizeListArray>,  // shape: (k × D) of Float32
    pub offsets:   Vec<usize>,                   // byte offset of partition p in storage
    pub lengths:   Vec<u32>,                     // number of vectors in partition p
    pub loss:      Option<f64>,                  // k-means training loss
}
```

**Build parameters** — `IvfBuildParams` in `rust/lance-index/src/vector/ivf/builder.rs`:

| Field | Default | Meaning |
|---|---:|---|
| `num_partitions` | `None` *(deprecated)* | Explicit `k`. If unset, derived from `target_partition_size`. |
| `target_partition_size` | `None` *(per-variant default)* | Target rows/partition (IVF_FLAT≈4096, IVF_SQ/PQ≈8192, IVF_HNSW_*≈1 M) |
| `max_iters` | 50 | k-means iterations |
| `centroids` / `retrain` | `None` / `false` | Bring your own IVF centroids; opt into retraining from them. |
| `sample_rate` | 256 | Rows sampled **per centroid** for training (so total sample ≈ `256 × k`) |

Distance type is **not** an `IvfBuildParams` field; it is plumbed through the
higher-level index-creation API (the `DistanceType` carried by the create-index
request and by the chosen quantizer).

**k-means core** — `rust/lance-index/src/vector/kmeans.rs` (with linalg kernels
from `rust/lance-linalg/`). Supports `DistanceType::{L2, Cosine, Dot}`
via `DistanceType` enum. Cosine is converted to L2 over unit-normalized
vectors internally.

**Why these target partition sizes?** The partition size governs the
recall/latency trade-off at query time:
- Smaller partitions → cheaper per-partition scan but more partitions to
  probe (`nprobes`) for the same recall.
- Larger partitions → pays off when there is a graph inside (HNSW) that
  amortises partition size (hence 1 M rows for IVF_HNSW_*).

---

## 3. PQ — Product Quantization

Compresses each D-dim vector into **M bytes** (default M=16) by independently
quantizing M non-overlapping sub-vectors against per-sub-vector codebooks of
256 centroids each.

```
   Original vector (D = 96, for diagram brevity)

   │← 12 ─→│← 12 ─→│← 12 ─→│ ... 8 sub-vectors of 12 dims each (M=8)
   ┌───────┬───────┬───────┬───┬───────┐
   │  s0   │  s1   │  s2   │ … │  s7   │
   └───────┴───────┴───────┴───┴───────┘

   For EACH sub-vector position m in {0..M}, train a small k-means
   with 256 centroids (= 2^nbits, with nbits=8 default):

     Codebook m:   c_m_0, c_m_1, …, c_m_255      (each is a 12-dim vector)

   Then quantize each sub-vector to the ID of its nearest centroid:

   ┌────┬────┬────┬───┬────┐
   │ q0 │ q1 │ q2 │ … │ q7 │    each q in [0,255]  →  1 byte
   └────┴────┴────┴───┴────┘

   Storage per vector = M bytes  (here 8, default 16).
   Codebook memory    = M × 256 × (D/M) × 4 bytes (float32)
                       = for D=768, M=16  →  16 × 256 × 48 × 4 B ≈ 768 KiB
```

**Struct:** `ProductQuantizer` — `rust/lance-index/src/vector/pq.rs`

```
pub struct ProductQuantizer {
    pub num_sub_vectors: usize,               // M
    pub num_bits:        u32,                 // typically 8
    pub dimension:       usize,               // D — vector dimensionality
    pub codebook:        FixedSizeListArray,  // flattened (M × 256 × D/M) f32
    pub distance_type:   DistanceType,
    // L2 fast-path: pre-transposed centroids for batched FMA (crate-private).
    // Populated only for f32 codebooks under L2 (Cosine is converted to L2).
    l2_targets:          Option<Arc<Vec<L2Prepared>>>,
}
```

**Build params** — `PQBuildParams`:

| Field | Default | Meaning |
|---|---:|---|
| `num_sub_vectors` | 16 | `M` — must divide `D` |
| `num_bits` | 8 | 256 centroids per sub-vector. 4-bit is *possible* but rarely used — prefer `IVF_RQ` for aggressive compression. |
| `max_iters` | 50 | per-sub-vector k-means iterations |
| `sample_rate` | 256 | training rows per codebook centroid |

**Distance at query time.** PQ uses **asymmetric distance**: the query is
full-precision, each sub-vector distance to each codebook centroid is
precomputed into a `M × 256` lookup table, and scoring a stored vector is
M table lookups plus a sum. Code: `rust/lance-index/src/vector/pq/distance.rs`
(`build_distance_table_l2`, `build_distance_table_dot`).

```
  Query q                Codebook m (256 centroids)
  ┌────────┐             ┌─────┐┌─────┐ … ┌─────┐
  │ q[mth] │──── dist ──▶│  0  ││  1  │   │ 255 │
  └────────┘             └─────┘└─────┘   └─────┘
                            │    │           │
                            ▼    ▼           ▼
                         ┌────────────────────────┐
                         │  dist_table[m][0..256] │   <-- M such tables
                         └────────────────────────┘

  Score stored vector v (its codes = q0, q1, …, q(M-1)):

     score(q, v) = Σ_m  dist_table[m][ v.codes[m] ]
```

---

## 4. SQ — Scalar Quantization

Per-dimension `int8` (default). Simpler than PQ, 1 byte per dim, no
cross-dimensional interaction.

```
  Per dimension d, compute global min/max over the training sample
  (or a per-dimension range). Then map:

       int8(x_d) = clamp( (x_d - min_d) / (max_d - min_d) * 255, 0, 255 )

  Storage = D bytes per vector (vs 4 D for f32).
```

**Struct:** `ScalarQuantizer` — `rust/lance-index/src/vector/sq.rs`

```
pub struct ScalarQuantizer {
    metadata: ScalarQuantizationMetadata,    // module-private field
}

// rust/lance-index/src/vector/sq/storage.rs
pub struct ScalarQuantizationMetadata {
    pub dim:      usize,
    pub num_bits: u16,         // 8 default (4 also present)
    pub bounds:   Range<f64>,  // populated lazily from the training sample
}
```

**Build params** — `SQBuildParams`:

| Field | Default |
|---|---:|
| `num_bits` | 8 |
| `sample_rate` | 256 |

Trade-off: SQ preserves per-dimension structure (PQ doesn't) so it can
re-rank better with a small overhead, but PQ's per-sub-vector codebooks
achieve far higher compression (16 B vs 768 B per 768-dim vector) at the
cost of accuracy.

---

## 5. HNSW — Hierarchical Navigable Small World

A graph-based ANN algorithm. Inside a partition, HNSW builds a multi-layer
proximity graph; search is a greedy descent from an entry point.

```
  Conceptual HNSW tower (3 layers shown; max_level default = 7)

   Layer 2 (sparse)
        ●───────────────●───────────────●
        │               │               │
        │               │               │
   Layer 1
        ●──●─────●──●─────●──●────●──●─●
        │  │     │  │     │  │    │  │ │
        │  │     │  │     │  │    │  │ │
   Layer 0 (all nodes, densest)
     ●─●─●─●─●─●─●─●─●─●─●─●─●─●─●─●─●─●─●
     └───────────────────────────────────┘
            each node has ~M neighbours on its layer

   Search (top-down):
     start at ENTRY_POINT (topmost layer)
     on each layer: greedy walk toward the query, keep nearest
     drop down a layer, repeat
     on layer 0: run full beam search with width `ef_search`
```

**Struct:** `HNSW` — `rust/lance-index/src/vector/hnsw/builder.rs`

**Build parameters** — `HnswBuildParams`:

| Field | Default | Meaning |
|---|---:|---|
| `m` | 20 | Out-degree per node per layer |
| `ef_construction` | 150 | Build-time beam width |
| `max_level` | 7 | Maximum layer index |
| `prefetch_distance` | `Some(2)` | CPU prefetch lookahead while walking neighbours |

Runtime-only: `ef_search` — the beam width at query time, the primary
recall/latency knob. Typically set on the query, not the index.

**Integration.** HNSW in Lance does not store raw vectors for distances —
it delegates distances to whatever **quantizer** is paired with it
(Flat, PQ, SQ). The struct composition for the HNSW-based composites is:

```
    HNSWIndex<Q>     where Q ∈ { Flat, ProductQuantizer, ScalarQuantizer }
    ────────────
     • Graph topology (layers, neighbour lists)
     • Quantizer Q for distance computation
     • Reference back to IVF partition
```

Path: `rust/lance-index/src/vector/hnsw/index.rs`.

---

## 6. RaBitQ — rotated bit quantization (`IVF_RQ`)

Recent addition (see commits `0108b96c`, `8f479dbf` and neighbours). Projects
vectors through a randomized rotation and then keeps only the **sign bits**
(or top-4 bits) per rotated dimension.

```
   x  (D-dim f32)
       │
       ▼   R: D×D rotation matrix (or fast FHT-KAC)
     R·x
       │
       ▼   sign(·)  (binary) or top-4-bits per dim (4-bit)
   binary-code   (D / 8 bytes for 1-bit;  D / 2 bytes for 4-bit)

  Distance approximation:
     Hamming-like on binary codes, or table-based 4-bit lookup.
     Final re-rank against full vectors (refine) is recommended.
```

**Struct:** `RabitQuantizer` — `rust/lance-index/src/vector/bq/builder.rs`

**Build params** — `RabitBuildParams`:

| Field | Default | Meaning |
|---|---:|---|
| `num_bits` | 1 | `1` (binary) or `4`. `IVF_RQ` typically uses 4. |
| `rotation_type` | `Fast` | `Fast` (FHT-KAC, O(D log D)) or `Matrix` (dense, O(D²)) |

Why it exists: at 4 bits/dim on a 768-dim vector it is ~2× smaller than PQ
with 16 × 8 bits, with different approximation characteristics. The 4-bit
distance kernel has ARM NEON SIMD for ~16× speed-up.

---

## 7. How the composites fit together

The generic builder is `IvfIndexBuilder<S, Q>` in
`rust/lance/src/index/vector/builder.rs`, parameterized on the sub-index type
and quantizer type:

```
           IvfIndexBuilder<S, Q>
                 │
                 │   S = sub-index type         Q = quantizer
                 │       ┌──────────┐              ┌──────────────┐
                 │       │  FLAT    │              │  Flat        │
                 │       │  HNSW    │              │  PQ          │
                 │       └──────────┘              │  SQ          │
                 │                                 │  RaBitQ      │
                 │                                 └──────────────┘
                 │
                 │  The seven exposed variants pick specific (S, Q):
                 │
                 │    IVF_FLAT       (Flat, Flat)
                 │    IVF_SQ         (Flat, SQ)
                 │    IVF_PQ         (Flat, PQ)
                 │    IVF_RQ         (Flat, RaBitQ)
                 │    IVF_HNSW_FLAT  (HNSW, Flat)
                 │    IVF_HNSW_SQ    (HNSW, SQ)
                 │    IVF_HNSW_PQ    (HNSW, PQ)
                 ▼
                                   IVF Partitions
                                        │
               ┌────────────────────────┼───────────────────────┐
               ▼                        ▼                       ▼
          partition 0               partition 1           partition k-1
          ┌───────────────┐         ┌───────────────┐     ┌───────────────┐
          │ Sub-index S   │         │ Sub-index S   │     │ Sub-index S   │
          │ (Flat/HNSW)   │         │ (Flat/HNSW)   │ ... │ (Flat/HNSW)   │
          │ over codes Q  │         │ over codes Q  │     │ over codes Q  │
          └───────────────┘         └───────────────┘     └───────────────┘
```

**Worked example: `IVF_HNSW_PQ`**, defaults, D = 768, N = 10 M vectors, L2:

```
  k  (partitions)     = N / target_partition_size
                      = 10_000_000 / 1_000_000 = 10

  IVF centroids       = 10 × 768 f32           = 30 KiB
  PQ codebook         = 16 × 256 × 48 f32      ≈ 768 KiB
  Per-vector PQ code  = 16 B
  Per-partition HNSW  ≈ N / k = 1 M nodes × (m=20 outgoing + fanout overhead)
                      ≈ roughly 200 MB of graph data per partition
  Per-partition total ≈ 16 MB PQ codes + 200 MB HNSW edges
  Whole index         ≈ 10 × ~216 MB            ≈ 2.2 GiB
```

(Exact sizes depend on HNSW's max_level distribution and per-layer `M_max`.
Treat the above as a back-of-the-envelope.)

---

## 8. Distance metrics and SIMD

**`DistanceType`** enum — `rust/lance-linalg/src/distance.rs`:

| Variant | Arrow types with kernels | Typical use |
|---|---|---|
| `L2` | f16, bf16, f32, f64, u8 | default for text/image embeddings |
| `Cosine` | f16, bf16, f32, f64 | on non-normalized vectors |
| `Dot` | f16, bf16, f32, f64 | when vectors are unit-normalized |
| `Hamming` | u8 | binary vectors (RaBitQ 1-bit codes) |

Kernels live per metric in `rust/lance-linalg/src/distance/{l2, cosine, dot,
hamming}.rs`, and per element-type in `rust/lance-linalg/src/simd/`
(`f32.rs`, `f64.rs`, `u8.rs`, `i32.rs`, `dist_table.rs`). Architectures
covered: `x86_64` (SSE / AVX2 / AVX-512 via target features),
`aarch64` (NEON), `loongarch64`.

**Recent kernels (cross-check commit log):**

- `perf: speed up RaBitQ 4-bit LUT distance on ARM by 16x` (`0108b96c`)
- `perf: add SIMD kernels for bf16 distance functions` (`d0124edf`)
- `perf: add SIMD-accelerated u8 dot product for SQ distance` (`8f479dbf`)
- `perf: add explicit SIMD types and distance kernels for f64` (`c913ff8f`)

---

## 9. The build pipeline in detail

Happens when the user calls `dataset.create_index(column="embedding",
index_type="IVF_HNSW_PQ", num_partitions=…, num_sub_vectors=…, …)`.

```
 Phase    What runs                                        Where
 ─────    ────────────────────────────────────             ──────────────────────────────
 (a)      Sample training rows                              lance/src/index/vector/builder.rs
 (b)      k-means → IvfModel centroids                      lance-index/src/vector/kmeans.rs
 (c)      Assign all rows to nearest centroid               lance-linalg SIMD L2/Cos/Dot
 (d)      Shuffle rows into per-partition Lance files       lance-index/src/vector/v3/shuffler.rs
 (e)      Train quantizer (PQ / SQ / RQ)
             • sample per-partition
             • per-sub-vector k-means (for PQ)              lance-index/src/vector/pq/builder.rs
 (f)      Encode every row into quantized code              (SIMD inner loops)
 (g)      Build sub-index per partition
             • Flat: just store codes contiguously
             • HNSW: insert codes layer by layer with
               ef_construction beam search                  lance-index/src/vector/hnsw/builder.rs
 (h)      Write segments under _indices/<uuid>/             lance-file / lance-index storage
 (i)      Commit new Manifest w/ IndexMetadata +
             fragment_bitmap of covered fragments           lance-table::format::manifest
```

Output: the shiny new index directory and a new manifest version. Fragments
added *after* the build are simply not in the bitmap; query time will flat-scan
those (the "delta" handling described in `03-index-on-disk-and-search.md` §5).

---

## 10. Tunables cheat-sheet

Recall and latency are three-way knobs: IVF, quantizer, HNSW. Typical
starting points for 768-dim text embeddings over ≤ 10 M rows:

| Param | Starting value | Direction for more recall | Direction for more speed |
|---|---|---|---|
| `num_partitions` (IVF) | `sqrt(N)` | — | — |
| `target_partition_size` | 8192 (flat), 1 M (HNSW) | larger | smaller |
| `nprobes` (query-time) | 20 | ↑ | ↓ |
| `num_sub_vectors` (PQ) | 16 (if D=768) | ↑ (must divide D) | ↓ |
| `num_bits` (PQ) | 8 | — | — |
| `m` (HNSW) | 20 | ↑ | ↓ |
| `ef_construction` | 150 | ↑ | build only |
| `ef_search` | 2·k | ↑ | ↓ |
| `refine_factor` (query-time) | 10 | ↑ | ↓ |

`refine_factor` is not an index parameter — it is applied at the scanner
level (see `03-index-on-disk-and-search.md` §6). It fetches `k × factor`
approximate results then re-ranks them exactly using the raw vectors from
the `DataFile`.

---

## 11. Quick reference — files to know

| Concern | Path |
|---|---|
| Top-level `IndexType` enum | `rust/lance-index/src/lib.rs` |
| Generic builder | `rust/lance/src/index/vector/builder.rs` |
| Logical vector index | `rust/lance/src/index/vector.rs` |
| IVF model | `rust/lance-index/src/vector/ivf/storage.rs` |
| IVF build params | `rust/lance-index/src/vector/ivf/builder.rs` |
| PQ | `rust/lance-index/src/vector/pq.rs` (+ `pq/builder.rs`, `pq/distance.rs`, `pq/storage.rs`) |
| SQ | `rust/lance-index/src/vector/sq.rs` (+ `sq/builder.rs`, `sq/storage.rs`) |
| HNSW builder | `rust/lance-index/src/vector/hnsw/builder.rs` |
| HNSW index integration | `rust/lance-index/src/vector/hnsw/index.rs` |
| RaBitQ / BQ | `rust/lance-index/src/vector/bq/builder.rs` (+ `bq/rotation.rs`) |
| k-means | `rust/lance-index/src/vector/kmeans.rs` |
| Shuffler | `rust/lance-index/src/vector/v3/shuffler.rs` |
| `IvfSubIndex` trait | `rust/lance-index/src/vector/v3/subindex.rs` |
| Distance enum + kernels | `rust/lance-linalg/src/distance.rs` (+ `distance/*.rs`) |
| SIMD | `rust/lance-linalg/src/simd.rs` (+ `simd/*.rs`) |

---

Continue to **`03-index-on-disk-and-search.md`** to see how these indexes
are physically stored and how a `nearest(...)` query executes.
