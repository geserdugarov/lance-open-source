# Multi-bit RaBitQ after rebasing to latest main

_Issue: investigate whether multi-bit (4/8-bit) RaBitQ is available, check
whether it landed in Lance 8.0, and describe how it is implemented._

## TL;DR

There are now three relevant states to keep separate:

- **1-bit RaBitQ (classic).** `num_bits = 1`; still the default and still stored
  as the binary sign plane in `_rabit_codes`.
- **Historical v7 branch behavior: multi-bit by padding.** The previous branch
  accepted `num_bits > 1` by expanding `code_dim = dim * num_bits` and storing
  extra random-hyperplane sign bits. That improved recall by spending more sign
  projections, but it was not the Extended RaBitQ per-dimension code.
- **Current branch: Extended / B-bit RaBitQ.** `code_dim = dim`;
  `num_bits` is validated in `1..=9`; the sign bit is stored in `_rabit_codes`;
  the remaining `num_bits - 1` bits are stored in `__blocked_ex_codes`; query
  uses binary FastScan, lower-bound pruning, and exact ex-code rerank.

## Current implementation on main

The current implementation matches the Extended RaBitQ shape that landed in the
8.0 line, with additional main-branch integration around distributed builds,
search scratch buffers, and tests.

### Bit budget and validation

- `RABIT_MIN_NUM_BITS = 1`, `RABIT_MAX_NUM_BITS = 9`, and
  `RABIT_BINARY_NUM_BITS = 1` are defined in `rust/lance-index/src/vector/bq.rs`.
- `validate_rq_num_bits(num_bits)` rejects values outside `1..=9` with
  `IVF_RQ num_bits must be in 1..=9`.
- `rabit_ex_bits(num_bits)` returns `num_bits - 1`, so 1-bit RaBitQ has no ex
  column and multi-bit RaBitQ has one binary sign bit plus ex bits.

### Encoding

- `RabitQuantizer::new_with_rotation` sets `code_dim = dim`, not
  `dim * num_bits`.
- `transform_split` rotates each residual into `code_dim` coordinates, packs the
  binary sign bits into `_rabit_codes`, and only creates ex-code buffers when
  `num_bits > 1`.
- `quantize_ex_code` implements the per-vector Extended RaBitQ scalar
  quantization. It searches a rescale factor, quantizes absolute normalized
  rotated coordinates into low bits, complements those low bits for negative
  coordinates, and combines them with the sign bit into the signed grid used for
  the ex factors.
- Ex codes are written in the blocked SIMD layout with
  `ex_dot::pack_blocked_row`.

### On-disk columns

- `_rabit_codes`: the binary sign plane, present for both 1-bit and multi-bit
  indexes.
- `__blocked_ex_codes`: the current multi-bit ex-code column and the format new
  writes emit.
- `__ex_codes`: legacy sequential ex-code layout. Current code reads it only as
  a compatibility fallback and repacks it to `__blocked_ex_codes` on load.
- `__add_factors` and `__scale_factors`: binary-plane reconstruction factors.
- `__add_factors_ex` and `__scale_factors_ex`: ex-code reconstruction factors,
  present for `num_bits > 1`.
- `__error_factors`: lower-bound error term used by the raw-query pruning path.

### Query path

For multi-bit raw-query searches:

1. Binary FastScan computes the binary-plane estimate from `_rabit_codes`.
2. The pruning path computes a lower bound from the binary estimate, query
   factor, and `__error_factors`, then compares it with the user bounds and the
   current top-k heap threshold.
3. Surviving rows are reranked with exact ex-code dot products from
   `__blocked_ex_codes` and the ex reconstruction factors.

`ApproxMode::Fast`, residual-query metadata, `num_bits <= 1`, or missing error
factors bypass the lower-bound gating path.

## Difference from the old v7 branch state

| Behavior | v7-era branch before rebase | Current branch after rebase |
| --- | --- | --- |
| `code_dim` | `dim * num_bits` | `dim` |
| Extra bits | Extra sign projections | Per-dimension ex bits |
| `num_bits` range | Not consistently validated | `1..=9` |
| Multi-bit columns | Only wider `_rabit_codes` plus binary factors | `_rabit_codes`, `__blocked_ex_codes`, ex factors, error factors |
| Query | Binary FastScan over expanded codes | Binary FastScan, lower-bound prune, ex-code rerank |
| Compatibility | Padding-era multi-bit layout | 1-bit compatible; legacy sequential ex-code readable |

The padding behavior should now be treated as a historical layout. A
padding-era multi-bit index has `num_bits > 1` but no ex-code column; current
code should reject it instead of silently interpreting the expanded sign bits as
Extended RaBitQ. Existing 1-bit indexes remain a clean subset because they only
require `_rabit_codes` and the binary factors.

## Tests 

- Unit tests validate `num_bits` bounds and split-code byte sizing.
- Builder tests cover invalid `num_bits`, supplied rotation metadata mismatches,
  and `code_dim = dim` for fast and matrix rotations.
- Storage tests cover ex-code dot kernels, blocked/sequential ex-code loading,
  remap preservation of split columns, pruning semantics, and multi-bit distance
  paths.
- IVF tests cover multi-bit index creation, persisted `__blocked_ex_codes`, ex
  factors, and search recall for `num_bits` values including 4, 6, and 9.
- Python vector-index tests include 9-bit IVF_RQ recall and stats checks.
- Distributed index merger tests cover multi-bit RQ metadata and split ex-code
  column handling.

## Remaining work

1. **Compatibility policy for padding-era indexes.** Keep the current
   fail-closed behavior for `num_bits > 1` indexes without `__blocked_ex_codes`
   or add an explicit metadata version/migration path if those experimental
   indexes must be supported.
2. **Public docs.** Update user-facing IVF_RQ docs to describe the current
   Extended RaBitQ layout and valid `num_bits` range. Avoid saying multi-bit is
   implemented by padding.
3. **Benchmark refresh.** Extend RaBitQ benchmarks beyond 1-bit and report
   recall/latency trade-offs for common bit budgets such as 4, 8, and 9 bits.
4. **Binding consistency.** Keep Rust, Python, and Java parameter names and
   validation behavior aligned; bindings should stay thin wrappers around the
   Rust validation and implementation.

## Source touchpoints

- `rust/lance-index/src/vector/bq.rs`: validation constants,
  `validate_rq_num_bits`, `rabit_ex_bits`, code-byte helpers.
- `rust/lance-index/src/vector/bq/builder.rs`: `code_dim = dim`,
  `transform_split`, `quantize_ex_code`, blocked ex-code writing.
- `rust/lance-index/src/vector/bq/storage.rs`: RQ columns, legacy ex-code
  normalization, query estimator, pruning counters, raw-query multi-bit scan.
- `rust/lance-index/src/vector/bq/transform.rs`: binary/ex/error factor
  computation.
- `rust/lance-index/src/vector/bq/ex_dot.rs`: blocked ex-code layout and
  query-times-ex-code dot kernels.
- `rust/lance-index/src/vector/bq/dist_table_quant.rs`: FastScan distance-table
  quantization.
- `rust/lance-index/src/vector/bq/prune.rs`: SIMD/portable lower-bound pruning
  masks.
- `rust/lance/src/index/vector/ivf/v2.rs`: IVF_RQ search integration, scratch
  sizing, and multi-bit persisted-code search tests.
- `python/src/dataset.rs` and `python/src/indices.rs`: Python entry points for
  `num_bits` and shared `rabitq_model` creation.

## Implementation PRs

- [#7021: refactor(index): scaffold ivfrq split code layout](https://github.com/lance-format/lance/pull/7021)
  prepared the binary/ex-code split layout, validation helpers, and metadata
  scaffolding while keeping only `num_bits = 1` enabled.
- [#7038: feat(index): support multi-bit IVF_RQ storage](https://github.com/lance-format/lance/pull/7038)
  added the multi-bit split-code quantization/storage path, ex-code columns, and
  ex reconstruction factors for `num_bits = 2..=9`.
- [#7078: feat(index): support raw-query ivf rq search](https://github.com/lance-format/lance/pull/7078)
  enabled raw-query search for new 1-bit and multi-bit split-code IVF_RQ
  indexes, including ex-code factors and public `num_bits > 1` creation.
- [#7179: feat(vector)!: add approx mode for RaBitQ search](https://github.com/lance-format/lance/pull/7179)
  added `approx_mode` and the RaBitQ fast/normal/accurate search-mode behavior.
- [#7205: perf(vector)!: add dedicated SIMD kernels for RaBitQ ex-code reranking](https://github.com/lance-format/lance/pull/7205)
  added direct ex-code rerank kernels, the blocked `__blocked_ex_codes` layout,
  and legacy sequential `__ex_codes` compatibility.
- [#7241: perf(vector): vectorize RaBitQ dist table quantization](https://github.com/lance-format/lance/pull/7241)
  optimized binary FastScan distance-table quantization used by the multi-bit
  search modes.
- [#7243: perf(vector): vectorize RaBitQ top-k lower-bound pruning scan](https://github.com/lance-format/lance/pull/7243)
  optimized the multi-bit raw-query lower-bound pruning scan before exact
  ex-code reranking.

## References

- Jianyang Gao, Cheng Long. _RaBitQ: Quantizing High-Dimensional Vectors with a
  Theoretical Error Bound for Approximate Nearest Neighbor Search._ SIGMOD 2024.
- Jianyang Gao, Yutong Gou, Yuexuan Xu, Yongye Su, Cheng Long, et al.
  _Practical and Asymptotically Optimal Quantization of High-Dimensional Vectors
  in Euclidean Space for Approximate Nearest Neighbor Search._ SIGMOD 2025 /
  arXiv:2409.09913.
- RaBitQ-Library reference kernels:
  <https://github.com/VectorDB-NTU/RaBitQ-Library>
- Lance RaBitQ tracking issue: lance-format/lance#4319
