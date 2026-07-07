# High-bit RaBitQ/SQ precision restoration

## Related tasks

- `Index Enhancement / RaBitQ Enhancement`: "Currently, the 1-bit RaBitQ is available, but the 4-bit and 8-bit RaBitQs are not."
- `Cache / Global Refine`: "High-bit RaBitQ/SQ precision restoration."
- `Cache / Hierarchical index`: "Multi-precision filtering: OBS (PQ/RaBitQ) + cache (SQ)".

Terminology note:

- `RaBitQ` is the human-facing name for Lance's `IVF_RQ`; current Rust symbols and modules still use the `Rabit*` spelling, implemented under `rust/lance-index/src/vector/bq*`.
- `SQ` means scalar quantization / `ScalarQuantizer`, implemented under `rust/lance-index/src/vector/sq*`.

## Working interpretation

This task is about vector-search recall in the cache/refine path:

1. A coarse ANN stage can use low-cost quantized data, often on object storage.
2. A "global refine" stage overfetches candidates and re-ranks them.
3. Instead of always reading raw vectors from the base table, the cache path should be able to restore precision by re-scoring candidates with a higher-bit quantized representation that is already cached, such as high-bit RaBitQ or SQ.
4. Exact raw-vector refine should remain available when users request exact re-rank, but the cache-oriented path should have a middle tier that is much cheaper than base-table `take()`.

In short: provide a high-precision cached re-score/refine tier for ANN candidates, using high-bit RaBitQ and/or true high-bit SQ, and make sure the bit width survives build, merge, cache serialization, prewarm, query, and binding APIs.

## Current state found in the repo

### Refine path

`Scanner::refine(factor)` is documented as reading extra candidates and re-ranking with original vector values. The indexed search planner overfetches `k * refine_factor`, then performs a `take()` of the vector column and runs flat KNN over the raw vectors.

Relevant files:

- `rust/lance/src/dataset/scanner.rs`
- `rust/lance/src/io/exec/knn.rs`
- `plans/docs/03-index-on-disk-and-search.md`

This is exact, but it costs base-table random reads. The plan's `Cache / Global Refine` label suggests a cache-resident alternative or complement.

### RaBitQ / IVF_RQ

Current code already has substantial high-bit RaBitQ support:

- `RQBuildParams` carries `num_bits`.
- `validate_rq_num_bits` accepts `1..=9`.
- `num_bits > 1` stores extra ex-code columns in addition to the sign-bit binary codes.
- Raw-query search, `ApproxMode::{Fast, Normal, Accurate}`, ex-code re-ranking, lower-bound pruning, and cache/prewarm code paths exist.
- There are end-to-end tests building and searching multi-bit `IVF_RQ` with `num_bits` values such as 4, 6, and 9.

Relevant files:

- `rust/lance-index/src/vector/bq.rs`
- `rust/lance-index/src/vector/bq/builder.rs`
- `rust/lance-index/src/vector/bq/storage.rs`
- `rust/lance/src/index/vector/ivf/v2.rs`
- `rust/lance/src/index/vector/ivf/partition_serde.rs`

This means the old row-15 wording ("4-bit and 8-bit RaBitQ are not available") is at least partly stale for the current tree. The remaining work is likely integration hardening: ensure high-bit RQ is actually used for precision restoration in cached/global refine scenarios, not just build/search unit coverage.

### SQ

SQ has an exposed `num_bits` field in metadata and build params, but the actual storage and distance implementation are still effectively SQ8:

- `SQBuildParams.num_bits` is a `u16`.
- `ScalarQuantizationMetadata.num_bits` is persisted.
- `ScalarQuantizer::transform` always calls `scale_to_u8`.
- `scale_to_u8` maps values into `0..=255`.
- SQ storage is `FixedSizeList<UInt8>`.
- SQ distance uses `l2_u8` / `dot_u8` and scales by 255-derived bounds.
- The Python docs say SQ supports only 8 bits.
- Python's generic `num_bits` kwarg currently updates PQ/RQ params, not SQ params.
- Java exposes SQ `numBits`, including tests that pass non-default values, but Rust SQ does not appear to implement true non-8-bit storage.

Relevant files:

- `rust/lance-index/src/vector/sq.rs`
- `rust/lance-index/src/vector/sq/builder.rs`
- `rust/lance-index/src/vector/sq/storage.rs`
- `python/src/dataset.rs`
- `python/python/lance/dataset.py`
- `java/src/main/java/org/lance/index/vector/SQBuildParams.java`
- `java/lance-jni/src/utils.rs`

This task should either implement true high-bit SQ, probably at least `UInt16` for 16-bit, or reject/document non-8-bit SQ clearly. Silently accepting `num_bits != 8` while producing SQ8 codes is a precision bug and API trap.

### Cache-specific considerations

The index cache can serialize partition entries for SQ and RaBitQ:

- SQ cache headers preserve `num_bits`, `dim`, distance type, and bounds.
- RaBitQ cache headers preserve `num_bits`, `code_dim`, rotation type, query estimator, and fast-rotation signs.
- RaBitQ has a runtime search cache for rotated centroids in raw-query mode.
- Cache codec versions are explicit. Any storage layout change, especially true high-bit SQ, needs versioning and backward compatibility.

Relevant files:

- `rust/lance/src/session.rs`
- `rust/lance/src/index/vector/ivf/partition_serde.rs`
- `rust/lance/src/index/vector/ivf/v2.rs`
- `rust/lance/src/dataset/tests/dataset_index.rs`

## Open-source status and roadmap

Status checked against public `lance-format/lance` on 2026-07-08:
[discussions](https://github.com/lance-format/lance/discussions),
[pull requests](https://github.com/lance-format/lance/pulls), and
[issues](https://github.com/lance-format/lance/issues). This section treats
GitHub upstream state as the source of truth. Local experimental branches may
contain additional work that has not landed upstream.

Release markers below use the first final Lance tag that contains the PR's
merged commit. Beta and RC tags are ignored. Since the latest final release is
`v8.0.0`, merged-after-`v8.0.0` and open PRs are marked `future 9.0`;
closed-unmerged historical PRs are marked `not released`.

| PR | First final release | Notes |
|---|---:|---|
| [`#4344`](https://github.com/lance-format/lance/pull/4344) | `v0.38.0` | RabitQ quantization |
| [`#4913`](https://github.com/lance-format/lance/pull/4913) | `v0.38.3` | vector index spec documentation |
| [`#5648`](https://github.com/lance-format/lance/pull/5648) | `v3.0.0` | Java `IVF_RQ` creation |
| [`#6359`](https://github.com/lance-format/lance/pull/6359) | `v6.0.0` | distributed `IVF_RQ` segment builds |
| [`#7014`](https://github.com/lance-format/lance/pull/7014) | `v8.0.0` | shared RaBitQ rotation for distributed Python builds |
| [`#7038`](https://github.com/lance-format/lance/pull/7038) | `v8.0.0` | multi-bit `IVF_RQ` storage |
| [`#7078`](https://github.com/lance-format/lance/pull/7078) | `v8.0.0` | raw-query `IVF_RQ` search |
| [`#7179`](https://github.com/lance-format/lance/pull/7179) | `v8.0.0` | RaBitQ `approx_mode` |
| [`#7205`](https://github.com/lance-format/lance/pull/7205) | `v8.0.0` | ex-code reranking SIMD kernels |
| [`#7241`](https://github.com/lance-format/lance/pull/7241) | `v8.0.0` | RaBitQ distance-table quantization |
| [`#7243`](https://github.com/lance-format/lance/pull/7243) | `v8.0.0` | top-k lower-bound pruning scan |
| [`#7163`](https://github.com/lance-format/lance/pull/7163) | `v8.0.0` | versioned cache-codec envelope |
| [`#7217`](https://github.com/lance-format/lance/pull/7217) | `v8.0.0` | `IVF_RQ` fragment-reuse remap fix |
| [`#7315`](https://github.com/lance-format/lance/pull/7315) | `v8.0.0` | PQ storage row-ID remap fix |
| [`#7481`](https://github.com/lance-format/lance/pull/7481) | `future 9.0` | merged after `v8.0.0`; SQ dot offset fix |
| [`#7355`](https://github.com/lance-format/lance/pull/7355) | `future 9.0` | open; SQ dot distance from dequantized values |
| [`#7566`](https://github.com/lance-format/lance/pull/7566) | `future 9.0` | open draft; covering columns for `IVF_PQ` vector search |
| [`#7440`](https://github.com/lance-format/lance/pull/7440) | `future 9.0` | open; vector index handle readers |
| [`#7640`](https://github.com/lance-format/lance/pull/7640) | `future 9.0` | open; shared IVF partition scans for batch queries |
| [`#7077`](https://github.com/lance-format/lance/pull/7077) | not released | closed unmerged; historical context only; superseded by merged multi-bit `IVF_RQ` work |

### Done upstream

- Base RaBitQ support exists: [`#4344`](https://github.com/lance-format/lance/pull/4344)
  added RabitQ quantization and [`#4913`](https://github.com/lance-format/lance/pull/4913)
  documented it in the vector index spec.
- Distributed and binding support has moved forward:
  [`#5648`](https://github.com/lance-format/lance/pull/5648) added Java
  `IVF_RQ` creation, [`#6359`](https://github.com/lance-format/lance/pull/6359)
  added distributed `IVF_RQ` segment builds, and
  [`#7014`](https://github.com/lance-format/lance/pull/7014) added shared
  RaBitQ rotation for distributed Python builds.
- Multi-bit RaBitQ storage landed in
  [`#7038`](https://github.com/lance-format/lance/pull/7038), with storage
  preparation for `num_bits=2..9` using split sign-bit and ex-code storage.
- Raw-query search and the current public accuracy knob landed in
  [`#7078`](https://github.com/lance-format/lance/pull/7078) and
  [`#7179`](https://github.com/lance-format/lance/pull/7179). These added
  raw-query `IVF_RQ` search metadata and public `approx_mode={fast,normal,accurate}`
  through Rust scanner, ANN proto serialization, Python query parsing, and
  distance-calculator options.
- RaBitQ high-bit performance work landed in
  [`#7205`](https://github.com/lance-format/lance/pull/7205),
  [`#7241`](https://github.com/lance-format/lance/pull/7241), and
  [`#7243`](https://github.com/lance-format/lance/pull/7243), covering ex-code
  reranking SIMD kernels, distance-table quantization, and lower-bound pruning.
- Cache serialization was stabilized by
  [`#7163`](https://github.com/lance-format/lance/pull/7163), which added a
  versioned cache-codec envelope for restart-surviving and backend-independent
  cache entries.
- Important maintenance fixes landed:
  [`#7217`](https://github.com/lance-format/lance/pull/7217) fixed `IVF_RQ`
  fragment-reuse remap behavior, and
  [`#7315`](https://github.com/lance-format/lance/pull/7315) fixed PQ storage
  row IDs after fragment-reuse remap.
- SQ dot-product correctness improved in
  [`#7481`](https://github.com/lance-format/lance/pull/7481), which accounts
  for SQ affine offsets when the quantization lower bound is non-zero.

### In progress or adjacent upstream work

- [`#7355`](https://github.com/lance-format/lance/pull/7355) is still open and
  also targets SQ dot distance by computing from dequantized values. It should
  be reconciled with merged [`#7481`](https://github.com/lance-format/lance/pull/7481)
  and the still-open recall report
  [`#7352`](https://github.com/lance-format/lance/issues/7352).
- [`#7566`](https://github.com/lance-format/lance/pull/7566) is an open draft
  that implements
  covering/included columns for `IVF_PQ` vector search. It is not high-bit
  precision restoration, but it is relevant because it avoids base-table
  `TakeExec` I/O for covered vector-search projections.
- [`#7440`](https://github.com/lance-format/lance/pull/7440) exposes vector
  index handle readers and relates to the public reader API request in
  [`#7319`](https://github.com/lance-format/lance/issues/7319).
- [`#7640`](https://github.com/lance-format/lance/pull/7640) shares IVF
  partition scans across batch vector queries. This is query-path performance
  work, not precision restoration, but it may interact with cached/prepared
  partition search.
- Multi-segment vector index work is active around
  [`#6309`](https://github.com/lance-format/lance/issues/6309) and discussion
  [`#6189`](https://github.com/lance-format/lance/discussions/6189). The
  discussion explicitly covers `IVF_FLAT`, `IVF_PQ`, and `IVF_SQ` first, while
  `IVF_RQ` is called out as a separate evolution path.
- Discussion [`#7575`](https://github.com/lance-format/lance/discussions/7575)
  proposes pluggable cache backends across Rust, Python, and Java. It is
  directly relevant to any long-lived cache-resident precision-restoration tier.
- Discussion [`#6909`](https://github.com/lance-format/lance/discussions/6909)
  proposes covering index columns for vector search, the design basis for
  open draft PR [`#7566`](https://github.com/lance-format/lance/pull/7566).
- Discussion [`#6408`](https://github.com/lance-format/lance/discussions/6408)
  raises TurboQuant as a future quantization direction and compares 4-bit
  quantization build time against PQ and RabitQ. This is exploratory and should
  not block this task.

### Open issues and risks to track

- [`#4319`](https://github.com/lance-format/lance/issues/4319), the original
  RabitQ tracker, is still open and its checklist is stale relative to the
  merged June 2026 work. It should be updated or split into current follow-ups.
- [`#7157`](https://github.com/lance-format/lance/issues/7157) reports
  `IVF_RQ` retrieval quality degradation as embedding dimension grows. This is
  directly relevant to any claim that high-bit RaBitQ restores precision.
- [`#7276`](https://github.com/lance-format/lance/issues/7276) tracks a 4-bit
  distance-table performance regression. High-bit precision restoration must not
  trade base-table I/O for an avoidable hot-loop regression.
- [`#7352`](https://github.com/lance-format/lance/issues/7352) tracks low
  recall for `IVF_SQ` / `IVF_HNSW_SQ` with dot distance. Merged
  [`#7481`](https://github.com/lance-format/lance/pull/7481) addresses one
  root cause, but the issue remains open and should be verified on the reported
  benchmark.
- [`#7201`](https://github.com/lance-format/lance/issues/7201) asked for
  vector-index I/O metrics and is closed. The precision-restoration work should
  still include metrics/assertions proving that a warm-cache path avoids
  base-table vector reads when that is the intended behavior.

### What should be done next

1. Reconcile upstream tracking.
   - Update or close stale tracker [`#4319`](https://github.com/lance-format/lance/issues/4319).
   - Treat closed-unmerged [`#7077`](https://github.com/lance-format/lance/pull/7077)
     only as historical context; prove the upstream query behavior from merged
     PRs and tests, not from that PR.
   - Create or reuse one explicit tracking issue for "high-bit quantized
     cache/global refine" if none exists after updating `#4319`.

2. Lock down RaBitQ precision restoration.
   - Add end-to-end tests for `IVF_RQ` with `num_bits=4` and `num_bits=8` across
     cold search, warm/prewarmed cache search, optimize/remap, and distributed
     or multi-segment search.
   - Assert that `ApproxMode::Normal` and `ApproxMode::Accurate` use high-bit
     ex-code scoring when the index was built with `num_bits > 1`, and that only
     `ApproxMode::Fast` is allowed to drop to sign-bit-only scoring.
   - Resolve or explicitly scope [`#7157`](https://github.com/lance-format/lance/issues/7157)
     before using high-dimensional `IVF_RQ` as a precision-restoration tier.
   - Re-run the 4-bit performance benchmark from
     [`#7276`](https://github.com/lance-format/lance/issues/7276) after any
     query-path changes.

3. Make the SQ bit-width contract unambiguous.
   - Decide whether this task implements true non-8-bit SQ or formally limits
     SQ to SQ8.
   - If SQ remains SQ8, validate `SQBuildParams.num_bits == 8` in Rust and align
     Python and Java errors/docs.
   - If high-bit SQ is required, add real storage and distance kernels for the
     selected widths, version cache serialization, and add recall tests.
   - Reconcile [`#7355`](https://github.com/lance-format/lance/pull/7355),
     [`#7481`](https://github.com/lance-format/lance/pull/7481), and
     [`#7352`](https://github.com/lance-format/lance/issues/7352) so SQ dot
     correctness is not left in a half-fixed state.

4. Define cache/global-refine semantics.
   - Keep existing `Scanner::refine(factor)` semantics as exact raw-vector
     re-rank unless the public API is intentionally changed.
   - Add a separate quantized cached re-score mode, `approx_mode` behavior, or
     cache policy that clearly means "approximate high-bit re-score without base
     table vector reads".
   - Use the cache envelope from [`#7163`](https://github.com/lance-format/lance/pull/7163)
     and keep discussion [`#7575`](https://github.com/lance-format/lance/discussions/7575)
     in mind so the feature can survive persistent or third-party cache
     backends.
   - Add metrics/tests that fail if a warm-cache quantized re-score silently
     falls back to `take()` of raw vectors.

5. Fit into index maintenance work.
   - Make sure high-bit RQ/SQ cache entries survive remap, compaction, optimize,
     copy, and multi-segment coexistence.
   - Align with discussion [`#6189`](https://github.com/lance-format/lance/discussions/6189)
     and tracking issue [`#6309`](https://github.com/lance-format/lance/issues/6309),
     especially because `IVF_RQ` is on a separate segment-evolution path.
   - Treat covering-index work in [`#6909`](https://github.com/lance-format/lance/discussions/6909)
     and [`#7566`](https://github.com/lance-format/lance/pull/7566) as adjacent:
     useful for avoiding metadata `TakeExec`, but not a replacement for
     high-bit vector re-score.

6. Update public docs and bindings.
   - Document the supported RaBitQ bit widths and whether all of them are public
     compatibility guarantees or implementation limits.
   - Document that SQ is either SQ8-only or list the real supported high-bit SQ
     widths.
   - Add Python and Java examples for any public `num_bits`, `approx_mode`, or
     cache-refine behavior that this task exposes.

## Proposed task description

Implement and validate a cache-aware precision restoration path for global ANN refine using high-bit quantizers.

The feature should allow Lance to overfetch candidates from an approximate vector index and re-score those candidates with a higher-precision cached representation before producing final top-k results. For RaBitQ, this means using the multi-bit `IVF_RQ` ex-code path, especially in `ApproxMode::Normal` or `ApproxMode::Accurate`, through cache/prewarm/reconstruction. For SQ, this means either implementing true high-bit SQ storage and distance or enforcing that SQ is SQ8 only and documenting that it is not part of "high-bit" precision restoration.

The end result should make cached/global refine recall closer to exact raw-vector refine while avoiding the base-table I/O cost whenever the configured precision tier is sufficient.

## Likely deliverables

1. Terminology cleanup
   - Use `RaBitQ` / `IVF_RQ` consistently in technical docs and task notes.
   - Use `SQ` consistently for scalar quantization.
   - Keep `Rabit*` only where referring to existing Rust identifiers.

2. RaBitQ high-bit refine hardening
   - Audit `IVF_RQ` with `num_bits > 1` through normal query, `Scanner::refine`, index cache prewarm, cache serialization, optimize/remap, and distributed merge.
   - Ensure cached/prepared partition search does not drop to one-bit scoring unless the user explicitly selected `ApproxMode::Fast`.
   - Add coverage for 4-bit and 8-bit, since those are the bit widths called out in the plan.

3. SQ decision
   - If true high-bit SQ is required, add storage and distance support for the selected bit widths, probably `UInt16` for 16-bit and possibly packed 4-bit if requested.
   - If SQ8 is the only supported SQ, validate `SQBuildParams.num_bits == 8` at Rust API boundaries and update Python/Java behavior and docs.
   - Ensure all bindings either expose the same supported bit widths or reject unsupported values with matching error messages.

4. Cache/global refine behavior
   - Define whether high-bit quantized re-score is a new refine mode, part of `approx_mode`, or internal cache policy.
   - Preserve exact raw-vector refine semantics for existing `Scanner::refine`.
   - Add metrics or tests that prove the cache refine path can run without base data-file reads after prewarm.

5. Documentation
   - Update vector index docs with supported bit widths.
   - Explain the precision ladder: coarse ANN, high-bit cached re-score, optional exact raw-vector refine.
   - Document quality/latency tradeoffs and expected use cases.

## Suggested acceptance criteria

- `IVF_RQ` with `num_bits=4` and `num_bits=8` can be built and queried from Rust and bindings where those APIs exist.
- Warm-cache searches over high-bit `IVF_RQ` preserve high-bit scoring and do not silently fall back to 1-bit scores except in `ApproxMode::Fast`.
- SQ non-8-bit behavior is no longer ambiguous: it either works with real higher precision or fails early with a descriptive error.
- Cache codec roundtrips preserve all high-bit RQ/SQ fields and remain backward compatible with existing SQ8 and RQ1 cache entries.
- Tests cover cold search, warm/prewarmed cache search, optimize/remap, and at least one recall assertion against exact search.
- Public docs and Python/Java API docs match the implemented behavior.

## Open questions

- Which bit widths are required by the product target: only 4 and 8 for RaBitQ, the current `1..=9` RQ range, or a smaller supported/public subset?
- Is the desired global refine output allowed to remain approximate, or must user-facing `refine_factor` continue to mean exact raw-vector re-rank?
- Should the high-bit precision tier be selected by `approx_mode`, a new query option, index metadata, or cache policy?
- What recall target should define "precision restoration" for Lance 8?

## Risk notes

- SQ currently accepts and persists `num_bits` but uses `UInt8` codes. That can make metadata imply higher precision than the actual index provides.
- `SQBuildParams::sample_size` scales as `sample_rate * 2^num_bits`; accepting large SQ bit widths without validation can create unreasonable sampling requests.
- Cache serialization has explicit codec versions. Any true high-bit SQ layout change must avoid misreading old SQ8 cache entries.
- RQ high-bit code is more complete, but cache/refine tests should target the composed system because bugs often appear at build/merge/cache/query boundaries.
