# High-bit RaBitQ/SQ precision restoration

## Related tasks

- `Index Enhancement / RaBitQ Enhancement`: "Currently, the 1-bit RaBitQ is available, but the 4-bit and 8-bit RaBitQs are not."
- `Cache / Global Refine`: "High-bit RaBitQ/SQ precision restoration."
- `Cache / Hierarchical index`: "Multi-precision filtering: OBS (PQ/RaBitQ) + cache (SQ)".

Terminology note:

- `RaBitQ` is the human-facing name for Lance's `IVF_RQ`; current Rust symbols and modules still use the `Rabit*` spelling, implemented under `rust/lance-index/src/vector/bq*`.
- `SQ` means scalar quantization / `ScalarQuantizer`, implemented under `rust/lance-index/src/vector/sq*`.
- `OBS` means object storage in the product task wording.

## Goal and stage order

The goal is a cache-aware precision restoration path for vector search:

1. Search cheaply over coarse or low-precision ANN data.
2. Overfetch candidates.
3. Re-score candidates with a higher-precision cached representation, such as high-bit `IVF_RQ` or SQ.
4. Optionally run exact raw-vector refine when the user requests exact re-ranking.

The implementation should be staged because each task depends on the previous one:

1. `Index Enhancement / RaBitQ Enhancement`: make high-bit `IVF_RQ`, especially 4-bit and 8-bit, a hardened and documented index capability.
2. `Cache / Global Refine`: define and implement the cache-resident precision restoration tier.
3. `Cache / Hierarchical index`: compose object-storage ANN filtering and cached SQ refinement into a multi-precision query path.

Status below combines the repository audit at base commit `b1570222c` with
public `lance-format/lance` state checked on 2026-07-20. Local experimental
branches may contain additional work that has not landed upstream.

## Open-source status and roadmap

Status checked against public `lance-format/lance` on 2026-07-20:
[discussions](https://github.com/lance-format/lance/discussions),
[pull requests](https://github.com/lance-format/lance/pulls), and
[issues](https://github.com/lance-format/lance/issues). This section treats
GitHub upstream state as the source of truth. Local experimental branches may
contain additional work that has not landed upstream.

Release markers below use the first final Lance tag that contains the PR's
merged commit. Beta and RC tags are ignored. Since the latest final release is
`v8.0.0`, merged-after-`v8.0.0` and open PRs are marked `future 9.x`;
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
| [`#7481`](https://github.com/lance-format/lance/pull/7481) | `future 9.x` | merged after `v8.0.0`; SQ dot offset fix |
| [`#7583`](https://github.com/lance-format/lance/pull/7583) | `future 9.x` | merged; preserve PQ `num_bits` in Python model training |
| [`#7679`](https://github.com/lance-format/lance/pull/7679) | `future 9.x` | merged; stabilize multi-bit `IVF_RQ` recall coverage |
| [`#7680`](https://github.com/lance-format/lance/pull/7680) | `future 9.x` | merged; batch IVF streaming partition search |
| [`#7768`](https://github.com/lance-format/lance/pull/7768) | `future 9.x` | merged; train vector segments on fragment subsets and validate shared models |
| [`#7355`](https://github.com/lance-format/lance/pull/7355) | `future 9.x` | open; SQ dot distance from dequantized values |
| [`#7566`](https://github.com/lance-format/lance/pull/7566) | `future 9.x` | open; covering columns for `IVF_PQ` vector search |
| [`#7440`](https://github.com/lance-format/lance/pull/7440) | `future 9.x` | open; vector index handle readers |
| [`#7640`](https://github.com/lance-format/lance/pull/7640) | `future 9.x` | open; shared IVF partition scans for batch queries |
| [`#7077`](https://github.com/lance-format/lance/pull/7077) | not released | closed unmerged; historical context only; superseded by merged multi-bit `IVF_RQ` work |

Roadmap mapping:

- Stage 1 is mostly merged upstream for storage and query mechanics. The 5-bit recall matrix and fragment-subset/shared-model validation improve the foundation, but explicit 4-bit and 8-bit hardening, public contract cleanup, and closure around [`#7157`](https://github.com/lance-format/lance/issues/7157) and [`#7276`](https://github.com/lance-format/lance/issues/7276) remain.
- Stage 2 is only partially in place. The cache-codec foundation exists, but the cache/global-refine API and SQ bit-width contract still need implementation decisions.
- Stage 3 is design-stage work. Fragment-subset segments, shared-model validation, batched IVF search, covering-index work, and multi-segment support are adjacent foundations, but there is not yet a defined hierarchical query planner for `OBS PQ/RaBitQ -> cached SQ -> optional exact refine`.

## Stage 1: Index Enhancement / RaBitQ Enhancement

Make high-bit `IVF_RQ` a real, tested, public capability. This stage is about the index itself, before treating it as a cache/global-refine building block.

Relevant local files:

- `rust/lance-index/src/vector/bq.rs`
- `rust/lance-index/src/vector/bq/builder.rs`
- `rust/lance-index/src/vector/bq/storage.rs`
- `rust/lance/src/index/vector/ivf/v2.rs`
- `rust/lance/src/index/vector/ivf/partition_serde.rs`

### Done

- Base RaBitQ support landed in [`#4344`](https://github.com/lance-format/lance/pull/4344), with vector-index spec documentation in [`#4913`](https://github.com/lance-format/lance/pull/4913).
- Java `IVF_RQ` creation landed in [`#5648`](https://github.com/lance-format/lance/pull/5648), distributed `IVF_RQ` segment builds landed in [`#6359`](https://github.com/lance-format/lance/pull/6359), and shared RaBitQ rotation for distributed Python builds landed in [`#7014`](https://github.com/lance-format/lance/pull/7014).
- Multi-bit `IVF_RQ` storage landed in [`#7038`](https://github.com/lance-format/lance/pull/7038). The current Rust code carries `RQBuildParams::num_bits`, validates `1..=9`, and stores extra ex-code columns when `num_bits > 1`.
- Raw-query `IVF_RQ` search and the public accuracy knob landed in [`#7078`](https://github.com/lance-format/lance/pull/7078) and [`#7179`](https://github.com/lance-format/lance/pull/7179), including `ApproxMode::{Fast, Normal, Accurate}`.
- RaBitQ high-bit performance work landed in [`#7205`](https://github.com/lance-format/lance/pull/7205), [`#7241`](https://github.com/lance-format/lance/pull/7241), and [`#7243`](https://github.com/lance-format/lance/pull/7243), covering ex-code reranking SIMD kernels, distance-table quantization, and lower-bound pruning.
- `IVF_RQ` fragment-reuse remap behavior was fixed in [`#7217`](https://github.com/lance-format/lance/pull/7217).
- Existing end-to-end tests build and search multi-bit `IVF_RQ` values such as `num_bits=4`, `num_bits=6`, and `num_bits=9`.
- [`#7679`](https://github.com/lance-format/lance/pull/7679) moved the main
  `IVF_RQ` recall matrix to 5-bit codes, kept both rotation types plus
  remap/multivector coverage, and raised the recall requirement to `0.9`.
- [`#7768`](https://github.com/lance-format/lance/pull/7768) made standalone
  vector segments train correctly on explicit fragment subsets and rejects
  merge/optimize when independently trained segments do not share the same
  IVF and quantizer model.

### In progress

- The original RaBitQ tracker, [`#4319`](https://github.com/lance-format/lance/issues/4319), is still open and appears stale relative to the merged multi-bit RaBitQ work.
- Multi-segment vector index work is active around [`#6309`](https://github.com/lance-format/lance/issues/6309) and discussion [`#6189`](https://github.com/lance-format/lance/discussions/6189). The discussion covers `IVF_FLAT`, `IVF_PQ`, and `IVF_SQ` first, while `IVF_RQ` is called out as a separate path.
- Open [`#7440`](https://github.com/lance-format/lance/pull/7440) proposes vector index handle readers and may affect how high-bit index internals are accessed by future query or cache code.
- Open [`#7640`](https://github.com/lance-format/lance/pull/7640) proposes shared IVF partition scans across batch vector queries. This is not precision restoration, but it can interact with prepared partition search.

### Open issues and risks

- The task wording that only 1-bit RaBitQ is available is now partly stale. The remaining gap is not basic storage availability; it is hardening 4-bit and 8-bit behavior through query, cache, remap, merge, and bindings.
- [`#7157`](https://github.com/lance-format/lance/issues/7157) reports `IVF_RQ` retrieval quality degradation as embedding dimension grows. This must be resolved or explicitly scoped before high-bit RaBitQ is claimed as a precision restoration tier.
- [`#7276`](https://github.com/lance-format/lance/issues/7276) tracks a 4-bit distance-table performance regression. Precision restoration should not trade base-table reads for an avoidable hot-loop regression.
- Closed-unmerged [`#7077`](https://github.com/lance-format/lance/pull/7077) should be treated only as historical context. Current behavior should be proven from merged PRs and tests.
- The public bit-width contract is unclear: the product task calls out 4-bit and 8-bit, while Rust currently accepts `1..=9`. The new 5-bit recall matrix strengthens multi-bit coverage but does not replace explicit 4-bit and 8-bit acceptance tests.
- Fragment-subset builds are valid, but separately trained segments are not
  automatically composable. Any distributed or hierarchical build that plans
  to merge segments must arrange shared precomputed model state.
- High-bit scoring must not silently degrade to sign-bit-only scoring except when the user selected `ApproxMode::Fast`.

### What should be done next

1. Update or split [`#4319`](https://github.com/lance-format/lance/issues/4319) so the tracker reflects the merged high-bit work and the remaining validation gaps.
2. Decide the public `IVF_RQ` bit-width contract: only 4 and 8, the current `1..=9`, or a documented subset.
3. Add end-to-end tests for `IVF_RQ` with `num_bits=4` and `num_bits=8` across cold search, warm/prewarmed cache search, optimize/remap, and distributed or multi-segment search.
4. Assert that `ApproxMode::Normal` and `ApproxMode::Accurate` use high-bit ex-code scoring when the index was built with `num_bits > 1`.
5. Re-run the 4-bit benchmark from [`#7276`](https://github.com/lance-format/lance/issues/7276) after query-path changes.
6. Resolve or explicitly scope [`#7157`](https://github.com/lance-format/lance/issues/7157) before depending on high-dimensional `IVF_RQ` for recall restoration.
7. Update Rust, Python, Java, and vector-index docs to describe supported `IVF_RQ` bit widths and `approx_mode` behavior.

## Stage 2: Cache / Global Refine

Define and implement a cache-resident precision restoration tier for global ANN refine. This stage should preserve exact raw-vector refine while adding a cheaper high-precision re-score path.

Relevant local files:

- `rust/lance/src/dataset/scanner.rs`
- `rust/lance/src/io/exec/knn.rs`
- `rust/lance/src/session.rs`
- `rust/lance/src/index/vector/ivf/partition_serde.rs`
- `rust/lance/src/index/vector/ivf/v2.rs`
- `rust/lance/src/dataset/tests/dataset_index.rs`
- `rust/lance-index/src/vector/sq.rs`
- `rust/lance-index/src/vector/sq/builder.rs`
- `rust/lance-index/src/vector/sq/storage.rs`
- `python/src/dataset.rs`
- `python/python/lance/dataset.py`
- `java/src/main/java/org/lance/index/vector/SQBuildParams.java`
- `java/lance-jni/src/utils.rs`

### Done

- Existing `Scanner::refine(factor)` semantics are exact: Lance overfetches `k * refine_factor`, reads original vector values from the base table, and runs flat KNN over raw vectors.
- Cache serialization was stabilized in [`#7163`](https://github.com/lance-format/lance/pull/7163) with a versioned cache-codec envelope.
- The index cache can serialize partition entries for both SQ and RaBitQ.
- RaBitQ cache headers preserve `num_bits`, `code_dim`, rotation type, query estimator, and fast-rotation signs.
- SQ cache headers preserve `num_bits`, `dim`, distance type, and bounds.
- SQ dot-product correctness improved in [`#7481`](https://github.com/lance-format/lance/pull/7481), which accounts for SQ affine offsets when the quantization lower bound is non-zero.
- [`#7583`](https://github.com/lance-format/lance/pull/7583) preserves PQ
  `num_bits` when Python supplies a pre-trained model. It is adjacent rather
  than an SQ/RaBitQ refine implementation, but it closes an important
  multi-precision model-metadata loss.
- [`#7201`](https://github.com/lance-format/lance/issues/7201), the vector-index I/O metrics request, is closed. The precision restoration work should still include assertions that prove the warm-cache path avoids base-table vector reads when that is intended.

### In progress

- [`#7355`](https://github.com/lance-format/lance/pull/7355) is still open and also targets SQ dot distance by computing from dequantized values. It must be reconciled with merged [`#7481`](https://github.com/lance-format/lance/pull/7481) and the open recall report [`#7352`](https://github.com/lance-format/lance/issues/7352).
- Discussion [`#7575`](https://github.com/lance-format/lance/discussions/7575) proposes pluggable cache backends across Rust, Python, and Java, which is relevant to any persistent cache-resident precision tier.
- [`#7566`](https://github.com/lance-format/lance/pull/7566) is open for covering/included columns in `IVF_PQ` vector search. It can reduce base-table `TakeExec` I/O for covered projections, but it is adjacent rather than a high-bit vector re-score path.
- The public query semantics for a cached quantized re-score tier are not yet defined: it could be a new refine mode, an `approx_mode` behavior, an index option, or a cache policy.

### Open issues and risks

- Changing `Scanner::refine(factor)` to become approximate would break the current documented meaning of exact raw-vector re-rank. A new mode or explicit option is safer.
- SQ has an exposed `num_bits` field in metadata and build params, but the current storage and distance path are effectively SQ8: `ScalarQuantizer::transform` maps to `u8`, storage is `FixedSizeList<UInt8>`, and distances use `l2_u8` / `dot_u8`.
- Python docs say SQ supports only 8 bits, Python's generic `num_bits` kwarg currently updates PQ/RQ params rather than SQ params, and Java exposes SQ `numBits`. The bindings are not aligned.
- Silently accepting `num_bits != 8` for SQ while producing SQ8 codes is a precision bug and API trap.
- `SQBuildParams::sample_size` scales as `sample_rate * 2^num_bits`; accepting large SQ bit widths without validation can create unreasonable sampling requests.
- Any true high-bit SQ storage layout needs cache codec versioning and backward compatibility with existing SQ8 cache entries.
- The feature needs metrics or tests that fail if warm-cache quantized re-score silently falls back to `take()` of raw vectors.

### What should be done next

1. Keep existing `Scanner::refine(factor)` semantics as exact raw-vector re-rank unless there is an intentional public API change.
2. Define the cache/global-refine API for approximate high-bit re-score: new refine mode, `approx_mode` extension, index metadata, or cache policy.
3. Decide the SQ contract. Either implement true non-8-bit SQ or validate `SQBuildParams::num_bits == 8` in Rust and align Python, Java, and docs.
4. If high-bit SQ is required, implement real storage and distance kernels for the selected widths, version the cache serialization, and add recall tests.
5. Reconcile [`#7355`](https://github.com/lance-format/lance/pull/7355), [`#7481`](https://github.com/lance-format/lance/pull/7481), and [`#7352`](https://github.com/lance-format/lance/issues/7352) so SQ dot-distance correctness is not left in a half-fixed state.
6. Add tests that prove a warm-cache high-bit re-score path can run without base data-file vector reads after prewarm.
7. Add recall assertions against exact search, not just index creation or query success.
8. Update public docs and binding examples for any exposed `num_bits`, `approx_mode`, or cache-refine behavior.

## Stage 3: Cache / Hierarchical index

Compose a multi-precision filtering path: low-cost ANN data on object storage, a higher-precision cached SQ or RaBitQ tier, and optional exact raw-vector refine. This stage should start after the Stage 1 and Stage 2 contracts are stable.

Relevant local references:

- `plans/docs/02-vector-indexes.md`
- `plans/docs/03-index-on-disk-and-search.md`
- `rust/lance/src/session.rs`
- `rust/lance/src/index/vector/ivf/v2.rs`
- `rust/lance/src/index/vector/ivf/partition_serde.rs`
- `rust/lance-index/src/vector/pq.rs`
- `rust/lance-index/src/vector/sq.rs`
- `rust/lance-index/src/vector/bq.rs`

### Done

- Lance already has the building blocks for IVF-based vector indexes: `IVF_FLAT`, `IVF_PQ`, `IVF_SQ`, `IVF_RQ`, `IVF_HNSW_FLAT`, and `IVF_HNSW_SQ`.
- Multi-bit RaBitQ and versioned cache serialization provide part of the foundation needed for a high-bit cached tier.
- PQ storage row IDs after fragment-reuse remap were fixed in [`#7315`](https://github.com/lance-format/lance/pull/7315), and `IVF_RQ` remap behavior was fixed in [`#7217`](https://github.com/lance-format/lance/pull/7217).
- [`#7768`](https://github.com/lance-format/lance/pull/7768) provides
  fragment-subset vector segments plus shared-model validation, which is a
  prerequisite for composing safe multi-segment tiers.
- [`#7680`](https://github.com/lance-format/lance/pull/7680) batches streaming
  IVF partition search away from the CPU pool, an adjacent execution-path
  foundation for multi-tier queries.
- Discussion [`#6909`](https://github.com/lance-format/lance/discussions/6909) defines the covering-index direction, and open [`#7566`](https://github.com/lance-format/lance/pull/7566) proposes covering/included columns for `IVF_PQ` vector search.

### In progress

- Multi-segment vector index work remains active around [`#6309`](https://github.com/lance-format/lance/issues/6309) and discussion [`#6189`](https://github.com/lance-format/lance/discussions/6189).
- [`#7566`](https://github.com/lance-format/lance/pull/7566) is adjacent because covering columns can avoid some base-table reads, but it does not replace a high-bit vector re-score tier.
- [`#7640`](https://github.com/lance-format/lance/pull/7640) may improve shared IVF partition scans for batch queries and should be considered when designing hierarchical scans.
- Discussion [`#7575`](https://github.com/lance-format/lance/discussions/7575) is relevant because a hierarchical design depends on cache capacity, eviction, persistence, and backend behavior.
- There is no clearly defined hierarchical query planner yet for "OBS PQ/RaBitQ first, cached SQ second, optional exact refine third."

### Open issues and risks

- Without Stage 2 semantics, the hierarchical task can drift into projection/cache optimization only. It needs an explicit high-bit re-score contract.
- Cache policy must decide what gets materialized, where it lives, how it is prewarmed, and how it is evicted across DRAM, SSD, and remote/object storage.
- Multi-precision ranking needs clear score semantics. Mixing PQ/RaBitQ first-pass scores with SQ second-pass scores can produce surprising behavior unless the final ranking stage is explicit.
- Row-ID and fragment remap correctness must hold across every tier: PQ/RaBitQ on object storage, cached SQ, and exact base-table vectors.
- Memory and SSD cost can grow quickly if cached SQ is materialized broadly. The design needs limits, metrics, and fallback behavior.
- The recall target for "precision restoration" is not yet defined. The stage needs a numeric recall goal against exact search and latency/I/O targets for cold and warm cache.
- Covering-index work is useful but not a replacement for high-bit vector precision restoration.

### What should be done next

1. Write the hierarchical query contract: OBS low-precision scan, cached high-precision re-score, and optional exact raw-vector refine.
2. Define how the planner selects tiers: index metadata, query option, cache policy, or cost model.
3. Define the cached SQ materialization format and how it interacts with pluggable cache backends from [`#7575`](https://github.com/lance-format/lance/discussions/7575).
4. Add instrumentation for per-tier candidates, bytes read, cache hits, base-table vector reads, and final recall.
5. Add tests for multi-fragment datasets, optimize/remap, compaction, copy, and multi-segment coexistence.
6. Add cold-cache and warm-cache benchmarks that compare low-precision only, cached high-precision re-score, and exact raw-vector refine.
7. Treat [`#6909`](https://github.com/lance-format/lance/discussions/6909) and [`#7566`](https://github.com/lance-format/lance/pull/7566) as adjacent work: integrate when useful, but do not count covered projections as vector precision restoration.

## Cross-stage acceptance criteria

- `IVF_RQ` with `num_bits=4` and `num_bits=8` can be built and queried from Rust and bindings where those APIs exist.
- Warm-cache searches over high-bit `IVF_RQ` preserve high-bit scoring and do not silently fall back to 1-bit scores except in `ApproxMode::Fast`.
- SQ non-8-bit behavior is no longer ambiguous: it either works with real higher precision or fails early with a descriptive error.
- Cache codec roundtrips preserve all high-bit RQ/SQ fields and remain backward compatible with existing SQ8 and RQ1 cache entries.
- Tests cover cold search, warm/prewarmed cache search, optimize/remap, and at least one recall assertion against exact search.
- Public docs and Python/Java API docs match the implemented behavior.
