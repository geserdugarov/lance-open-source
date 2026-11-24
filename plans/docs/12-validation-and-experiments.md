# Validation, benchmarks, and risk retirement

Status: **proposed gates**, 2026-11-13. No measurements in this document are results. Thresholds are starting criteria to confirm with workload owners during phase 0.

Use `ded121cd9a99dc83040cf41049dc2bd3963241b9` (2026-11-24, `15.0.0-beta.29`) for the development reference;
`v14.0.3` is the latest local stable tag. Keep external connector pins separate
and do not treat this source refresh as a completed integration run. E9 and
the refactor-invariance row were added on 2026-11-24 for the upstream
crate-architecture proposal
([07](07-ecosystem-evidence.md#upstream-crate-architecture-proposal)).

## First collect a reproducible baseline

Record every run's dataset generation/seed, dimensions, metrics, fragment and index-segment counts, deletion/overlay distribution, code commits, lockfiles, hardware, storage endpoint characteristics, worker count, CPU/thread limits, batch sizes, model digest and precision. Record warm/cold cache state separately. Keep query ground truth pinned to the same snapshot as the measured run.

Choose three workloads: ingestion plus index maintenance; SQL filtered scan/join plus vector search; NPU embedding/training/assignment pipeline. Include a skewed dataset and a workload with many small segments. Public synthetic workloads should be sufficient to reproduce correctness and comparative performance.

## Experiments and decision gates

| ID | Experiment | Evidence required | Decision |
| --- | --- | --- | --- |
| E1 | Lance-Ray pinned at `f155c32` or a qualified descendant, at 1/2/4/8 workers | First reproduce v0.5.0's fallback L2/Dot mismatch separately; require corrected metrics for quality numbers. Test overlay-updated rows within covered fragments and compare identities/distances with single-node Lance at the same snapshot. Then record throughput, p95 latency, efficiency, driver/worker RSS and transferred bytes | Reject silent row omission or incompatible scores before interpreting scaling; isolate scheduler, I/O, candidate-merge and compute costs |
| E2 | Contiguous vs balanced/interleaved fragment groups | Build skew, segment coverage shape, post-build compaction eligibility and query fanout | Choose a compaction-aware assignment policy; do not optimize build time alone |
| E3 | Kill workers and coordinator at each publication stage | Durable plan, selected attempt, exact visible coverage, orphan accounting and reconciled outcome | Admit recovery protocol only after duplicate attempts and lost acknowledgements are safe |
| E4 | `pglance` modernization and boundary spike | Exact engine/Lance/pgrx/Arrow matrix, semantic comparison, cancellation, batch throughput and RSS | Select in-process or sidecar design on evidence |
| E5 | SQL snapshot/filter/type conformance | Multi-fragment rescans, joins, nulls, casts, collation, timezone, LIMIT with residual filters | Enable only tested pushdown/snapshot guarantees |
| E6 | NPU assignment/training/rerank against CPU | Stage and wall-clock time, transfers, memory, distance error, recall, CPU reopening | Select only kernels with useful end-to-end benefit and matching semantics |
| E7 | Mixed-version admission and stable upgrade rehearsal | Rejected unsupported plans; stable historical fixture results; adapter changes and engineer time | Qualify the release combination and quantify maintenance savings |
| E8 | Storage/metadata adapter conformance | Concurrent publishers, conditional operations, ambiguous outcomes, credential refresh and GC behavior | Admit a provider/manifest implementation only for the guarantees it passes |
| E9 | Rebuild the pinned adapters across an upstream crate-refactor step | Unchanged adapter source against the public `lance` / `pylance` API; identical query results; plans serialized before the step still admitted or explicitly rejected after it; dependency closure (`cargo tree`) and build, clippy and test timings before and after | Advance the development pin only when the re-exports hold; consider a direct sub-crate dependency only after upstream decides on it |

Report distributed efficiency as `throughput(W) / (W * throughput(1))` at the same input size and correctness criteria. Include both strong scaling (fixed total input) and weak scaling (fixed input per worker) when useful. Do not mix them into one speedup claim.

## Correctness matrix

| Area | Required cases and assertions |
| --- | --- |
| Dataset visibility | Multiple fragments, deleted rows, sparse overlays, stable row IDs and physical-address remapping; no stale or resurrected rows |
| Index ownership | Disjoint assignment, uncovered fragments plus stale overlay rows inside covered fragments, multiple segments per logical index, duplicate attempts and partial replacement; every fallback row must have one task owner |
| Vector quality | Use exact top-k at the same snapshot for L2, cosine and Dot. Compare single-process, distributed and accelerated Dot IVF-PQ with refinement disabled. Include null rows and elements, sliced multivectors, empty inputs, zero vectors, NaNs and invalid dimensions. |
| ANN global merge | Compare identities/distances with single-node Lance at the same snapshot and options; cover mixed indexed/fallback L2/Dot scores, implicit exact rescore, code-only RQ, oversampling, different models, filters and ties; also assert recall against exact ground truth |
| Physical merge | Identical versus incompatible model artifacts, unsupported layout composition, source changes; rejection must occur before publication |
| Consolidation | Carry the same plan/options to commit in each binding; reject partial surviving selected coverage; distinguish bounded suffix model reuse from Rust selected-segment retraining |
| Current query modes | Compare both IVF_HNSW_RQ graph-scoring modes to core, including propagation to every worker. Cover flat-RQ code-only refinement, adaptive shared scans, cross-column refinement and WARP/XTR eligibility. |
| Metadata / cache identity | Independent covering fields, persisted scalar result domains, every merged build contribution, snapshot changes and table recreation; no reuse of stale listings/rankings |
| Compaction / update races | Race index builds against deletes, updates, rewrites and source-version cleanup. Check IVF_HNSW recall after deferred compaction on cold load and prewarm. Check row identity and index coverage after repacking, and recover from a failed repack following a published rewrite. |
| SQL | Projection of residual-filter columns, null three-valued logic, overflow, decimals, timezone/collation, parameterized rescans, cancellation and transaction lifecycle |
| Resource bounds | Large uncovered-fragment fallback, skewed partitions, bounded in-flight batches, driver candidate/metadata growth, spill and cancellation |
| Devices | CPU-only installation, missing accelerator, unsupported operation, OOM, failure during async copy, precision drift, deterministic seed where supported |
| Protocol | Unknown required capability, stale coordinator epoch, wrong plan/model/schema digest, truncated descriptor and mismatched worker version |
| Refactor invariance | Same visibility, coverage, fallback and scoring results before and after an upstream crate move; public paths still resolve through re-exports; no persisted-format change |

Exercise a Rust composite commit with and without the experimental opt-in.
Verify one manifest publication for valid actions and rejection of unknown
actions. Incompatible vector segments must fail before the new manifest is
published. Cover disjoint and conflicting action footprints and
conflicts with named operations. Wire transport tests must retain explicit
empty fragment selections and required list-structure/visibility projections.
[Composite transaction tests](../../rust/lance/src/dataset/tests/dataset_transactions.rs)

All new bugs/features require regression tests in the owning existing suites. Use `rstest` or parametrized pytest for input variations, descriptive cases, and small unit fixtures (normally under one second each). Large cluster/device experiments belong in integration/performance suites; do not weaken correctness/recall assertions to shorten tests.

## Failure matrix

| Injection point | Expected behavior |
| --- | --- |
| Worker dies before writing | Retry a new attempt; no publication |
| Worker dies after writing, before reporting | Keep the source tag; set `error_if_tagged_old_versions=False` on concurrent cleanup so an old tag does not abort it. Recover within the seven-day artifact grace period with `delete_unverified=False`, or keep cleanup suspended; another attempt may be selected |
| Two attempts finish | One selected descriptor per task; duplicate coverage never reaches publication |
| Coordinator dies after barrier | Recover persisted intent and revalidate source/capabilities before publishing |
| Publication lands but acknowledgement is lost | Read the authoritative outcome; do not blindly append/rebuild/clean up |
| Source advances or fragments are rewritten | Enforce the job policy: validated compatible commit, explicit reduced-coverage result where allowed, or replan; no silent promise of exact coverage |
| Source snapshot disappears | Without tagged FRI history, commit can clear coverage; with tagged FRI history, commit/merge staged replay rejects the missing source and requires rebuilding. Exercise both and check effective coverage after success; FRI lineage tags are distinct from retention tags |
| Stale coordinator resumes | Fenced record updates and publication prevent a second owner from acting |
| GC overlaps an active job | Tags retain snapshots, but default `error_if_tagged_old_versions=True` raises on an old tagged version. Test that failure and configure `False` for cleanup around retained tags. Uncommitted index files remain eligible after seven days; require build plus recovery inside that fixed window with `delete_unverified=False`, or suspend cleanup. Protection beyond seven days requires a core retention change |
| NPU fails mid-batch | Discard incomplete result; explicit failure or a new recorded fallback attempt |
| Query worker times out | Fail the complete query by default; no undisclosed partial top-k |

Exactly-once **visibility** is an objective of the selected publication/recovery protocol, not a claim of exactly-once task execution. Evaluate the existing `Transaction.uuid` and `read_transaction` APIs for append reconciliation before designing another deduplication mechanism. Source inspection shows append keeps its UUID through internal commit retries; that does not prove a fresh client submission is idempotent. Extend E3 with conflict retries, lost acknowledgement, coordinator restart, later commits and cleanup of transaction history. Persist the transaction identity before publication, inspect all candidate committed versions, and retain uncertainty if history is unavailable. The source trace and remaining protocol requirements are in [13 §E](13-examples-and-contracts.md#e-proposed-coordinator-algorithm).

## Proposed quantitative acceptance criteria

- **Correctness:** zero silent duplicate/lost rows, stale visibility, unvalidated model mixing or unknown commit outcomes in the scripted failure matrix.
- **Recall:** repository minimum `recall@k >= 0.5` where applicable, plus a workload-specific target at least as strict as the CPU/reference baseline. Initial proposal: no more than one percentage point absolute recall loss versus the agreed baseline. Raising this floor is expected for production workloads.
- **NPU value:** initial promotion target of at least `1.5x` end-to-end improvement for one selected operation/workload at equal quality, including conversion/transfer; otherwise keep it experimental or stop that kernel effort. This is a proposed decision threshold, not a predicted gain.
- **CPU regression:** initial threshold no more than 5% repeatable p95/throughput regression in relevant CPU baselines from introducing optional backends. Evaluate variance/confidence before failing a change.
- **Memory:** configure and publish a per-process/host/device budget; no OOM at the qualified concurrency, and memory must plateau for fixed batch size rather than grow with all scanned rows. Include driver metadata/candidate accumulation.
- **Upgrade:** complete a documented stable-release upgrade rehearsal with matching adapter behavior. Quantify changed adapter files and engineering time against the initial baseline; set an effort target after measuring that baseline. Once the upstream crate refactor starts landing, include one upgrade that crosses a refactor step (E9).
- **Recovery:** all scripted failure cases converge to a known terminal state within the product's agreed recovery objective. Set a numeric SLO from the deployment requirements in phase 0; no fabricated availability claim.

## Existing tests and commands to extend

Relevant existing locations include [index tests](../../rust/lance/src/index/tests/), [optimization implementation/tests](../../rust/lance/src/index/optimize.rs), [concurrent compaction/reindex tests](../../rust/lance/src/dataset/optimize/tests/concurrent_reindex.rs), [external manifest tests](../../rust/lance-table/src/io/commit/external_manifest.rs), [Python commit-index tests](../../python/python/tests/test_commit_index.py), [vector-index tests](../../python/python/tests/test_vector_index.py), and [fragment tests](../../python/python/tests/test_fragment.py). For example, the concurrent reindex suite already exercises commits past compaction and cleaned-up source versions; extend it instead of reproducing those invariants in a Ray-only test.

These are baseline locations. The upstream crate refactor would move some of them and proposes per-interface contract tests, including one suite every index plugin must pass (train, remap, search with prefilter), run against each implementation and a fake. When those suites exist, put adapter and backend conformance cases into them instead of maintaining a parallel harness.

Before running implementation checks, follow [rust/AGENTS.md](../../rust/AGENTS.md), [python/AGENTS.md](../../python/AGENTS.md) or [java/AGENTS.md](../../java/AGENTS.md). Required Rust gates include `cargo fmt --all`, `cargo clippy --all --tests --benches -- -D warnings`, and appropriate targeted tests/checks. Python lint uses the documented environment and `uv run make lint` from `python/`. Benchmarks use the repository's `release-with-debug` profile; `release-no-lto` is restricted to the uses documented by the root guide.

Stable historical compatibility uses checked-in `test_data` fixtures with version-asserting generators and only stable releases in the automatic compatibility matrix. New branch/prerelease capability tests stay in ordinary integration tests. Hardware absence can block device qualification but should not block CPU reference and protocol tests; a skipped hardware test needs a tracking issue under repository policy.
