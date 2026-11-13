# Validation, benchmarks, and risk retirement

Status: **proposed gates**, 2026-11-13. No measurements in this document are results. Thresholds are starting criteria to confirm with workload owners during phase 0.

## First collect a reproducible baseline

Record every run's dataset generation/seed, dimensions, metrics, fragment and index-segment counts, deletion/overlay distribution, code commits, lockfiles, hardware, storage endpoint characteristics, worker count, CPU/thread limits, batch sizes, model digest and precision. Record warm/cold cache state separately. Keep query ground truth pinned to the same snapshot as the measured run.

Choose three workloads: ingestion plus index maintenance; SQL filtered scan/join plus vector search; NPU embedding/training/assignment pipeline. Include a skewed dataset and a workload with many small segments. Public synthetic workloads should be sufficient to reproduce correctness and comparative performance; private workload data is optional.

## Experiments and decision gates

| ID | Experiment | Evidence required | Decision |
| --- | --- | --- | --- |
| E1 | Existing Lance-Ray at 1/2/4/8 workers, same data and quality | Throughput, p95 latency, efficiency, driver RSS, worker RSS, bytes transferred | Identify scheduler, I/O, candidate-merge or compute bottleneck before adding mechanisms |
| E2 | Contiguous vs balanced/interleaved fragment groups | Build skew, segment coverage shape, post-build compaction eligibility and query fanout | Choose a compaction-aware assignment policy; do not optimize build time alone |
| E3 | Kill workers and coordinator at each publication stage | Durable plan, selected attempt, exact visible coverage, orphan accounting and reconciled outcome | Admit recovery protocol only after duplicate attempts and lost acknowledgements are safe |
| E4 | `pglance` modernization and boundary spike | Exact engine/Lance/pgrx/Arrow matrix, semantic comparison, cancellation, batch throughput and RSS | Select in-process or sidecar design on evidence |
| E5 | SQL snapshot/filter/type conformance | Multi-fragment rescans, joins, nulls, casts, collation, timezone, LIMIT with residual filters | Enable only tested pushdown/snapshot guarantees |
| E6 | NPU assignment/training/rerank against CPU | Stage and wall-clock time, transfers, memory, distance error, recall, CPU reopening | Select only kernels with useful end-to-end benefit and matching semantics |
| E7 | Mixed-version admission and stable upgrade rehearsal | Rejected unsupported plans; stable historical fixture results; adapter changes and engineer time | Qualify the release combination and quantify maintenance savings |
| E8 | Storage/metadata adapter conformance | Concurrent publishers, conditional operations, ambiguous outcomes, credential refresh and GC behavior | Admit a provider/manifest implementation only for the guarantees it passes |

Report distributed efficiency as `throughput(W) / (W * throughput(1))` at the same input size and correctness criteria. Include both strong scaling (fixed total input) and weak scaling (fixed input per worker) when useful. Do not mix them into one speedup claim.

## Correctness matrix

| Area | Required cases and assertions |
| --- | --- |
| Dataset visibility | Multiple fragments, deleted rows, sparse overlays, stable row IDs and physical-address remapping; no stale or resurrected rows |
| Index ownership | Disjoint assignment, uncovered fragments, multiple segments per logical index, duplicate attempts and partial replacement; intended coverage must be accounted for |
| Vector quality | Exact ground truth at the same snapshot; L2/cosine/dot as supported; null rows/elements, empty inputs, zero vectors, NaNs and wrong dimensions according to the selected API contract |
| ANN global merge | Candidate oversampling, different segment models, refinement, filters and ties; measure recall instead of checking only successful execution |
| Physical merge | Identical versus incompatible model artifacts, unsupported layout composition, source changes; rejection must occur before publication |
| Compaction / update races | Index build versus deletion/update/compaction; source version cleanup; preserved row identity or explicit conflict/replan |
| SQL | Projection of residual-filter columns, null three-valued logic, overflow, decimals, timezone/collation, parameterized rescans, cancellation and transaction lifecycle |
| Resource bounds | Large uncovered-fragment fallback, skewed partitions, bounded in-flight batches, driver candidate/metadata growth, spill and cancellation |
| Devices | CPU-only installation, missing accelerator, unsupported operation, OOM, failure during async copy, precision drift, deterministic seed where supported |
| Protocol | Unknown required capability, stale coordinator epoch, wrong plan/model/schema digest, truncated descriptor and mismatched worker version |

All new bugs/features require regression tests in the owning existing suites. Use `rstest` or parametrized pytest for input variations, descriptive cases, and small unit fixtures (normally under one second each). Large cluster/device experiments belong in integration/performance suites; do not weaken correctness/recall assertions to shorten tests.

## Failure matrix

| Injection point | Expected behavior |
| --- | --- |
| Worker dies before writing | Retry a new attempt; no publication |
| Worker dies after writing, before reporting | Keep the source tag; recover within the fixed seven-day artifact grace period with `delete_unverified=False`, or keep cleanup suspended; another attempt may be selected |
| Two attempts finish | One selected descriptor per task; duplicate coverage never reaches publication |
| Coordinator dies after barrier | Recover persisted intent and revalidate source/capabilities before publishing |
| Publication lands but acknowledgement is lost | Read the authoritative outcome; do not blindly append/rebuild/clean up |
| Source advances or fragments are rewritten | Enforce the job policy: validated compatible commit, explicit reduced-coverage result where allowed, or replan; no silent promise of exact coverage |
| Stale coordinator resumes | Fenced record updates and publication prevent a second owner from acting |
| GC overlaps an active job | Source tags already protect snapshots. Uncommitted index files remain eligible after seven days even for active jobs; require build plus recovery inside that fixed window with `delete_unverified=False`, or suspend cleanup. Protection beyond seven days requires a core retention change before a no-age-only-deletion gate can pass |
| NPU fails mid-batch | Discard incomplete result; explicit failure or a new recorded fallback attempt |
| Query worker times out | Fail the complete query by default; no undisclosed partial top-k |

Exactly-once **visibility** is an objective of the selected publication/recovery protocol, not a claim of exactly-once task execution. Evaluate the existing `Transaction.uuid` and `read_transaction` APIs for append reconciliation before designing another deduplication mechanism. Source inspection shows append keeps its UUID through internal commit retries; that does not prove a fresh client submission is idempotent. Extend E3 with conflict retries, lost acknowledgement, coordinator restart, later commits and cleanup of transaction history. Persist the transaction identity before publication, inspect all candidate committed versions, and retain uncertainty if history is unavailable. The source trace and remaining protocol requirements are in [13 §E](13-examples-and-contracts.md#e-proposed-coordinator-algorithm).

## Proposed quantitative acceptance criteria

- **Correctness:** zero silent duplicate/lost rows, stale visibility, unvalidated model mixing or unknown commit outcomes in the scripted failure matrix.
- **Recall:** repository minimum `recall@k >= 0.5` where applicable, plus a workload-specific target at least as strict as the CPU/reference baseline. Initial proposal: no more than one percentage point absolute recall loss versus the agreed baseline. Raising this floor is expected for production workloads.
- **NPU value:** initial promotion target of at least `1.5x` end-to-end improvement for one selected operation/workload at equal quality, including conversion/transfer; otherwise keep it experimental or stop that kernel effort. This is a proposed decision threshold, not a predicted gain.
- **CPU regression:** initial threshold no more than 5% repeatable p95/throughput regression in relevant CPU baselines from introducing optional backends. Evaluate variance/confidence before failing a change.
- **Memory:** configure and publish a per-process/host/device budget; no OOM at the qualified concurrency, and memory must plateau for fixed batch size rather than grow with all scanned rows. Include driver metadata/candidate accumulation.
- **Upgrade:** complete a documented stable-release upgrade rehearsal without rewriting private business logic. Quantify patches, changed adapter files and engineering time against the phase-0 baseline; set a numeric effort target after measuring the baseline.
- **Recovery:** all scripted failure cases converge to a known terminal state within the product's agreed recovery objective. Set a numeric SLO from the deployment requirements in phase 0; no fabricated availability claim.

## Existing tests and commands to extend

Relevant existing locations include [index tests](../../rust/lance/src/index/tests.rs), [optimization implementation/tests](../../rust/lance/src/index/optimize.rs), [concurrent compaction/reindex tests](../../rust/lance/src/dataset/optimize/tests/concurrent_reindex.rs), [external manifest tests](../../rust/lance-table/src/io/commit/external_manifest.rs), [Python commit-index tests](../../python/python/tests/test_commit_index.py), [vector-index tests](../../python/python/tests/test_vector_index.py), and [fragment tests](../../python/python/tests/test_fragment.py). For example, the concurrent reindex suite already exercises commits past compaction and cleaned-up source versions; extend it instead of reproducing those invariants in a Ray-only test.

Before running implementation checks, follow [rust/AGENTS.md](../../rust/AGENTS.md), [python/AGENTS.md](../../python/AGENTS.md) or [java/AGENTS.md](../../java/AGENTS.md). Required Rust gates include `cargo fmt --all`, `cargo clippy --all --tests --benches -- -D warnings`, and appropriate targeted tests/checks. Python lint uses the documented environment and `uv run make lint` from `python/`. Benchmarks use the repository's `release-with-debug` profile; `release-no-lto` is restricted to the uses documented by the root guide.

Stable historical compatibility uses checked-in `test_data` fixtures with version-asserting generators and only stable releases in the automatic compatibility matrix. New branch/prerelease capability tests stay in ordinary integration tests. Hardware absence can block device qualification but should not block CPU reference and protocol tests; a skipped hardware test needs a tracking issue under repository policy.
