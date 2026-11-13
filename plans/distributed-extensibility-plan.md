# Lance distributed execution and extensibility plan

**Proposed for discussion, 2026-11-13.** Scope: PostgreSQL-family integration, Ray distributed workloads, Ascend NPU qualification, and extension boundaries that reduce recurring upgrade work. This plan is not an implementation or a commitment from upstream maintainers.

## Recommended direction

Evolve the existing Lance distributed primitives and public connectors into a tested composition: **SQL engine for SQL semantics; Ray for scheduling; Lance for storage/index correctness; optional backends for numerical acceleration.** Keep private algorithms and deployment policy behind documented interfaces, and contribute shared invariants and conformance upstream.

The project is not starting from zero: Lance has worker-build/publish APIs, serialized maintenance plans, metadata/provider seams and streaming-ingestion primitives; Lance-Ray already has distributed vector search; `pglance` exists. The major gaps are recoverability, bounded execution, version/capability contracts, modern SQL integration and qualified NPU kernels. See [current-state audit](docs/06-distributed-current-state.md) and [public ecosystem evidence](docs/07-ecosystem-evidence.md).

LanceDB includes an open-source embedded library alongside its commercial distributed service. The shared opportunity is reusable correctness, performance and extension interfaces that benefit both a managed service and Huawei's engine/hardware integrations. The proposed cooperation boundary is in [11](docs/11-upstream-and-upgrade-strategy.md).

## Scope and working assumptions

- PostgreSQL is the reference SQL engine pending a specific engine/version selection. openGauss/GaussDB-family compatibility is separately qualified.
- Begin with read-only snapshot-consistent SQL access and batch/maintenance workflows. Strong cross-system transactions, a full distributed SQL executor, and product control-plane features need separate requirements.
- Keep core CPU functionality and default builds independent of vendor runtimes. Separate NPU embedding generation from acceleration of Lance internals.
- Reuse existing namespace protocols, connector repositories, plans and commit handlers. Add interfaces only for demonstrated gaps and avoid a generic plugin framework redesign.
- Honor all stable file-format contracts and preserve legacy read behavior. New functionality targets current writers; unstable revisions do not become compatibility constraints.
- The audited implementation is upstream snapshot `042ccc837` (workspace `15.0.0-beta.10`), not the latest public main. Stable reference checked on the research date is `v14.0.2`. Refresh pins before implementation.

## Proposed milestones

Indicative schedule: **22 weeks** with approximately six full-time engineering roles, access to an Ascend test environment and timely upstream review. Roles: core lead, storage/runtime engineer, Ray engineer, SQL engineer, NPU engineer, and test/performance engineer. These are planning assumptions, not assigned people or delivery promises. A smaller team should preserve the dependency order and extend the schedule.

| Phase / window | Work and artifacts | Proposed accountable role | Dependencies | Exit gate |
| --- | --- | --- | --- | --- |
| P0 · weeks 1–2 | Confirm engine/version, deployment/SLOs and three workloads; reproduce connector baselines; inventory private patches; pin stable and development lanes; short RFCs | Core lead with all owners | Hardware/access and workload owners | E1/E4 baseline runs, tested dependency matrix, measured upgrade baseline, approved scope for first slices |
| P1 · weeks 3–6 | Recovery record and publication reconciliation; source/model/task identity; capability admission; exact coverage checks; retention policy; CPU reference tests | Storage/runtime + core | P0; maintainer review of shared seams | E3/E8 failure matrix passes for one operation; incompatible workers rejected; no silent duplicate publication |
| P2 · weeks 5–10 | Extend Lance-Ray: bounded fallback/search batches, compaction-aware assignment, durable index/maintenance orchestration, cancellation and resource budgets | Ray engineer + core | P0 for profiling; P1 for recovery guarantees | Multi-node 1/2/4/8-worker results, no table-size-proportional fallback memory, quality and recovery gates |
| P3 · weeks 7–12 | Modernize `pglance`; choose process boundary; snapshot-pinned reads, safe type mapping, streaming/cancellation, measured pushdown/costing | SQL engineer | P0 engine spike; shared scan contract from P1/P2 | E4/E5 conformance; explicit supported SQL/engine matrix and EXPLAIN behavior |
| P4 · weeks 9–16 | NPU runtime qualification; external embedding pipeline; assignment prototype; minimal optional compute seam; training/rerank only after profiles | NPU engineer + core | Hardware matrix; CPU reference; P1 provenance | E6 quality, CPU reopening, memory and end-to-end benefit; unsupported cases fail/fallback explicitly |
| P5 · weeks 17–22 | Integrated SQL/Ray/device trials, soak/failure tests, stable upgrade rehearsal, packaging and operating guide, upstream PRs and compatibility report | Test/performance engineer + owners | P1–P4 gates; maintainer feedback | E7 upgrade evidence, zero unresolved correctness failures, published supported combinations and known limits |

P2 profiling and the SQL/device spikes can run before P1 completes; their production claims depend on its contracts. Correctness dependency chain: P0 → P1 → P2 → P5, with P3/P4 also required for P5. In this schedule, P4 finishes in week 16 and controls P5's week-17 start; P2 finishes in week 10. Upstream merge/release availability is a separate dependency and can shift promotion dates. If shared changes remain unmerged, track a small explicit patch set and keep the integration experimental.

## Initial implementation backlog

Each item is a bounded RFC/PR series, not one large cross-language patch. IDs are local plan references, not existing GitHub issues. Priorities are **High** and **Normal**; P0–P5 identify schedule phases only.

| ID / priority | Slice and likely home | Depends on | Acceptance / evidence |
| --- | --- | --- | --- |
| B01 / High | Pin and reproduce public Lance-Ray, `pglance`, namespace and CPU workloads | None | Exact artifact matrix and baseline reports |
| B02 / High | Inventory internal patches and classify by semantic owner | None | Patch ledger, upgrade effort baseline, candidate public seams |
| B03 / High | Operation envelope + capability admission around existing plans | B01 | Unknown required features rejected; source identities retained |
| B04 / High | Durable coordinator/attempt selection and publication reconciliation in connector layer | B03 | Worker/driver failure injection, stale coordinator fencing and ambiguity recovery; evaluate existing `Transaction.uuid`/`read_transaction` for append first |
| B05 / High | Verify existing direct-commit checks; qualify upstream #10797 on upgrade; enforce remaining tuning/job/model identity and intended coverage in the coordinator | B01/B03 | Metric/dimension/sub-index/quantizer mismatch rejection already exists; test coverage after commit, including cleaned source versions. SQ mismatch rejection is fixed upstream; shared-bounds worker input needs separate evaluation |
| B06 / High | Bounded Ray flat fallback and shared metric/visibility semantics | B01 | Large uncovered scan stays within budget; exact/ANN comparison and null/deletion cases |
| B07 / Normal | Compaction-aware fragment/segment grouping in Lance-Ray | B01 | Compare contiguous/balanced coverage; preserve compaction and acceptable skew |
| B08 / Normal | Index optimization/compaction recovery using native plans | B04/B05 | Resume build/merge/publication without duplicate visibility; clean up only safe artifacts |
| B09 / High | `pglance` dependency/ABI modernization spike | B01 | One chosen engine major passes tiny scan/join and cancellation |
| B10 / Normal | SQL snapshot, type/predicate, streaming and cost contracts | B03/B09 | E5 semantics and honest EXPLAIN; unsupported work stays local |
| B11 / Normal | Ascend/Ray runtime qualification and embedding pipeline | B01 | Pinned image, resource visibility, output vectors accepted by CPU Lance |
| B12 / Normal | Optional assignment/rerank backend; training follows measurements | B05/B11 | Quality, transfer-inclusive performance, canonical serialization, CPU-only package usability |
| B13 / Normal | Current-format vector extension RFC if backend/private-index use case requires it | B02/B12 | Two consumers or reference implementations; no legacy-loader retrofit |
| B14 / High | Conformance harness, metrics and failure fixtures in owner suites | Starts with B03; extends throughout | [12](docs/12-validation-and-experiments.md) gates pass; no Ray-only copy of core invariants |
| B15 / Normal | Stable upgrade rehearsal and compatibility adapter packaging | B08/B10/B12/B14 | Recorded engineering effort and patch reduction; private logic preserved behind contracts |

Current issues to coordinate with include model readers, segment identity, index optimization, reusable Ray search and interleaved coverage. The [dated watchlist](docs/07-ecosystem-evidence.md#current-collaboration-watchlist) records their actual status; notably distributed-training PR #7321 is **closed unmerged**. Search for overlapping work before creating new issues or PRs.

## Gates and measurements

Correctness precedes scale: pin snapshots, validate intended coverage, account for all attempts, and resolve ambiguous commits. Test multi-fragment data, null/empty cases, deletes/overlays and compaction races. ANN tests must assert recall, with the repository's floor of 0.5 and a stricter workload target. Proposed NPU promotion requires at least 1.5x end-to-end improvement for a selected operation at equal quality; it is a hypothesis to test, not a promised gain.

Memory budgets cover driver, workers, object-store/shuffle buffers, Python/Ray objects and device allocations. Measure queue time and transfer costs. For upgrades, count private core patches, adapter edits and engineering hours rather than claiming interface stability from file-format stability. See the complete [validation and experiments](docs/12-validation-and-experiments.md).

## Risks and decisions to retire

| Risk / unresolved decision | Owner | Resolution point |
| --- | --- | --- |
| Exact SQL engine and extension deployment policy | SQL lead / product owner | P0; default PostgreSQL reference until selected |
| Current connector dependency skew | SQL and Ray owners | P0; qualify artifacts instead of broad minimum-version claims |
| External publication/ledger split | Storage/runtime | P1 fault injection; one authoritative publication path and transaction-identity reconciliation |
| Default cleanup deletes uncommitted index files after seven days, even during active jobs | Storage/runtime | P0 operating constraint: tag source snapshots and finish build plus recovery within seven days with `delete_unverified=False`, or suspend cleanup. `older_than` cannot extend the grace period; longer jobs alongside cleanup require a core retention change |
| Poor scaling from fanout, driver aggregation or skew | Ray/performance | P2 E1/E2 results; segment/concurrency budgets |
| NPU runtime compatibility or weak end-to-end benefit | NPU owner | P4 gate; retain CPU/reference path and stop unhelpful kernels |
| Private plugin tied to legacy or unstable APIs | Core lead | P1/B13 RFC; additive current-format seam, versioned adapter and rebuild policy |
| Cross-table SQL consistency or synchronous writes expected | Product and SQL owners | P0 scope decision; separate transaction design if required |
| Upstream design/merge timing | Core lead and maintainers | Every milestone; track disagreements and small temporary patches |

## First ten working days

Days 1–2: confirm workload, engine, deployment and hardware assumptions; refresh public issues/releases and inventory internal features. Days 3–5: reproduce a stable Ray baseline and PostgreSQL FDW scan; collect profiles and dependency gaps. Days 6–8: run one crash/retry demonstration and one NPU scheduling/embedding experiment, with honest limitations. Days 9–10: review the small execution-contract RFCs, select the first core and connector changes, and set measured latency/recovery/upgrade targets.

## Reading and onboarding

Open [distributed-onboarding.html](distributed-onboarding.html) for the illustrated walkthrough, ownership map, recovery exercise, NPU cost calculator and phase guide. The [documentation index](README.md) connects the existing vector guides with this new program. Start a contributor with [06](docs/06-distributed-current-state.md), [08](docs/08-extensible-target-architecture.md), their integration chapter, and [13's labeled examples](docs/13-examples-and-contracts.md).
