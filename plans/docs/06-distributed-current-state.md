# Distributed Lance: current state and extension audit

External research date: **2026-11-13**. Lance implementation refreshed against
`67faafe65` on **2026-11-16**. Status: evidence and assessment, not a feature announcement.

## Recommendation

Build a reusable distributed integration layer around Lance's existing storage protocols. Extend Lance-Ray and evaluate the existing `pglance` adapter before starting replacements. Keep SQL planning, cluster scheduling, device execution, and operational policy in separate owners; keep visibility, row identity, index coverage, and publication validation in Lance.

Lance already has distributed work units. The main opportunity is making those units easier to operate, recover, integrate, and upgrade. A managed distributed database also needs routing, resource control, availability, security, monitoring, and support; storage primitives alone do not provide these properties.

## What was inspected

| Evidence boundary | Exact baseline | Interpretation |
| --- | --- | --- |
| Implementation baseline | `67faafe651d3bbec9ee537d7313890328e9e7fa3`, 2026-11-16 | Later planning and agent-configuration commits are outside this implementation baseline |
| Workspace package | `15.0.0-beta.22` | Development APIs must not be advertised as stable release capabilities |
| Latest stable tag in this checkout | `v14.0.3`, release commit dated 2026-11-15 | Stable reference from local history; this audit did not run its binaries |
| External integration evidence | Repository/release pins recorded on 2026-11-13 in chapter 07 | Kept at their recorded revisions; the Lance rebase does not qualify those adapters against the new core |

External releases, repository pins, and dated issue states are in [07-ecosystem-evidence.md](07-ecosystem-evidence.md). The [vector architecture overview](00-overview.md) and [distributed index guide](05-distributed-vector-index-creation.md) use the same `67faafe65` implementation baseline; external issue-status refreshes are dated separately.

Classification used throughout this plan:

- **Implemented:** found in the inspected source, with a named API or test. This does not mean production certification.
- **Published:** independently visible in a linked external release or repository; a published development branch is not a release guarantee.
- **Proposed:** design or acceptance criteria in this documentation; no implementation is claimed.
- **Unverified:** integration, performance, or service behavior for which evidence is insufficient.

## Capability inventory

All source links below resolve within this checkout. For a permanent upstream link, use `https://github.com/lance-format/lance/blob/67faafe651d3bbec9ee537d7313890328e9e7fa3/` followed by the same repository path.

| Capability | Implemented surface and evidence | What an external system still owns |
| --- | --- | --- |
| Distributed ingestion | [Distributed write guide](../../docs/src/guide/distributed_write.md), [Python fragment writer](../../python/python/lance/fragment.py): `write_fragments`; coordinator uses `LanceDataset.commit` | Input partition assignment, selecting one successful attempt, replay protection, durable job state |
| Concurrent publication | [CommitHandler](../../rust/lance-table/src/io/commit.rs), conditional put / rename / external handlers; [transaction processing](../../rust/lance-table/src/transaction.rs) | Correct store capabilities, one authoritative publication path, commit-outcome reconciliation |
| Transaction V2 wire specification | [Action schemas](../../protos/transaction/actions.proto) and [specification](../../docs/src/format/table/transaction.md) are present, but [runtime decoding](../../rust/lance-table/src/transaction/proto.rs) rejects `CompositeOperation` | Do not advertise multi-action execution from schema availability; named operations remain the implemented path |
| External metadata authority | [ExternalManifestStore](../../rust/lance-table/src/io/commit/external_manifest.rs) and [namespace integration](../../rust/lance-namespace/src/lib.rs) | Adapter implementation, database/service availability, fencing and conformance; an external store is not automatically a PostgreSQL implementation |
| Distributed index creation | [DatasetIndexExt / IndexSegment](../../rust/lance/src/index/api.rs), [lifecycle](../../rust/lance/src/index/lifecycle.rs), [Python dataset](../../python/python/lance/dataset.py): uncommitted build, optional merge, commit | Snapshot pinning, target coverage, model identity, task/attempt selection, recovery |
| Distributed index optimization | [OptimizeIndicesPlan](../../rust/lance/src/index/optimize.rs): serialized plans, build/merge tasks, commit; Rust/Python/Java bounded consolidation, Rust UUID-selected consolidation | Persist planning options through commit; distinguish no-training suffix consolidation from selected-segment rebuilding; validate full selected coverage |
| Distributed compaction | [CompactionPlan / CompactionPlanner](../../rust/lance/src/dataset/optimize/planning.rs), [CompactionTask / RewriteResult](../../rust/lance/src/dataset/optimize/execution.rs), [commit](../../rust/lance/src/dataset/optimize/commit.rs) | Task execution policy, transient storage budgets, task-version admission and cleanup coordination |
| Query subdivision | [Scanner source](../../rust/lance/src/dataset/scanner/source.rs), [vector planning](../../rust/lance/src/dataset/scanner/vector.rs), Python `index_segments` and fragment selection | Interprocess dispatch, global merge, cancellation, admission control, and service availability; Lance-Ray supplies a published distributed vector-search implementation |
| Overlay visibility / query caching | Internal [IndexVisibility](../../rust/lance/src/index/visibility.rs) shares exclusion/replay selections; [session caches](../../rust/lance/src/session/index_caches.rs) bind listings and eligible RQ rankings to snapshots | Reuse core semantics; an internal owner is not yet a public distributed fallback-selection API |
| Streaming ingestion primitives | [MemWAL](../../rust/lance/src/dataset/mem_wal.rs), shard manifests, writer epochs and durable/non-durable modes | Writer placement, failover service, workload routing and durability configuration; this is not PostgreSQL WAL or proof of distributed SQL transactions |
| Object storage / I/O extension | [ObjectStoreProvider / registry](../../rust/lance-io/src/object_store/providers.rs), [wrappers and credentials](../../rust/lance-io/src/object_store.rs) | Provider integration, credential policy, cache topology and deployment |
| Scalar index plugins | [register_scalar_index_plugin](../../rust/lance-index/src/registry.rs), [ScalarIndexPlugin](../../rust/lance-index/src/scalar/registry.rs), whole-predicate planning, optional [lance-geo RTree](../../rust/lance-geo/src/rtree.rs) | Static linking and registration before the first registry; same-version compilation; independent key/covering-field and row-domain semantics |
| Custom vector index extension | [Session::register_index_extension](../../rust/lance/src/session.rs) is `pub`, but [VectorIndexExtension](../../rust/lance/src/session/index_extension.rs) lives in a `pub(crate)` module | External crates cannot name/implement the internal trait despite the public registration method; it also loads through legacy `V1FileReader`. A current-format external seam requires an additive core change |
| Accelerator training | [Python vector helpers](../../python/python/lance/vector.py), [Torch k-means](../../python/python/lance/torch/kmeans.py), [CPU linear algebra](../../rust/lance-linalg/src/lib.rs) | Current string validation accepts CUDA/MPS; no verified end-to-end Ascend backend in this snapshot |

## Architecture and actual coupling

The repository already separates important owners:

~~~text
SQL / Ray / Python / Java adapters
`-- lance: dataset, scanner, index lifecycle, compaction, MemWAL
    |-- lance-table: manifests, transactions, index metadata
    |-- lance-index: index implementations and plugins
    |   |-- lance-index-core: index contracts
    |   `-- lance-linalg: numerical algorithms
    |-- lance-geo (optional): RTree implementation and registration
    |-- lance-file / lance-encoding: physical format
    |-- lance-io: object stores, scheduling, readers
    `-- lance-datafusion*: expression and execution integration
~~~

This is an ownership diagram, not a claim that all Cargo dependencies form a perfectly isolated stack. [ARCHITECTURE.md](../../ARCHITECTURE.md) is the authoritative navigation guide. In particular, `lance-index-core` still depends on DataFusion common, expression and execution crates; `lance-datafusion-exec` avoids the umbrella `datafusion` crate but uses its component crates. A PostgreSQL adapter cannot assume that the index core is already engine-neutral at the type or dependency level. See their [Cargo manifest](../../rust/lance-index-core/Cargo.toml) and [execution manifest](../../rust/lance-datafusion-exec/Cargo.toml).

| Boundary | Assessment | Smallest useful next step |
| --- | --- | --- |
| Plan / execute / commit | Strong separation already exists in compaction and index optimization | Reuse these payloads; add a negotiated execution envelope and recovery tests where semantics are missing |
| Index algorithm / dataset lifecycle | Generally clear ownership; provenance and visibility remain dataset concerns | Keep new accelerator kernels below lifecycle logic; avoid implementing deletion handling in each adapter |
| Engine / storage execution | DataFusion types appear in public/internal interfaces | Translate a bounded scan/search request at the outer boundary; extract shared contracts only when two implementations need them |
| Storage / deployment | Provider and wrapper seams are useful | Exercise an external provider and a cache wrapper against the same conformance tests |
| Plugins / release cadence | Scalar registry explicitly documents an unstable Rust interface; there is no externally implementable vector extension point | Recompile scalar plugins per supported release; propose an additive current-format vector seam after maintainer review if a concrete consumer needs it |
| Python / Rust numerical behavior | Accelerator helpers contain device selection and training orchestration | Keep Python device integration thin, and centralize persisted model validation in Rust |
| Scheduler / resource use | Task separation exists, but worker concurrency multiplies internal threads and memory | Express budgets, measure nested parallelism, and cap per-task allocation/concurrency |

## Correctness details that affect distributed designs

A pinned numeric dataset version is necessary but insufficient as a complete job identity. Bind work to table identity/incarnation, branch, schema/field IDs, operation parameters, and model artifacts as well. Do not pass credentials inside serialized plans.

For segment creation, Lance checks structural and provenance properties, including overlapping coverage and field identity. Direct commit also rejects coexisting vector segments with different metrics, dimensions, sub-index types or quantizer types. The coordinator must verify tuning parameters, intended coverage and job/model/attempt identity. Direct commit supports separately trained physical segments; physical merge has stricter model requirements. SQ creation merge rejects unequal dimensions, bit widths and learned bounds ([#10797](https://github.com/lance-format/lance/pull/10797), included here), without a shared-bounds worker input. IVF_HNSW_RQ is also implemented, including graph-rebuilding merges. The detailed checks and creation-versus-optimization restrictions are in [05, sections 6–9](05-distributed-vector-index-creation.md#6-validation-and-commit-semantics).

Merged coverage must be fresh against every contributing build version, not
just one representative version. Current manifests may store independent
covering fields; use `keyed_fields()` and `dependency_fields()` rather than
reconstructing dependencies in an adapter. Current JSON/NGram/RTree/MinHash
layouts can return physical addresses even on stable-row-ID tables.

The query contract includes adaptive shared scans, cross-column refinement and
WARP multivector search. These do not automatically extend Lance-Ray's pinned
search implementation. Admit supported request shapes explicitly and compare
against the core scanner; see [03](03-index-on-disk-and-search.md).

Memory observations cover build stages, scheduler/pool ownership and execution
reservation peaks. They help diagnose distributed budgets but are not one
enforced process or cluster limit.

Workers write artifacts before publication. Task retry can therefore leave objects even when the task reports failure. Select one successful result per task and use a fresh artifact UUID for each attempt. On an ambiguous commit result, resolve publication before retrying or deleting anything.

Tags protect source snapshots, but cleanup defaults to `error_if_tagged_old_versions=True` and raises when a tagged version is old enough to clean. Set `error_if_tagged_old_versions=False` on concurrent cleanup jobs to retain the snapshot and clean around it, or suspend cleanup. Tags do not protect uncommitted artifacts: cleanup can delete unreferenced index files after a fixed seven days, independent of `older_than`; `delete_unverified=True` removes the grace period. Finish build plus recovery inside that window with `delete_unverified=False`, or suspend cleanup. Longer jobs alongside cleanup require a core retention change. If a source version disappears, commit can clear coverage without tagged fragment-reuse history; with tagged FRI history, commit/merge staged replay rejects the missing source and requires rebuilding. Verify effective coverage after successful publication. Snapshot retention tags and tagged FRI history are separate mechanisms.

Distributed search also needs row-level fallback. The pinned Lance-Ray release and main plan fallback by uncovered fragment and force `fast_search=True` on segment tasks. Source inspection therefore indicates that newer overlay rows inside covered fragments can be omitted: core masks them out of ANN, and the adapter bypasses the core fallback that would re-score them. Released v0.5.0 also mixes incompatible L2/Dot distance conventions; pinned main fixes those formulas. Neither observation establishes runtime qualification. See [07](07-ecosystem-evidence.md#lance-ray-search-correctness-boundaries) for evidence and [09](09-sql-and-ray-integration.md#distributed-search-correctness) for the proposed contract.

MemWAL already contains writer fencing and generation concepts. Reuse their documented semantics when that ingestion path is selected; do not create a competing writer-epoch scheme inside Ray or represent ordinary Lance snapshot commits as MemWAL durability.

## Research limits

This is source inspection and dated public-source research, not a cluster benchmark or validation of a complete SQL/Ray/NPU stack. Lance implementation references match `67faafe65`; external connector pins and issue observations retain their own dates. Public issue reports identify investigation targets, not independently reproduced defects. The [validation plan](12-validation-and-experiments.md) turns those gaps into explicit experiments.
