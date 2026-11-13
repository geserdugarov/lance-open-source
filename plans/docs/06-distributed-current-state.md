# Distributed Lance: current state and extension audit

Research date: **2026-11-13**. Status: evidence and assessment, not a feature announcement.

## Recommendation

Build a reusable distributed integration layer around Lance's existing storage protocols. Extend Lance-Ray and evaluate the existing `pglance` adapter before starting replacements. Keep SQL planning, cluster scheduling, device execution, and operational policy in separate owners; keep visibility, row identity, index coverage, and publication validation in Lance.

Lance already has distributed work units. The main opportunity is making those units easier to operate, recover, integrate, and upgrade. A managed distributed database also needs routing, resource control, availability, security, monitoring, and support; storage primitives alone do not provide these properties.

## What was inspected

| Evidence boundary | Exact baseline | Interpretation |
| --- | --- | --- |
| Implementation baseline | `042ccc8376f1ba03ab198a5cbfdd7f243853f954`, local `upstream/main`, 2026-11-05 | On branch `main-described`, `git rev-list --left-right --count upstream/main...HEAD` returned `0 5`; the five extra commits change documentation, agent skills and configuration, not implementation |
| Workspace package | `15.0.0-beta.10` | Development APIs must not be advertised as stable release capabilities |
| Latest public stable release checked | `v14.0.2`, 2026-10-29 | Stable release reference; this audit did not run its binaries |
| Public main observed during research | `e13a2ee341b0e14d715c8baac15cded1a8034b7f`, 2026-11-13 | Newer than the inspected checkout; not silently substituted for it |

External releases, repository pins, and current issue states are in [07-ecosystem-evidence.md](07-ecosystem-evidence.md). The earlier [vector architecture overview](00-overview.md) and [distributed index guide](05-distributed-vector-index-creation.md) remain useful source maps. Their source baseline is 2026-11-05; issue-status refreshes are dated separately.

Classification used throughout this plan:

- **Implemented:** found in the inspected source, with a named API or test. This does not mean production certification.
- **Published:** independently visible in a linked external release or repository; a published development branch is not a release guarantee.
- **Proposed:** design or acceptance criteria in this documentation; no implementation is claimed.
- **Unverified:** integration, performance, or service behavior for which evidence is insufficient.

## Capability inventory

All source links below resolve within this checkout. For a permanent upstream link, use `https://github.com/lance-format/lance/blob/042ccc8376f1ba03ab198a5cbfdd7f243853f954/` followed by the same repository path.

| Capability | Implemented surface and evidence | What an external system still owns |
| --- | --- | --- |
| Distributed ingestion | [Distributed write guide](../../docs/src/guide/distributed_write.md), [Python fragment writer](../../python/python/lance/fragment.py): `write_fragments`; coordinator uses `LanceDataset.commit` | Input partition assignment, selecting one successful attempt, replay protection, durable job state |
| Concurrent publication | [CommitHandler](../../rust/lance-table/src/io/commit.rs), conditional put / rename / external handlers; [transaction processing](../../rust/lance-table/src/transaction.rs) | Correct store capabilities, one authoritative publication path, commit-outcome reconciliation |
| External metadata authority | [ExternalManifestStore](../../rust/lance-table/src/io/commit/external_manifest.rs) and [namespace integration](../../rust/lance-namespace/src/lib.rs) | Adapter implementation, database/service availability, fencing and conformance; an external store is not automatically a PostgreSQL implementation |
| Distributed index creation | [DatasetIndexExt / IndexSegment](../../rust/lance/src/index/api.rs), [lifecycle](../../rust/lance/src/index/lifecycle.rs), [Python dataset](../../python/python/lance/dataset.py): uncommitted build, optional merge, commit | Snapshot pinning, target coverage, model identity, task/attempt selection, recovery |
| Distributed index optimization | [OptimizeIndicesPlan](../../rust/lance/src/index/optimize.rs): serialized plans, build tasks, merge tasks, commit | Transport and task scheduling; persisted recovery record; capability checks across workers |
| Distributed compaction | [CompactionPlan / CompactionPlanner](../../rust/lance/src/dataset/optimize/planning.rs), [CompactionTask / RewriteResult](../../rust/lance/src/dataset/optimize/execution.rs), [commit](../../rust/lance/src/dataset/optimize/commit.rs) | Task execution policy, transient storage budgets, task-version admission and cleanup coordination |
| Query subdivision | [Scanner source](../../rust/lance/src/dataset/scanner/source.rs), [vector planning](../../rust/lance/src/dataset/scanner/vector.rs), Python `index_segments` and fragment selection | Interprocess dispatch, global merge, cancellation, admission control, and service availability; Lance-Ray supplies a published distributed vector-search implementation |
| Streaming ingestion primitives | [MemWAL](../../rust/lance/src/dataset/mem_wal.rs), shard manifests, writer epochs and durable/non-durable modes | Writer placement, failover service, workload routing and durability configuration; this is not PostgreSQL WAL or proof of distributed SQL transactions |
| Object storage / I/O extension | [ObjectStoreProvider / registry](../../rust/lance-io/src/object_store/providers.rs), [wrappers and credentials](../../rust/lance-io/src/object_store.rs) | Provider integration, credential policy, cache topology and deployment |
| Scalar index plugins | [register_scalar_index_plugin](../../rust/lance-index/src/registry.rs), [ScalarIndexPlugin](../../rust/lance-index/src/scalar/registry.rs) | Static linking and registration before the first registry; same-version compilation; plugin ownership and test coverage |
| Custom vector index extension | No external vector extension point: [VectorIndexExtension](../../rust/lance/src/session/index_extension.rs) lives in a [pub(crate) module](../../rust/lance/src/session.rs) | External crates cannot implement this internal trait; it also loads through legacy `V1FileReader`. A current-format external seam requires an additive core change |
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

For segment creation, Lance checks structural and provenance properties, including overlapping coverage and field identity. Direct commit also rejects coexisting vector segments with different metrics, dimensions, sub-index types or quantizer types. The coordinator must verify tuning parameters, intended coverage and job/model/attempt identity. Direct commit supports separately trained physical segments; physical merge has stricter model requirements. The baseline's SQ creation merger lacks bounds equality validation; [#10797](https://github.com/lance-format/lance/pull/10797) fixed that upstream on 2026-11-13, without adding shared-bounds worker input. The detailed checks and creation-versus-optimization restrictions are in [05, sections 6–9](05-distributed-vector-index-creation.md#6-validation-and-commit-semantics).

Workers write artifacts before publication. Task retry can therefore leave objects even when the task reports failure. Select one successful result per task and use a fresh artifact UUID for each attempt. On an ambiguous commit result, resolve publication before retrying or deleting anything. Tags already protect source snapshots from cleanup. They do not protect uncommitted artifacts: default cleanup deletes unreferenced index files after a fixed seven days, independent of `older_than`; `delete_unverified=True` removes the grace period. Finish build plus recovery inside that window with the flag false, or suspend cleanup for the job. Supporting longer jobs alongside cleanup requires a core retention change. If a source version disappears, commit can succeed with its segment coverage cleared, so verify effective coverage after publication.

MemWAL already contains writer fencing and generation concepts. Reuse their documented semantics when that ingestion path is selected; do not create a competing writer-epoch scheme inside Ray or represent ordinary Lance snapshot commits as MemWAL durability.

## Research limits

This is source inspection and public-source research, not a cluster benchmark, security certification, or validation of a complete SQL/Ray/NPU stack. The reviewed source is eight days behind the observed public main. Public issue reports identify investigation targets, not independently reproduced defects. No private LanceDB implementation, internal Huawei feature list, or commercial agreement was available. The [validation plan](12-validation-and-experiments.md) turns those gaps into explicit experiments.
