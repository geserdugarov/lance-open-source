# Target architecture and extension contracts

Status: **proposal**, 2026-11-13. Existing seams are identified in [06](06-distributed-current-state.md); none of the conceptual interfaces below is a newly available Lance API.

## Design objective

A feature should have one semantic owner and a small number of integration points. Updating Lance should mostly require updating and testing an adapter, rather than carrying edits throughout scanners, index builders, manifests and language bindings.

The proposed composition is:

~~~text
Applications
|-- PostgreSQL-family adapter
|   `-- SQL planning, joins, SQL types, permissions, SQL transaction semantics
|-- Ray adapter
|   `-- task scheduling, resources, distributed data pipelines
`-- Managed service adapters (including possible LanceDB/Huawei integrations)
    `-- routing, tenancy, SLOs, deployment and operational policy

Shared operation boundary
|-- scan/search request + pinned snapshot + capability requirements
|-- existing build/optimization/compaction plans + execution envelope
`-- operation outcome + provenance + metrics + typed error

Lance semantic owners
|-- dataset/scanner: visibility, deletes, overlays, row identity, index fallback
|-- dataset/index lifecycle: plan, validate, publish, conflict handling
|-- lance-table: durable metadata and commit protocol
|-- lance-index / lance-linalg: algorithms and optional batch compute backend
`-- lance-file / lance-encoding / lance-io: format and physical I/O

External implementation packages
|-- SQL-engine ABI adapter
|-- Ray task runner + durable coordinator store
|-- storage / namespace / credential adapters
`-- optional Ascend backend and other device backends
~~~

This is a target ownership map, not a request to create all these packages immediately. Start with existing crates and repositories. Create a new crate only when it removes a demonstrated dependency or gives an independently maintained implementation a useful home.

## Authority: avoid two owners of the same truth

| Concern | Authority | Constraint |
| --- | --- | --- |
| Table name, location, access policy | Selected namespace/catalog | A resolved URI is not a sufficient tenant or table-incarnation identity |
| Committed Lance snapshot | Selected Lance commit handler / manifest authority | Readers and writers must agree on the handler; do not mix object-store and external authorities for the same table |
| Job state and selected task attempts | Durable coordinator record | It records publication intent and outcome; it must reconcile against Lance after a timeout |
| SQL transactions and SQL object lifecycle | SQL engine and its adapter | A Lance commit does not join PostgreSQL transactions automatically |
| Data/index visibility and row identity | Lance | SQL and Ray must not reconstruct deletion, overlay or remapping semantics |
| Resource placement | Ray or another scheduler | Placement does not implement numerical kernels or durable publication |
| Kernel precision and device execution | Compute backend under a declared operation contract | The backend may not silently change metric, dimensions or serialization |

Using PostgreSQL as a coordinator database or an `ExternalManifestStore` implementation is a separate decision from querying Lance through a PostgreSQL FDW. Neither provides cross-system atomic transactions by itself.

## Proposed bounded contracts

### 1. Snapshot and operation identity

An execution envelope carries an operation ID, protocol revision, a concrete table identity, branch, source manifest version/identity, schema fingerprint, target fields and operation parameters. A version is scoped to its table and incarnation. Prefix reuse and table recreation must not accidentally validate old work.

Protect source snapshot lifetime with an existing Lance tag; cleanup honors tags. Uncommitted output has no equivalent protection: default cleanup can delete unreferenced index files after a fixed seven days, independent of `older_than`, and `delete_unverified=True` removes even that grace period. Initially require build plus recovery to finish within seven days of the oldest output with the flag false, or suspend cleanup for the job. A catalog/job reservation is not honored automatically. Longer jobs with concurrent cleanup require a core retention change, such as an artifact lease integrated into cleanup; this is new behavior to design and test, not merely a test gate.

### 2. Plan, execute, validate, publish

Reuse `OptimizeIndicesPlan`, `CompactionPlan`, fragment metadata and segment metadata. The external envelope adds scheduler-neutral identity, capability requirements and resource budgets without duplicating Lance's algorithm plan.

The coordinator persists the plan before dispatch. A worker writes an immutable, uniquely identified result and returns a small descriptor. A barrier selects one attempt per task, checks intended coverage, and validates parameters/model hashes. Lance performs its own semantic and publication checks. The coordinator then records the authoritative committed version.

~~~text
PLANNED -> RUNNING -> VALIDATED -> COMMITTING -> COMMITTED
               |          |            |
               |          |            `-> RECONCILING -> COMMITTED / RETRY / REPLAN
               |          `-> REPLAN when source requirements no longer hold
               `-> RETRY with a fresh attempt and artifact identity

CANCEL_REQUESTED -> QUIESCING -> CANCELLED -> retention-protected cleanup
~~~

Cancellation after a commit has landed cannot roll back that commit. Report the actual outcome. A stale coordinator must lose the right to select attempts or publish: fencing must be enforced at the coordinator store and publication boundary, not merely recorded in JSON. If an existing handler cannot enforce the chosen protocol, use a single authorized publisher until a supported implementation is available.

### 3. Scan/search request and result

Use a small, typed request containing the snapshot, projection, supported predicate representation, fragment/segment ownership, search parameters, result shape, deadline and resource budget. Unsupported predicates remain in the SQL engine. Do not serialize PostgreSQL planner pointers or arbitrary DataFusion physical plans as the cross-version public protocol.

Keep Arrow batches as the internal stream shape. For an in-process ABI boundary, evaluate Arrow C Data/Stream with explicit ownership and cancellation. For interprocess transport, use a versioned message protocol with Arrow IPC; evaluate Flight only if streaming RPC is actually needed. The [Arrow C Data specification](https://arrow.apache.org/docs/format/CDataInterface.html) applies to same-process interchange and is not a network protocol.

A logical scan boundary does not imply a new SQL engine. DataFusion remains an implementation detail where useful. Extracting its component types from existing interfaces is incremental work, justified by a concrete adapter requirement.

### 4. Compute backend

Begin with batch operations that have clear inputs and outputs: IVF assignment, training primitives, PQ encoding or exact reranking. Specify dtype, metric, normalization, null/NaN policy, shapes, precision, memory budget, cancellation and completion semantics. Kernel implementations must not own manifests, namespace lookups, SQL sessions or Ray actors.

A possible capability description is `operations`, `dtypes`, `metrics`, `max_batch_bytes`, and an implementation revision. This is design notation, not a proposed universal plugin framework. Prefer two working implementations and tests before stabilizing a trait.

### 5. Storage, metadata and cache providers

Use `ObjectStoreProvider`, store wrappers, credential accessors, namespaces and `CommitHandler` as the initial seams. Verify wrapper behavior across all optimized paths, including specialized listing. Bind cache entries to table/snapshot/object identity and applicable credential scope; a path alone can alias recreated tables or different endpoints. Keep distributed cache topology external to `lance-file`.

## Compatibility and capability negotiation

A successfully deserialized plan is not necessarily correctly executable. The current compaction `TaskData` comments explicitly warn that older workers can ignore output-clustering/metadata fields. Require capability admission before sending such tasks. See [planning.rs](../../rust/lance/src/dataset/optimize/planning.rs).

Proposed rules:

1. Coordinator records protocol revision, producer version, minimum required capabilities and the selected worker implementation versions.
2. Workers reject unknown required features and incompatible major protocol versions. Optional additive metadata may be ignored only where the contract explicitly permits it.
3. Results repeat plan/task/attempt/source identities and actual backend implementation. The barrier verifies them before publication.
4. A job initially uses one tested Lance/plugin/runtime combination. Mixed-version execution is enabled only for combinations admitted by conformance tests.
5. New persistent formats follow stable Lance format contracts. Execution envelopes are separately versioned; prototyping them does not require changes to stable data files.

## Decision record

| Decision | Proposed default | Alternative / decision trigger |
| --- | --- | --- |
| SQL integration | Evolve a read-only FDW based on `pglance` after a dependency/ABI spike | Table access method only if required semantics/performance justify WAL, vacuum and transaction integration |
| SQL isolation boundary | Compare in-process Rust adapter with a small scan sidecar during phase 0 | Sidecar wins only if isolation/upgrade gains outweigh transport and operation cost |
| Distributed execution | Extend Lance-Ray with durable job state and existing plans | Another scheduler consumes the same protocol when a real consumer exists |
| Device acceleration | Optional Ascend implementation, CPU reference path | Add kernel families after profiling; no mandatory vendor runtime in default Lance |
| Index extension | Existing scalar registry for statically linked scalar plugins; no external vector extension point exists | `VectorIndexExtension` is in a `pub(crate)` module and uses `V1FileReader`; propose an additive current-format seam only for a concrete consumer |
| Publication | Existing correct handler and one authoritative publisher | External metadata implementation requires crash/concurrency conformance before use |
| Private feature preservation | Out-of-tree provider/backend/policy modules | A small temporary patch may be tracked with an upstream issue and removal criterion; no promise of zero maintenance |

## Review rubric for every future feature

A design review must name: the invariant owner; the existing extension point; new dependencies; the tested fallback/error behavior; the minimal public surface; format effects; supported capability combinations; observable failure outcomes; and how an external implementation will be upgraded. Reject abstractions that merely rename existing calls, vendor conditionals scattered through core algorithms, or binding-only fixes for storage invariants.

Measure extensibility rather than counting interfaces: number of core files touched by an adapter, number of private patches, upgrade engineering time, dependency leakage, and the percentage of adapters tested without rebuilding unrelated surfaces. Targets and experiments are in [12](12-validation-and-experiments.md).
