# PostgreSQL-family and Ray integration design

Status: proposed implementation path, informed by existing public integrations. Date: 2026-11-13. **Working assumption:** PostgreSQL is the reference engine; openGauss or another PostgreSQL-family engine receives a separate compatibility assessment. SQL compatibility is not extension ABI compatibility.

Lance API references are checked against `ded121cd9a99dc83040cf41049dc2bd3963241b9` (2026-11-24). External
connector claims retain the exact revisions in [07](07-ecosystem-evidence.md).
Reconciled on 2026-11-24 with the upstream crate-architecture proposal
([07](07-ecosystem-evidence.md#upstream-crate-architecture-proposal)), which
is not implemented at this baseline.

## Reuse the available integrations

[Public ecosystem evidence](07-ecosystem-evidence.md) records a real `lance-format/pglance` FDW and Lance-Ray with distributed vector search. Start with those projects and their maintainers. The inspected `pglance` source uses an older Lance dependency and a minimal scan-oriented callback surface; it is a starting point, not proof that current Lance, pushdown, parallel execution or transactions work. Rebase/dependency assessment and a small reproducible scan experiment precede a production design.

A reference deployment can compose independent services:

~~~text
SQL client -> PostgreSQL -> FDW / scan adapter -> Lance scan workers
                                              |           |
                                              |           `-> object storage
                                              `-> bounded streaming results

Pipeline client -> Lance-Ray -> Ray CPU / NPU workers -> Lance artifacts
                         |
                         `-> durable coordinator record -> one manifest publisher

Namespace/catalog -> table identity + location + scoped access
~~~

Ray is well suited to ingestion, embedding generation, index construction and maintenance. Its distributed vector-search path is also a candidate for query execution. Benchmark query startup, pool lifetime, cancellation and concurrent load before placing it on a low-latency SQL request path. A resident scan service is an alternative, not a required second architecture.

## SQL milestones and semantics

| Stage | Deliverable | Exit condition |
| --- | --- | --- |
| S0: engine/version spike | Choose one PostgreSQL major and pin `pglance`, Lance, Arrow and pgrx; determine process boundary | A tiny scan and join pass with a documented build/runtime matrix |
| S1: snapshot reads | Read-only foreign table, explicit snapshot binding, projection, type mapping, streaming and cancellation | Repeated scans in the documented statement/transaction scope use the same snapshot; zero duplicate/missing rows across fragments |
| S2: safe pushdown | Predicate subset, statistics/cost model, EXPLAIN output and optional parallel scan | Results match local SQL evaluation for each supported expression; unsupported expressions remain local |
| S3: search operator | Explicit vector-search function/operator contract with filter semantics and recall measurements | SQL users can distinguish exact from approximate results and identify the pinned snapshot |
| S4: asynchronous writes / CDC | Separate ingestion API or transactional source outbox with replay-safe application | Source commits are replayed without duplicate application; lag and outcome are observable |
| S5: stronger transactions | Separate design for coordinated commits, crash recovery and SQL visibility | Only pursue if required by a measured use case; no transactional claim before the complete protocol is tested |

The PostgreSQL FDW interface exposes scan, planning and optional parallel/async callbacks. These are host integration hooks; registering an FDW does not make it a distributed executor. Predicate shipping and cost estimates belong to the adapter. See [FDW planning](https://www.postgresql.org/docs/current/fdw-planning.html) and [callback reference](https://www.postgresql.org/docs/current/fdw-callbacks.html).

### Snapshot binding

For a first milestone, resolve and pin a Lance version at statement start and reuse it for every scan task, rescan and join reference to that foreign table. Describe this as **statement-scoped external snapshot consistency**. PostgreSQL `REPEATABLE READ` must not be advertised as covering an external table until the adapter retains a per-transaction snapshot binding, protects retention and handles rescans and prepared statements accordingly.

Two different Lance tables can be independently pinned yet lack a common transaction snapshot. Multi-table atomicity and point-in-time consistency need an additional catalog/transaction protocol. Do not infer them from version numbers.

The Rust Transaction V2 opt-in can atomically apply several actions to one
Lance manifest. PostgreSQL transaction state and an external CDC offset
still have their own publication and recovery protocols. [Commit API](../../rust/lance/src/dataset/write/commit.rs)

### Type and expression contract

Start with a deliberately bounded mapping: signed integers, booleans, strings, fixed-precision supported decimals, timestamps with explicit timezone rules, and supported vector arrays. Define nulls, overflow, precision loss, ordering and collation behavior. Reject unsupported conversions descriptively. Nested/blob types can be added when an actual SQL representation is chosen.

Projection must include columns needed by residual predicates even when the SELECT list does not include them. Push down only expressions whose null, cast, comparison, timezone and collation semantics match. Initially leave volatile functions, unsupported collations and complex expressions in the SQL executor. SQL LIMIT pushdown is safe only after accounting for residual filtering and ordering; ANN top-k has its own explicit contract.

Upstream has not yet chosen the engine-neutral predicate vocabulary that its index API and operations layer will share. Keep the supported subset in an adapter-local form that maps one-to-one onto Lance filters, so the target can change from DataFusion expressions to Lance-owned predicates without revisiting semantics. The proposed per-index `Option<Cost>` would be a natural input to FDW cost estimates, but it does not exist; S2 costing starts from fragment and index statistics.

### Execution boundary choices

An in-process adapter avoids RPC but must reconcile PostgreSQL memory contexts, errors, interrupts and process lifecycle with Rust async tasks and Arrow lifetimes. A sidecar separates dependency upgrades and failures but adds serialization, authentication, deployment and cancellation propagation. Measure both if either is plausible; do not export Rust trait objects as a stable C ABI. For a same-process Arrow boundary, use explicit release callbacks. PostgreSQL tuple conversion may still allocate/copy.

The upstream proposal takes the same position on ABI: its new traits stay Rust traits, and seams are only shaped so a C interface could be added later (Arrow arrays and streams, serializable options, coarse calls). It also expects connectors to be able to depend on a core table and index API without DataFusion or the scanner, which would shrink an in-process adapter's dependency closure. Whether connectors should depend on those crates directly is a deferred upstream decision. Record the `lance` dependency closure in the S0 matrix, and keep the adapter on the public `lance` API until that is settled.

A table access method is a much larger commitment than an FDW: it needs a plan for tuple identity, indices, vacuum, WAL/recovery, locks and SQL transaction behavior. Defer it until the read-only route demonstrates a concrete limitation that warrants that work.

### CDC proposal

If PostgreSQL is the transactional source, capture an outbox entry in the source transaction or consume a selected logical-decoding stream. Preserve source transaction/position and ordering. Batch application to Lance needs a durable deduplication/outcome protocol, especially after a successful commit with a lost acknowledgement. A bare at-least-once retry of append can duplicate rows. An outbox is not a distributed transaction between PostgreSQL and Lance; expose replication lag and asynchronous visibility. Select and validate the concrete CDC mechanism in a later milestone.

## Ray workload lifecycle

~~~text
Driver: resolve snapshot -> persist plan -> schedule task descriptors
Workers: open pinned snapshot -> read batches -> write unique outputs
Driver: select attempts -> validate coverage/model/source -> publish
Recovery driver: load durable record -> reconcile publication -> resume safely
~~~

Use a durable store outside ephemeral actor memory for job records. Ray task retry/reconstruction does not by itself make object-store side effects exactly once. A coordinator actor may help serialize execution, but actor restart must reconstruct its state and invalidate stale publishers.

Task messages carry small descriptors; data is streamed from shared storage or exchanged as bounded Arrow batches. Do not collect the whole table or all training batches on the driver. For large task counts, persist result descriptors incrementally and account for the final metadata/manifest size. Keep worker environments pinned and broadcast model artifacts by immutable location and digest.

Train shared models when physical merge requires them. Direct commit already checks coexisting vector segments for matching metric, dimension, sub-index type and quantizer type. The coordinator checks tuning, model/job/attempt identity and intended coverage. Independent learned models are valid in separate segments, but physical merge requires interchangeable model contents. Shared centroids alone do not make SQ workers merge-compatible: bounds are trained per worker. Keep differing models separate; [#10797](https://github.com/lance-format/lance/pull/10797) is included here and rejects incompatible SQ models without supplying shared-bounds worker input.

For distributed optimization, persist and pass the planning options to commit.
The bindings expose bounded consolidation, whose complete-coverage validation
is stronger than ordinary optimization. Rust's separate UUID-selected
consolidation can retrain and is not exposed by the Python/Java converters at
this baseline. See [05 §9](05-distributed-vector-index-creation.md#9-incremental-creation-and-distributed-optimization).

Fragment-level primitives with driver-side commits are the shape the upstream
proposal generalizes into a core table plus serializable tasks. Two points
from that work affect task ownership here. Resolve rows to fragments at the
pinned snapshot, never from row-ID bit layout, which breaks under stable row
IDs; `LanceDataset.get_row_addresses` does this at the current baseline but is
a development API newer than `v14.0.3` and than Lance-Ray's declared `pylance`
floor. And #8973 sketches a shard-level work unit that need not equal one
fragment, so treat fragment-ID task ownership as the current contract, not a
permanent one.

### Distributed search correctness

The equivalence target is **the same result as single-node Lance at that
snapshot**, with the same index selection, search/refinement options, filters
and tie policy. This is a proposed acceptance target, not a claim about the
current connector. Compare identities and distances to that reference as well
as measuring ANN recall against exact ground truth.

Assign each selected segment to exactly one task. Define fallback as
**uncovered fragments plus stale rows inside covered fragments**. Proposed
ownership: segment tasks return ANN candidates, while dedicated fallback tasks
consume disjoint, core-derived fragment/row selections and search each fallback
row once. Stale rows must have a fallback-task owner even when their fragment
has a segment owner. Core must expose or execute this selection; the adapter
must not reconstruct overlay/FRI semantics. Until that boundary exists, route
affected queries through the complete core scanner. Arbitrary fragment sharding
must not repeatedly search a segment that spans several fragments.

The upstream task layer, with index-segment scans and partial index searches
as serializable tasks, is the proposed home for that boundary, and the
proposal keeps coverage and fallback logic in Lance. Its ownership list names
unindexed-fragment fallback but not stale rows inside covered fragments. Raise
that case, with the E1 regression, on the task-layer project so the task
definition covers it.

The inspected Lance-Ray release and main force `fast_search=True` for segment
tasks and plan fallback per fragment. Core then masks stale overlay rows out
of ANN but skips `knn_combined`, where those rows would be re-scored. Source
inspection therefore predicts silent omission of overlay-updated rows within
covered fragments; this remains to be reproduced in a runtime regression test.
The released v0.5.0 fallback also uses non-squared L2 and `-dot`, while Lance
uses squared L2 and `1 - dot`. Pin quality baselines to `f155c32` or a qualified
descendant with those formula fixes. See [the pinned evidence](07-ecosystem-evidence.md#lance-ray-search-correctness-boundaries).

Each worker must apply Lance's visibility, deletions and filter semantics, and
return stable identity sufficient to fetch/refine at the pinned snapshot. Core
normally takes raw ANN vectors and re-scores the ANN/fallback union exactly;
Lance-Ray merges approximate and exact distances as returned. The proposed
global merge must reproduce core's rescore rule, or its explicit code-only RQ
rule when requested ([03 §6](03-index-on-disk-and-search.md#6-exact-and-quantized-refinement)).
For exact disjoint search with filters applied, local top-k suffices for global
top-k. For ANN, candidate truncation and refinement order must also match the
single-node plan: global sorting cannot recover candidates discarded locally.
Oversampling alone does not establish equivalence.

Admit cross-column refinement and multivector search as separate request
capabilities. Cross-column refinement obtains coarse top-M candidates before
scoring another Float32 vector space; distance bounds apply to the final metric.
WARP aggregates query-token contributions and imputes missing similarities,
which is not an ordinary per-segment document top-k merge. Until the adapter
implements and tests those rules, use the complete core scanner for those
requests. Adaptive shared batch search also has narrower eligibility than
fixed-probe shared search; follow the core planner's checks.

For IVF_HNSW_RQ, send the same `rq_graph_scoring` to each segment task.
The default full-code walk and the optional one-bit walk can visit different
nodes. A driver rerank cannot recover a candidate the worker never visited.
Check the worker's support before dispatch. [ANN execution schema](../../protos/ann.proto)

Use bounded candidate aggregation: with `W` workers, local candidate limit `K'`, and candidate row width `B`, result payload is approximately `W * K' * B`, before framing and buffers. The current public Lance-Ray returns a materialized top-k table; keep wide payload columns out of candidate exchange and fetch them after selection where supported. Do not imply it is a general streaming distributed SQL engine.

FTS needs a separate ranking design: combining local BM25 scores without consistent corpus statistics can change ranking. The proposed `as_ranked()` capability covers BM25 as well as ANN, but a per-segment ranked stream does not by itself supply those statistics. Hybrid search, global aggregation and distributed joins are distinct capabilities, not consequences of vector fanout.

### Budgets and operational behavior

Coordinate Ray resources with Lance/Tokio/Rayon threads. A Ray task requesting one CPU does not guarantee every native library uses only one thread. Budget decoded data, shuffle, model arrays, candidates, Python/Ray objects, device memory and spill independently. Limit in-flight tasks and outstanding batches; cancellation must stop new work and eventually release streams and device buffers.

Use core build-memory events and execution reservation peaks alongside worker
RSS and device measurements. Runtime session cache resizing controls cache
budgets, not total worker memory; neither telemetry nor a Ray CPU request
enforces a global Lance allocation limit.

For compaction tasks that re-encode data, Python's `data_cache_bytes` and
Java's `withDataCacheBytes` set the column-buffer budget per output writer.
Account separately for input scans and concurrent writers on the same worker.

Define deadlines, retryability, partial-result policy, admission limits and metrics. Query failure should default to an explicit failure, not silently return a partial top-k. If a product needs partial results, make incompleteness visible in a separate API contract.

## Engine-family portability checklist

For openGauss/GaussDB-family targets, verify the exact product/version and supported extension deployment model, FDW callback ABI, build toolchain, Arrow/pgrx viability, process and error model, type/collation semantics, parallel executor, snapshot mechanism and packaging policy. These questions do not imply an existing compatible port. The engine adapter should contain the resulting differences; keep them out of Lance's format/index crates.
