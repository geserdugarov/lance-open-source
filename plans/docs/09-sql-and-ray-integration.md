# PostgreSQL-family and Ray integration design

Status: proposed implementation path, informed by existing public integrations. Date: 2026-11-13. **Working assumption:** PostgreSQL is the reference engine; openGauss or another PostgreSQL-family engine receives a separate compatibility assessment. SQL compatibility is not extension ABI compatibility.

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

### Type and expression contract

Start with a deliberately bounded mapping: signed integers, booleans, strings, fixed-precision supported decimals, timestamps with explicit timezone rules, and supported vector arrays. Define nulls, overflow, precision loss, ordering and collation behavior. Reject unsupported conversions descriptively. Nested/blob types can be added when an actual SQL representation is chosen.

Projection must include columns needed by residual predicates even when the SELECT list does not include them. Push down only expressions whose null, cast, comparison, timezone and collation semantics match. Initially leave volatile functions, unsupported collations and complex expressions in the SQL executor. SQL LIMIT pushdown is safe only after accounting for residual filtering and ordering; ANN top-k has its own explicit contract.

### Execution boundary choices

An in-process adapter avoids RPC but must reconcile PostgreSQL memory contexts, errors, interrupts and process lifecycle with Rust async tasks and Arrow lifetimes. A sidecar separates dependency upgrades and failures but adds serialization, authentication, deployment and cancellation propagation. Measure both if either is plausible; do not export Rust trait objects as a stable C ABI. For a same-process Arrow boundary, use explicit release callbacks. PostgreSQL tuple conversion may still allocate/copy.

A table access method is a much larger commitment than an FDW: it needs a plan for tuple identity, indexes, vacuum, WAL/recovery, locks and SQL transaction behavior. Defer it until the read-only route demonstrates a concrete limitation that warrants that work.

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

Train shared models when physical merge requires them. Direct commit already checks coexisting vector segments for matching metric, dimension, sub-index type and quantizer type. The coordinator checks tuning, model/job/attempt identity and intended coverage. Independent learned models are valid in separate segments, but physical merge requires interchangeable model contents. Shared centroids alone do not make SQ workers merge-compatible: bounds are trained per worker. Keep those outputs separate; [#10797](https://github.com/lance-format/lance/pull/10797) adds incompatible-SQ-model rejection upstream after this baseline, not shared-bounds worker input.

### Distributed search correctness

Plan ownership at one pinned snapshot. Assign each selected index segment to exactly one task and include uncovered fragments once according to the documented fallback policy. A fragment can be covered by a segment spanning many fragments; arbitrary fragment sharding must not cause repeated search of the same segment.

Each worker applies Lance's visibility, overlays, deletion masks and filter semantics. It returns distances plus stable identity sufficient to fetch/refine rows at the same snapshot. For an exact disjoint search with all filters already applied, local top-k is sufficient for global top-k. ANN candidate lists do not have that guarantee: oversampling and global sorting cannot recover candidates that were never returned. Measure recall against exact ground truth and evaluate exact reranking where required.

Use bounded candidate aggregation: with `W` workers, local candidate limit `K'`, and candidate row width `B`, result payload is approximately `W * K' * B`, before framing and buffers. The current public Lance-Ray returns a materialized top-k table; keep wide payload columns out of candidate exchange and fetch them after selection where supported. Do not imply it is a general streaming distributed SQL engine.

FTS needs a separate ranking design: combining local BM25 scores without consistent corpus statistics can change ranking. Hybrid search, global aggregation and distributed joins are distinct capabilities, not consequences of vector fanout.

### Budgets and operational behavior

Coordinate Ray resources with Lance/Tokio/Rayon threads. A Ray task requesting one CPU does not guarantee every native library uses only one thread. Budget decoded data, shuffle, model arrays, candidates, Python/Ray objects, device memory and spill independently. Limit in-flight tasks and outstanding batches; cancellation must stop new work and eventually release streams and device buffers.

Define deadlines, retryability, partial-result policy, admission limits and metrics. Query failure should default to an explicit failure, not silently return a partial top-k. If a product needs partial results, make incompleteness visible in a separate API contract.

## Engine-family portability checklist

For openGauss/GaussDB-family targets, verify the exact product/version and supported extension deployment model, FDW callback ABI, build toolchain, Arrow/pgrx viability, process and error model, type/collation semantics, parallel executor, snapshot mechanism and packaging policy. These questions do not imply an existing compatible port. The engine adapter should contain the resulting differences; keep them out of Lance's format/index crates.
