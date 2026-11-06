# Ecosystem: Make Four Engines Work as One

**Research date:** 6 November 2026. **Scope:** Huawei Cloud AI DataLake, Aura, Ray, Apache Flink, Apache Spark, and their shared lake integration. **Review artifacts:** [one-slide PowerPoint](aidatalake-four-engines-ecosystem.pptx) · [HTML review](aidatalake-four-engines-ecosystem.html) · [rendered slide](aidatalake-four-engines-ecosystem.png).

This report extends the architectural conclusions in commits `d4d46d3f6` and `e3e8527dc`. It distinguishes **documented Huawei service capabilities**, **released upstream capabilities**, and **proposed integration contracts**. Documentation establishes available building blocks; it does not certify the deployed binaries, interoperability, or performance of a particular cluster.

## 1. Message for the slide

**AI DataLake combines four complementary engines over a governed lake: Aura makes multimodal data usable, Ray scales AI computation on CPUs/GPUs/NPUs, Flink keeps changing data current, and Spark prepares and analyzes historical data.** Their shared value comes from reusable tables, consistent identities and permissions, and reliable handoffs between jobs.

The requested division of responsibility is a useful default:

- **Aura:** process images, video, audio, and text; extract features and generate embeddings through model/UDF execution; save the resulting vectors, metadata, and media references in Lance; search and curate the resulting datasets.
- **Ray:** distribute Python and AI processing across heterogeneous compute, including GPUs and Ascend NPUs; run inference, enrichment, embedding backfills, and training-data preparation.
- **Flink:** continuously process structured and semi-structured events and change streams; maintain state, join/enrich events, and publish fresh results.
- **Spark:** run large batch SQL/ETL, historical joins, aggregations, reconciliation, and feature preparation over structured and semi-structured data.

These roles agree with Huawei's engine descriptions. They are workload emphases: Aura also manages accelerator resources, while Spark and Flink can participate in multimodal pipelines. [Aura](https://support.huaweicloud.com/productdesc-aidatalake/aidatalake_08_0007.html), [Ray](https://support.huaweicloud.com/productdesc-aidatalake/aidatalake_08_0008.html), [Flink](https://support.huaweicloud.com/productdesc-aidatalake/aidatalake_08_0010.html), [Spark](https://support.huaweicloud.com/productdesc-aidatalake/aidatalake_08_0009.html).

**“Work as one” is a platform contract, not a claim that all four engines share one optimizer, scheduler, API, or transaction.** Jobs can use different engines without exporting an entire dataset between each stage. Some outputs, caches, indices, and checkpoints still require physical writes.

## 2. What the two commits contribute

| Inspected commit | Relevant conclusion | Consequence for this ecosystem slide |
|---|---|---|
| `d4d46d3f6` — [100B vector-platform comparison](100b-vector-ecosystem-comparison.md) | Lance plus a distributed engine is the preferred lake/mining architecture in that assessment. The caller owns scheduling, distributed execution, and service operations. The report's workload is 20B → 100B individual 1,024-dimensional vectors, 5B additions/day, and large top-k retrieval. | Explain which engine does which work above the shared lake. Preserve the distinction between storage/index primitives and a complete distributed service. |
| `e3e8527dc` — [Lance 10 → 14 upgrade](aidatalake-lance-10-to-14-upgrade.md) | Recommend evaluating Lance 14.0.2 with explicit stable file format 2.2, distributed maintenance, selective reads, and cross-engine qualification. GPU/NPU scheduling and Lance kernel acceleration are separate concerns. | Put connector/runtime compatibility, snapshot handoffs, and coordinated maintenance into the integration agenda. Keep algorithm and release detail in the companion report. |

The new slide complements the existing platform comparison and upgrade slide. It should sit near `part2.pptx`'s “Lance in the Agentic Data Platform” and before detailed engine-specific optimization plans.

**A correction from rechecking primary sources:** the current Huawei **26.9.0 bulletin explicitly lists Paimon for Spark and Flink**, including Paimon 1.2.0 in Spark's ecosystem. The earlier upgrade report says the bulletin did not establish Paimon details. Use the currently documented engine-specific support below; this research leaves the earlier files unchanged. [26.9.0 bulletin](https://support.huaweicloud.com/verspt-bulletin-aidatalake/aidatalake_bulletin_0003.html).

## 3. Engine comparison

This table describes recommended responsibility in this architecture, not an exclusive capability boundary or a performance ranking.

| Dimension | Aura | Ray | Flink | Spark |
|---|---|---|---|---|
| Primary role | Multimodal transformation, retrieval, and curation | Distributed AI/Python execution and accelerator utilization | Continuous event/change processing | Large batch processing and analytical SQL |
| Typical inputs | Images, clips, audio, text, embeddings, associated metadata | Media, tensors, embedding columns, selected training/evaluation records | Events, logs, CDC records, changing business dimensions | Historical tables, relational exports, large structured/semi-structured datasets |
| Development interface | SQL, DataFrame API, Python UDFs | Ray tasks/actors and Ray Data | SQL, Table API, DataStream, PyFlink | SQL, DataFrames, PySpark, Scala/Java |
| Execution emphasis | Multimodal operators, pipeline execution, query optimization | Parallel tasks, reusable model actors, batched inference | Event time, keyed state, windows, streaming joins, checkpoints | Parallel scans, joins, shuffle, aggregation, scheduled recomputation |
| Example | Decode clips, create embeddings, write Lance, retrieve a scenario cohort | Re-embed selected records on GPUs/NPUs; prepare training batches | Deduplicate device events and enrich them with live metadata | Join historical facts and dimensions; produce quality features and cohorts |
| Shared-lake output | Embeddings, text/features, metadata and media references in Lance | Versioned features or enrichment results in Lance; derived datasets | Fresh structured tables in Iceberg/Paimon; qualified Lance publication when needed | Curated Iceberg/Paimon tables; Lance access through a qualified connector |
| Important overlap | Can also schedule CPU/GPU/NPU work and process structured data | Can also create embeddings, run search tasks, and execute index jobs | Can carry media references and invoke model services | Can read binary media and run model/UDF processing |
| Key integration task | Preserve lazy media access, push filters down, bound distributed search/merge | Feed accelerators efficiently; pin models and inputs; make retries safe | Connect source progress to durable publication; handle updates/deletes | Certify schema mappings, snapshots, pushdown, and maintenance concurrency |

The interfaces and broad roles are documented by the four engine guides above. Apache Spark explicitly provides a binary-file source; Apache Flink provides asynchronous external I/O. These support a broader capability envelope than “non-multimodal only.” A custom Flink model call is not evidence of a built-in accelerator inference runtime. [Spark 4.0.2 binary source](https://spark.apache.org/docs/4.0.2/sql-data-sources-binaryFile.html), [Flink 1.20 async I/O](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/asyncio/).

## 4. The shared architecture

The diagram below is a **proposed responsibility model**, assembled from documented components. It does not imply equal support for every engine/format combination.

```text
AI DataLake workspace and workflow coordination
|
+-- Shared control
|   +-- LakeFormation: table identity, metadata, permissions
|   +-- Job dependencies, model/UDF versions, snapshot handoffs
|   +-- Resource quotas, monitoring, lineage, maintenance ownership
|
+-- Compute endpoints
|   +-- Aura: multimodal SQL/DataFrame processing and retrieval
|   +-- Ray: CPU/GPU/NPU AI processing and feature backfill
|   +-- Flink: continuous events, changes, and stateful enrichment
|   +-- Spark: historical SQL/ETL, joins, and batch preparation
|
+-- Shared data on OBS
    +-- Lance: embeddings, features, text, media references, versions
    +-- Iceberg / Paimon: structured tables and business changes
    +-- Media objects, model artifacts, checkpoints, temporary outputs
```

Huawei documents workspaces as an isolation boundary and LakeFormation as the associated metadata/permission service; the underlying data resides on OBS. Several workspaces can bind to one LakeFormation instance. This supports shared governance, but every engine still needs a functioning catalog and authorization integration. [Workspace and LakeFormation relationship](https://support.huaweicloud.com/productdesc-aidatalake/aidatalake_08_0005.html).

### Four meanings of “one”

| Shared contract | What it should provide | What must be demonstrated |
|---|---|---|
| One discoverable data estate | Stable catalog/table identity and a consistent schema description | Aura, Ray, Flink, and Spark resolve the intended table and supported snapshot; catalog names alone are insufficient |
| One reusable dataset lineage | Durable record IDs and explicit source/output versions | A cohort selected in Aura is exactly the cohort enriched in Ray, even after concurrent ingestion |
| One governance model | The same intended permissions and audit identity | Connector and direct-object access enforce the relevant policy; temporary credentials refresh correctly |
| One operational workflow | Dependencies, retries, resource budgets, freshness tracking | A failed stage resumes without publishing duplicate/inconsistent output or deleting a downstream reader's data |

Lance contributes versioned table/index operations and distributed build primitives. Its tagged documentation explicitly assigns worker launch and orchestration to the caller. The workflow layer should therefore designate an owner for each commit and maintenance operation. [Lance 14.0.2 distributed indexing](https://github.com/lance-format/lance/blob/v14.0.2/docs/src/guide/distributed_indexing.md).

## 5. Aura: turn multimodal content into reusable data

Huawei describes Aura's column-oriented abstraction for media, SQL/DataFrame interfaces, Python UDF extensibility, heterogeneous scheduling, and pipeline optimizations. These are the basis for combining decoding, feature extraction, embedding generation, and relational filtering. Its SQL guide identifies Lance as a storage option for AI and embedding workloads. [Aura engine](https://support.huaweicloud.com/productdesc-aidatalake/aidatalake_08_0007.html), [Aura SQL development flow](https://support.huaweicloud.com/sqlref-aura-aidatalake/aidatalake_061_0270.html).

**Recommended workflow:** read a manifest of media references, decode only the required content, apply the chosen model/UDF, validate outputs, and publish embeddings plus associated metadata to Lance. Later, select candidate IDs with metadata/text/vector predicates and fetch media for the selected rows. This is an architectural recipe; it is not a claim that every model is a built-in Aura function.

For example, a driving-data workflow might produce one record per clip or extracted frame, recording the parent asset, frame/time range, embedding model, processing version, and quality flags. Aura can then select a scenario cohort and combine it with structured vehicle/weather metadata. Which representation is appropriate depends on whether downstream search is over clips, frames, or patches.

Huawei's Lance guide describes storing vectors and structured fields together, lazy blob access, versioned data, and adding feature columns without rewriting the whole dataset. These capabilities support iterative enrichment, subject to the selected SDK and connector. [Huawei Lance guide](https://support.huaweicloud.com/devg-aidatalake/aidatalake_05_0034.html).

Recommended implementation priorities:

1. Carry references and projected columns through expensive SQL operators; fetch media after selection when possible.
2. Record model identity, preprocessing version, dimensions, metric, and normalization alongside embeddings. Equal dimensions do not imply a compatible embedding space.
3. Reject dimension/type mismatches and explicitly handle missing media, failed inference, and null vectors.
4. Pin input snapshots for long jobs; publish output only after validation.
5. Bound candidate exchange and global top-k memory. Keep retrieval and index-maintenance resource budgets visible.

The [local Aura indexing design](aura-distributed-indexing.md) provides additional CN/DN integration detail. It remains a design reference, not independent evidence that all described behavior has shipped.

## 6. Ray: keep AI computation fed and accelerators busy

Ray is the natural choice when the main work is distributed Python, model inference, or custom AI processing. Ray Data's inference guide demonstrates loading models once in actors and processing batches, with CPU preprocessing and accelerator inference arranged as separate stages. Batch size, model memory, data loading, and concurrency all affect utilization. [Ray batch inference](https://docs.ray.io/en/latest/data/batch-inference.html).

**Recommended workflow:** read the pinned selection or dataset, prepare inputs on CPUs, execute model batches on the chosen accelerator, then write validated feature results back to Lance. Use stable record IDs to reconcile results. Avoid moving decoded media or full embedding tables through the coordinator.

### GPU and NPU support: precise boundaries

| Layer | Verified foundation | Required qualification |
|---|---|---|
| Ray resource scheduling | Ray 2.54.0 recognizes `GPU` and Ascend `NPU` resources; the tagged guide shows NPU tasks/actors and `ASCEND_RT_VISIBLE_DEVICES` | Validate the actual device image, model runtime, driver/CANN combination, memory behavior, and multi-device execution |
| Upstream support status | The 2.54.0 guide labels NVIDIA GPU support as Ray-team supported and Huawei Ascend as experimental/community supported | Evaluate Huawei's managed runtime separately; the upstream label does not describe a contracted Huawei service guarantee |
| Model computation | A scheduled task can run a compatible accelerator model/backend | Scheduling a task does not port its Python libraries or kernels to the device |
| Lance ANN/build kernels | A Ray job can call Lance and organize distributed work | GPU/NPU inference does not automatically accelerate Lance search, assignment, quantization, or index writing |

Source for the scheduling and support-level statements: [Ray 2.54.0 accelerator guide](https://github.com/ray-project/ray/blob/ray-2.54.0/doc/source/ray-core/scheduling/accelerators.rst). The separate Lance/Ascend work is discussed in the [local NPU research](lance-clustering-npu-ascend-research.md) and the earlier upgrade report.

The released `lance-ray` connector adds practical building blocks: v0.5.0 includes distributed column addition, segment-based vector search, and distributed IVF_RQ construction. This makes it useful for backfills and index workflows as well as ordinary reads/writes. Reuse these paths before designing an alternative orchestration implementation. [v0.5.0 release](https://github.com/lance-format/lance-ray/releases/tag/v0.5.0), [data evolution](https://lance.org/integrations/ray/data-evolution/).

Aura and Ray overlap deliberately. Choose Aura when SQL/DataFrame composition and retrieval dominate; choose Ray when custom Python/model execution dominates. A workflow can use both, but the reviewed evidence does not establish an automatic Aura-to-Ray dispatch mechanism or a single shared execution plan. A versioned table/cohort handoff is sufficient.

## 7. Flink and Spark: the structured-data foundation

### Flink: continuous correctness and freshness

Use Flink for incoming events and change streams that require state, time-aware correlation, deduplication, enrichment, or continuously updated aggregates. Its checkpointing coordinates operator state and source progress. End-to-end exactly-once output additionally requires replayable sources and transactional or idempotent sinks. [Flink 1.20 fault tolerance](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/learn-flink/fault_tolerance/).

**Recommended base path:** business events/change records → Flink validation and enrichment → Iceberg/Paimon structured tables → subsequent Aura/Ray jobs where embeddings are needed. The 26.9.0 service bulletin lists both table integrations for Flink. Direct streaming into Lance is a useful additional path after its connector semantics are qualified. [Huawei release matrix](https://support.huaweicloud.com/verspt-bulletin-aidatalake/aidatalake_bulletin_0003.html).

For multimodal events, keep an asset reference and stable key in the event stream; use a bounded external inference call or a separate enrichment stage when appropriate. If inference is slow, explicitly choose how late results, retries, out-of-order updates, and failed records affect publication.

### What the released Lance sink actually guarantees

The latest upstream `lance-flink` release observed is v0.1.0. Its SQL sink's `getChangelogMode` returns **INSERT only**. Its sink implementation writes and commits during `flush()`, calls `flush()` from `snapshotState`, and does not restore commit state in `initializeState`. [Tagged SQL sink](https://github.com/lance-format/lance-flink/blob/v0.1.0/src/main/java/org/apache/flink/connector/lance/table/LanceDynamicTableSink.java#L50), [tagged sink implementation](https://github.com/lance-format/lance-flink/blob/v0.1.0/src/main/java/org/apache/flink/connector/lance/LanceSink.java#L147).

**Static-code inference:** a failure after a Lance commit but before checkpoint completion can lead to replayed input being appended again. This research did not run a fault-injection experiment. Connector marketing or Flink's own state guarantee should not be used to claim checkpoint-consistent Lance CDC. The finding concerns this tagged upstream connector, not an inspected Huawei private implementation.

Recommended completion work is checkpoint-linked, idempotent publication with durable recovery state; keyed update/delete handling; and an explicit read/change-feed contract. Until qualified, use the connector within its verified operation set or retain a durable intermediate stream/table and publish through a validated writer.

### Spark: historical context, bulk joins, and reconciliation

Use Spark for large historical scans, relational joins, aggregations, batch feature preparation, and reconciliation between streaming results and authoritative source tables. Existing SQL and PySpark jobs can remain part of the platform. Huawei's engine guide also describes Python UDF/model integration, so structured processing is a primary role rather than an intrinsic media limitation. [Spark engine](https://support.huaweicloud.com/productdesc-aidatalake/aidatalake_08_0009.html).

The upstream Lance Spark connector is more than a basic reader. v0.8.0 includes distributed scalar/FTS builds, incremental `REFRESH INDEX`, `OPTIMIZE INDEX`, index-coverage reporting, branch/tag reads, and Blob v2 improvements. Assign a maintenance owner when these functions coexist with Aura or Ray jobs. [v0.8.0 release](https://github.com/lance-format/lance-spark/releases/tag/v0.8.0).

**Recommended workflow:** Spark prepares a stable cohort and joins historical business attributes; Aura enriches/searches multimodal columns; Ray executes expensive model computation; results remain linked by durable IDs and recorded input snapshots. A workflow need not visit all four engines to be part of the shared platform.

## 8. Service and connector compatibility

### Documented AI DataLake service baseline

| Engine | Version in the 26.9.0 bulletin | Relevant documented format coverage |
|---|---|---|
| Aura | 3.9.0 | Lance, Iceberg V2/V3, media and conventional file formats |
| Ray | 2.54.0 | Media and common file readers/writers; qualify the installed Lance connector separately |
| Spark | 4.0.2 | Iceberg V2/V3 and Paimon; Spark ecosystem lists Paimon 1.2.0 |
| Flink | 1.20 | Iceberg and Paimon connectors, alongside Kafka and other listed connectors |

Source: [26.9.0 bulletin](https://support.huaweicloud.com/verspt-bulletin-aidatalake/aidatalake_bulletin_0003.html). These are published service combinations, not a local deployment inventory. A missing entry in a change-oriented bulletin is not proof of unsupported functionality. Confirm Lance packages and operations in the target Ray/Spark/Flink image before promising cross-engine access.

### Latest upstream connector releases observed

| Connector | Release / publication date | Dependency or engine contract | Implication |
|---|---|---|---|
| `lance-ray` | 0.5.0 / 24 July 2026 | `pylance>=9.0.0`, `ray[data]>=2.41.0`, Python `>=3.10,<3.14` | Dependency lower bounds do not certify every future combination; pin the tested environment |
| `lance-spark` | 0.8.0 / 6 October 2026 | Root build pins Lance 12.0.0; modules cover Spark 3.4/3.5/4.0/4.1/4.2 | Select the matching Spark/Scala/JDK module and qualify Lance capabilities before adopting newer table features |
| `lance-flink` | 0.1.0 / 17 July 2026 | Lance 7.0.0, Arrow 18.3.0; modules for Flink 1.18/1.19/1.20 | This is a materially older Lance dependency; table capability and write/recovery semantics need explicit testing |

Release metadata was checked using GitHub's latest-release API. Dependency evidence: [Ray manifest](https://github.com/lance-format/lance-ray/blob/v0.5.0/pyproject.toml), [Spark POM](https://github.com/lance-format/lance-spark/blob/v0.8.0/pom.xml), [Flink release](https://github.com/lance-format/lance-flink/releases/tag/v0.1.0), [Flink POM](https://github.com/lance-format/lance-flink/blob/v0.1.0/pom.xml).

Continue the earlier report's evaluation baseline of **Lance 14.0.2 and explicit file format 2.2**, but certify the consumers before enabling new table capabilities. Library version, file format, index format, catalog implementation, and connector version are separate dimensions. The tagged format guide also documents an all-null inner `FixedSizeList` case requiring readers at least 11.1.0, illustrating why “supports 2.2” is insufficient for the old Flink dependency. [Tagged versioning guide](https://github.com/lance-format/lance/blob/v14.0.2/docs/src/format/file/versioning.md).

## 9. The handoff contract that makes the engines cooperate

The following is a **recommended integration contract**, not a claim that one current product feature implements it automatically.

| Concern | Record or enforce | Failure avoided |
|---|---|---|
| Record identity | Stable business/asset ID; child ID for chunk/frame; source revision | Positional joins assigning embeddings to the wrong records after filtering or repartitioning |
| Input identity | Catalog/table identity and exact snapshot/version for every source | Different stages reading different meanings of “latest” |
| Model identity | Model revision/digest, tokenizer/preprocessing version, embedding dimensions, dtype, normalization, metric | Mixing incompatible embedding spaces or silently changing search semantics |
| Publication | Workflow/run/task IDs, deterministic result keys, an authoritative commit owner, durable completion evidence | Duplicate output after retries or partial output presented as complete |
| Source progress | Stream offsets/checkpoint or batch boundary associated with the published output | Inability to explain freshness or recover the correct range |
| Cross-format joins | Business ID plus source snapshot pair and selected record versions | Assuming Lance and Iceberg/Paimon share an atomic transaction |
| Media access | Immutable object identity/version or supported blob descriptor, access policy, retention | A stored URI later resolving to different bytes or disappearing during a job |
| Lifecycle | Snapshot retention, index coverage, compaction/cleanup ownership, active-job references | Cleanup racing with training, backfill, or query execution |

Preserve record identity through Arrow batches and shuffle; Arrow representation alone does not define a durable workflow identity. Avoid using row order as an inter-engine join key. Internal Lance row IDs can be useful within their supported snapshot/lineage contract, but an explicit application key is the safer cross-format boundary.

For independent table commits, publish a **workflow manifest only after all required outputs are verified**. Consumers read the manifest's named versions. This provides an application-level consistent selection of outputs; it does not turn the underlying writes into one atomic cross-table transaction. Failed or orphaned outputs need a retention and cleanup policy.

For shared compute, allocate separate budgets to streaming ingestion, inference, retrieval, and index maintenance. CPU preprocessing and object I/O must keep up with accelerator demand. Admission control should prevent an embedding backfill from consuming the headroom required for fresh-data publication.

## 10. Three concrete workflows

### A. Multimodal dataset creation and embedding refresh

1. **Spark, when needed:** prepare a source inventory and join business metadata at recorded snapshots.
2. **Aura:** decode/chunk media and produce an initial embedding dataset in Lance using the selected UDF/model.
3. **Ray, when needed:** execute a larger model or refresh features on GPU/NPU workers over a pinned input selection.
4. **Lance plus the designated execution owner:** publish validated data and maintain the relevant indices.
5. **Aura:** search, filter, and curate; record the selection version for downstream consumers.

Aura may execute the whole transformation if its runtime fits. Splitting into Ray is a workload choice, not a requirement to generate an embedding twice.

### B. Fresh business context for a multimodal lake

1. **Flink:** process incoming product/device/customer changes with source progress, event ordering, deduplication, and explicit deletion semantics.
2. **Iceberg/Paimon:** persist the structured output using the qualified connector's contract.
3. **Aura or Ray:** select changed assets and compute the required derived features, avoiding a full re-embedding pass.
4. **Validated Lance writer:** apply the intended changes and publish an output version with source progress and model identity.
5. **Retrieval layer:** expose separately the latest committed version and the extent of ANN-index coverage.

A direct Flink-to-Lance path can shorten the pipeline after its insert/update/delete and recovery guarantees are proven. The existence of checkpoints in the engine is not that proof.

### C. Retrieval-to-training feedback

1. **Aura:** find the relevant scenario using vector, text, and metadata conditions; emit IDs/scores before materializing large media.
2. **Spark, when needed:** add historical attributes, enforce cohort constraints, and reconcile counts.
3. **Ray:** read the selected version, evaluate or enrich records, and supply batches to the chosen training/evaluation runtime.
4. **Shared lake:** retain selection lineage and model metrics so the next experiment can reproduce or compare the cohort.
5. **Flink, when needed:** continuously add fresh outcomes that inform the next selection cycle.

These examples explain the platform's value through connected work. They do not require every engine to read every format or participate in every job.

## 11. Scale and measurements inherited from the 100B study

The engine split should preserve the earlier workload assumptions without presenting them as measured performance:

| Planning anchor | Calculation | Integration consequence |
|---|---|---|
| 100B × 1,024 FP32 embeddings | `100e9 × 1024 × 4` = **409.6 TB** of vector payload | Avoid unnecessary full-table export/reload between jobs; use selective and streaming reads |
| 5B new embeddings/day | `5e9 / 86400` ≈ **57,870 vectors/s** average | Size transformation, inference, publication, and index upkeep as one sustained pipeline |
| One million selected IDs/scores | `1e6 × (8 + 4)` = **12 MB**, before protocol/additional columns | Pass a compact selection before fetching media or full vectors |
| Five-minute unindexed tail at that arrival rate | About **17.36M vectors**, **71.11 GB** FP32 payload | Track commit freshness and indexed freshness independently |

Decimal units; these are arithmetic lower bounds excluding media, metadata, replication, retained versions, and temporary space. Creating embeddings may require substantially more upstream media I/O and compute. No reviewed source or experiment in this task certifies the combined 100B workload across these four engines.

Measure the complete workflow, not just an engine's busiest operator:

- **Data freshness:** arrival → durable structured commit → embedding completion → Lance commit → indexed visibility.
- **Correctness:** unique IDs, missing/duplicate records, update/delete agreement, snapshot reproducibility, model-version consistency, null behavior.
- **Compute efficiency:** accelerator useful work and data-wait time, samples/second, batching efficiency, CPU preparation throughput, peak host/device memory.
- **Storage efficiency:** bytes read/written, remote request count, small-file growth, shuffle bytes, cache behavior, retained snapshots, temporary build space.
- **Recovery:** retries, checkpoint/commit duration, recovery time, duplicated computation, orphaned outputs, retained progress.
- **Retrieval quality and economics:** global recall at the requested k, latency under concurrent ingestion, cost per indexed billion records, and cost per useful training/evaluation selection.

For business value, compare the same end-to-end output with and without shared-table reuse. A reduction in data copies should appear as measured bytes, elapsed time, and cost; avoid an unqualified “zero-copy across four engines” claim.

## 12. Delivery priorities and acceptance gates

### Engine implementation opportunities

These proposals identify concrete work inside the engines and their connectors that would improve the shared ecosystem. They extend the documented foundations in sections 5–8; they do not imply that every listed primitive is missing from the target deployment. Confirm the remaining implementation scope against its actual runtime and connector versions.

| Engine | Proposed implementation | Benefit to AI DataLake | Acceptance evidence |
|---|---|---|---|
| **Aura** | Push runtime join filters into active Lance scans; carry lazy media references through joins, sorting, and Top-K, materializing payloads after selection | Less object-store I/O and fewer intermediate rows when multimodal retrieval joins business metadata | Identical results/recall with lower remote bytes and rows exchanged; bounded memory; correct null and filter behavior |
| **Ray** | Connect Lance prefetch to model-aware adaptive batching and bounded CPU/GPU/NPU queues; persist input/model/task progress and publish backfill output idempotently | Better accelerator utilization and embedding jobs that resume using completed work | Lower accelerator data-wait and higher end-to-end throughput at the same model quality; recovery produces complete output without duplicate publication |
| **Flink** | Add keyed Lance upserts/deletes and checkpoint-linked, replay-safe publication with restored commit state | Reliable continuous updates to the multimodal lake, with explainable freshness and deletion behavior | Fault injection around commit and checkpoint completion; correct replay, late updates, deletes, and recovery of source progress |
| **Spark** | Improve Lance-to-Iceberg/Paimon join planning using connector statistics, filter/projection pushdown, and delayed media reads where supported; coordinate table/index maintenance with other engines' active work and retained snapshots | Efficient combined analysis of structured and multimodal data; maintenance that preserves downstream workflows | Reduced scan/shuffle bytes at identical results; concurrent-writer and snapshot-retention checks during maintenance |

Reuse existing features when implementing these improvements: Aura's query/UDF execution, Ray's scheduling and batched model actors, `lance-ray` column evolution, and Spark's released index build/refresh/optimization operations. Flink's proposed Lance change processing addresses the specific insert-only and recovery boundaries documented in section 7. The runtime-filter and lazy-media proposal also aligns with the Aura integration direction in `tmp/part2.pptx`.

### Delivery order

| Priority | Deliverable | Acceptance evidence |
|---|---|---|
| 1 | Runtime/connector inventory and shared catalog contract | Record actual versions, supported operations and table capabilities; all intended engines resolve/access the correct table with the expected permissions |
| 2 | Aura → Lance → Ray reference workflow | Exact ID alignment, model metadata, repeatable snapshot reads, bounded memory, and recoverable publication across multiple fragments |
| 3 | Flink structured ingestion and selected Lance publishing path | Kill/restart around sink commit and checkpoint completion; prove intended duplicate/update/delete behavior and bounded freshness lag |
| 4 | Spark batch interoperability and maintenance ownership | Schema/null/binary/vector mapping, filter/projection behavior, snapshot reads, and concurrent maintenance tests |
| 5 | GPU/NPU utilization and end-to-end scale | Same model/output-quality contract; include CPU preparation, device transfer, object I/O, retries, and idle cost; scale progressively toward the inherited workload |

Include schema evolution, null columns/vectors, all-null nested values, empty batches, duplicate delivery, late changes, expired credentials, worker loss, coordinator restart, and concurrent compaction/cleanup. Qualification should cover actual reader/writer pairs; matching format labels alone are insufficient.

Designate one owner for each concern: the workflow owns dependencies and handoffs; an engine owns its operator state/execution; Lance owns supported storage/index semantics; the publication path owns commit coordination; platform governance owns credentials and policy. This keeps responsibility clear when a job crosses engine boundaries.

## 13. Slide content and presenter notes

**Title:** Ecosystem: Make Four Engines Work as One

**Subtitle:** Match workloads to engines; share data, governance, and reproducible outputs.

| Engine | Primary role | Typical work | Proposed engine improvements |
|---|---|---|---|
| Aura | Multimodal processing; SQL / DataFrame + UDFs | Decode, extract, embed; save vectors + metadata in Lance | Runtime join-filter pushdown; lazy media across SQL operators |
| Ray | Accelerated AI processing; CPU / GPU / NPU | Distributed inference + backfill; prepare training / evaluation data | GPU/NPU prefetch + adaptive batching; resume embedding backfills safely |
| Flink | Structured streaming; events / change data | Clean, deduplicate, enrich; event-time joins + state | Lance upserts / deletes; checkpoint-linked, replay-safe commits |
| Spark | Structured batch processing; SQL / ETL | Historical joins + aggregates; feature preparation + backfill | Optimize cross-format joins; coordinate table / index maintenance |

**Shared foundation:** Lance for embeddings, media references, and versioned selections; Iceberg/Paimon for structured tables and changes; OBS and LakeFormation for storage and shared governance.

**Integration message:** Shared catalog and permissions · Stable IDs and snapshot handoffs · Coordinated commits and recovery.

**Small-print qualification:** Primary roles overlap. Improvements are proposals extending existing capabilities; implementation scope and sources are in the report and speaker notes.

**Suggested spoken explanation:** “Aura turns multimodal content into searchable data, including embeddings stored in Lance. Ray supplies flexible AI computation and GPU/NPU execution. Flink maintains fresh event-driven context, while Spark performs large historical joins and batch preparation. To strengthen the ecosystem, we can push join filters and lazy media handling further in Aura, improve prefetch and resumable backfills in Ray, implement reliable Lance change processing in Flink, and optimize cross-format joins and shared maintenance in Spark.”

The slide follows both reference decks' 16:9 canvas, Arial typography, navy text/header, red and blue emphasis, thin table borders, and white background. It uses an editable native PowerPoint comparison table. Detailed caveats and primary-source links belong in speaker notes and this report.

## 14. Evidence and limits

- The two requested commits were inspected as repository history and used to establish context, not as substitutes for current primary evidence.
- Huawei sources describe the published service architecture and engine capabilities. Apache/Ray/Lance sources describe their respective upstream contracts. They are intentionally distinguished in the compatibility section.
- Upstream release metadata and tagged connector manifests/source were checked on 6 November 2026. Documentation using `latest` can change; the Ray accelerator support comparison uses the service-aligned 2.54.0 tag.
- Recommendations about job ownership, workflow manifests, stable IDs, and acceptance tests are engineering analysis derived from the integration requirements.
- No production environment, device, deployed connector, or 100B benchmark was exercised. The Flink replay concern is a source inspection finding to validate with fault injection.
- Source links accompany the claims they support. The HTML contains the complete report and a render of the actual one-slide PowerPoint for offline review.
