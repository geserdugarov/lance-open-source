# Lance, Milvus, Qdrant, and PostgreSQL + pgvector at 100 billion vectors

**Research date: 2026-11-06.** This report replaces the ecosystem comparison in the [earlier industry overview](aidatalake-lance-100b-industry-ecosystem-overview.md) for the workload below. It retains that report's useful capacity and large-result considerations, expands the comparison to the four requested systems, and rechecks current primary sources. The [Aura indexing proposal](aura-distributed-indexing.md) provides integration context; it is a proposal, not evidence of delivered performance.

**Review artifacts:** [one-slide PowerPoint](100b-vector-ecosystem-comparison.pptx) · [HTML review](100b-vector-ecosystem-comparison.html) · [slide preview](100b-vector-ecosystem-comparison.png).

## 1. Decision for this workload

**Shortlist Lance with a distributed execution layer and Milvus Distributed.** For a platform that combines global retrieval, dataset curation, SQL, and training, **Lance + Aura is the preferred architectural direction**, provided the team will own distributed execution and maintenance. **Milvus is the strongest packaged distributed-service baseline** to evaluate against it, including a Milvus-over-Lance configuration. This is an engineering recommendation based on workload fit, not a measured performance ranking.

**Keep Qdrant as a credible alternative, especially if application serving and filtered retrieval become more important.** Its distributed deployment, disk placement, compression, and filtering are substantial capabilities. Its main question here is the cost of maintaining and searching HNSW across the entire corpus at very large k. **Use PostgreSQL + pgvector for transactional applications, metadata/control services, or smaller working sets; do not select plain PostgreSQL + pgvector as the default global 100B-vector search tier.** Building the missing distributed layer changes the proposed solution substantially.

**No reviewed public evidence verifies the complete combination of 100B × 1,024 dimensions, 95–99% recall, top-10K–1M, 5B additions/day, minute-level latency, and concurrency 16 for any of the four systems.** A capacity claim, a 10B benchmark, or a fast top-10 query does not establish this requirement. Section 8 separates the available evidence from the conclusions it can support.

### Confirmed requirements and remaining assumptions

| Item | Requirement / interpretation |
|---|---|
| Initial / target corpus | **20 billion → 100 billion individual vectors**, each with **1,024 dimensions**. |
| Growth | **5 billion additional vectors per day**. Treat this as append ingestion; replacement updates and deletes require separate sizing. |
| Query scope | **The full corpus.** Tenant/time restrictions cannot be used to make the main benchmark smaller. ANN can prune candidates internally, but recall must be measured against all eligible vectors. |
| Quality | **Recall@k of approximately 95–99%.** Evaluate 0.95 and 0.99 separately; do not assume the cost difference is small. |
| Result size | **k = 10,000; 100,000; 1,000,000**. |
| Concurrent queries | **1, 4, and 16**, while ingestion and maintenance continue. |
| Latency | **Minute level.** Report against 60-, 180-, and 300-second budgets until the exact percentile and deadline are agreed. These are evaluation bands, not user-approved SLAs. |
| Vector precision | **FP32 assumed** for sizing and reference-quality scoring. FP16 and compressed originals are separate quality experiments. |
| Still unspecified | Metric and normalization, embedding model/distribution, acceptable indexing lag, arrival bursts, initial-build deadline, retention, payload size, hardware/cloud, availability and recovery targets. |

The corpus reaches 100B **16 days after the initial 20B**, assuming no retention or deletion. Continuing the same growth produces 105B on day 17; 100B is a capacity checkpoint unless retention is specified. Concurrency is not QPS: at 16 continuously occupied slots and a three-minute mean response time, throughput is only about **5.33 completed queries/minute**.

### What “Lance” means in this comparison

Lance is an open table/file format and storage/indexing engine. It exposes distributed build primitives, but its documentation explicitly assigns worker scheduling and workflow orchestration to the caller. A storage library is not a ready-to-operate replicated search service. **The deployable 100B candidate is Lance + Aura or another distributed engine**, with the engine and its costs named explicitly. LanceDB OSS and LanceDB Enterprise are different products layered on Lance; Enterprise benchmark results are not results for the standalone Lance library. [Distributed indexing](https://lance.org/guide/distributed_indexing/); [LanceDB distributed architecture](https://www.lancedb.com/blog/how-lancedb-accelerates-vector-search-at-10-billion-scale).

The other baseline scopes are **Milvus Distributed OSS**, **Qdrant distributed OSS**, and **PostgreSQL with the pgvector extension**. Managed offerings are options, not features silently credited to the OSS cores. In particular, Qdrant documents automatic rebalancing and online resharding as Cloud capabilities, including its Hybrid/Private offerings; self-hosted OSS can move existing shards but needs a deliberate shard-count strategy. [Qdrant deployment](https://qdrant.tech/documentation/scaling/distributed_deployment/); [Cloud scaling](https://qdrant.tech/documentation/cloud/cluster-scaling/).

## 2. Comparison at a glance

These judgments describe the confirmed workload. “Needs validation” does not mean unsupported, and “best fit” does not mean a verified SLA.

| Decision axis | Lance + distributed engine | Milvus Distributed | Qdrant distributed | PostgreSQL + pgvector |
|---|---|---|---|---|
| Primary strength | Shared versioned data for retrieval, analytics, and training | Complete distributed vector-service architecture | Focused vector service, filtering, hybrid retrieval | SQL, joins, transactions, and existing PostgreSQL operations |
| Durable data model | Columnar tables and indices in object storage; executor caches | Object-backed segments/indices; separately scalable service roles | Shards with persistent segment storage and replicas | PostgreSQL relations, WAL, and database replication |
| Dense search options relevant here | IVF with PQ/RaBitQ; IVF/HNSW variants | IVF quantizers, HNSW variants, DiskANN and other disk paths | HNSW plus quantization and exact scans | HNSW, IVFFlat, exact SQL scans |
| Work needed for 100B | Distributed query/build orchestration, budgets, recovery, capacity qualification | Deployment/tuning of a distributed service; segment and index lifecycle qualification | Shard layout, placement/replication, graph-build throughput, large-k qualification | External sharding, distributed top-k, bulk lifecycle, and substantial custom operations |
| Full-corpus top-10K–1M | Natural fit with a scan/SQL pipeline; global merge and large-k recall remain engineering work | SearchIterator beyond ordinary search limits; measure complete iterator execution | Test large query limits, graph traversal and merging; scroll is not ranked ANN pagination | SQL can express the result; cross-shard execution is outside plain pgvector |
| Adding 5B/day | Append data, build bounded segments, publish coverage; control the unindexed tail | Streaming/bulk ingestion, growing→sealed segments, asynchronous index building | WAL/appendable segments and background optimizers; indexing competes for resources | Inserts and ANN maintenance add WAL and database work; partition/bulk loading helps |
| Metadata / text | Scalar and full-text indices; SQL through engines | Scalar filtering, standard/iterative filtering, hybrid retrieval | Payload indices, filterable HNSW/ACORN, sparse and text retrieval | Relational filtering, joins, native text search |
| Reproducible mining | Dataset versions/tags plus versioned selections | Explicit visibility/snapshot or materialized-job contract needed | Replica consistency controls; a separate reproducible-export contract needed | MVCC within a database; durable datasets and cross-shard snapshots need design |
| Main economic exposure | Engineering ownership, object I/O, cache misses, refinement, compaction | Service overhead, build resources, cached indices, segment churn | Replicated storage, graph overhead, random I/O, optimizer backlog | Heap/TOAST + ANN + WAL/replicas, vacuum, custom sharding |
| Role in this evaluation | **Preferred lake/mining architecture** | **Primary distributed-service comparator** | **Secondary candidate; prove large-k economics** | **Control/transactional tier; weak default for the global corpus** |

Capability sources: [Lance index formats](https://lance.org/format/index/), [Lance integrations](https://lance.org/integrations/), [Milvus architecture](https://milvus.io/docs/architecture_overview.md), [Milvus index choices](https://milvus.io/solutions/distributed-vector-database), [Qdrant indexing](https://qdrant.tech/documentation/manage-data/indexing/), [Qdrant storage](https://qdrant.tech/documentation/manage-data/storage/), [pgvector v0.8.7](https://github.com/pgvector/pgvector/blob/v0.8.7/README.md). The ranking and workload consequences are analysis based on these capabilities and the calculations below.

## 3. Capacity, ingestion, and query economics

### 3.1 Storage floors

All capacities are **decimal TB/GB**, calculated for one vector column with no compression of the original FP32 values. Code sizes are ideal packed payloads, not total index sizes. Add IDs, quantizer factors, centroids/codebooks, graph structure, alignment, scalar/text indices, metadata, replicas, retained versions, and temporary build/merge space.

| Representation | Bytes/vector | At 20B | At 100B | Added per day at 5B |
|---|---:|---:|---:|---:|
| FP32 originals | 4,096 | 81.92 TB | **409.60 TB** | **20.48 TB** |
| FP16 originals | 2,048 | 40.96 TB | 204.80 TB | 10.24 TB |
| 8-bit/component codes | 1,024 | 20.48 TB | 102.40 TB | 5.12 TB |
| 5-bit/component codes, e.g. RQ5 | 640 | 12.80 TB | **64.00 TB** | **3.20 TB** |
| 4-bit/component codes | 512 | 10.24 TB | 51.20 TB | 2.56 TB |
| 1-bit/component codes | 128 | 2.56 TB | **12.80 TB** | 0.64 TB |
| PQ with 128 / 256 8-bit subcodes | 128 / 256 | 2.56 / 5.12 TB | 12.80 / 25.60 TB | 0.64 / 1.28 TB |
| One 64-bit ID | 8 | 0.16 TB | 0.80 TB | 0.04 TB |

FP32 + RQ5 + one ID copy is already **474.4 TB at 100B**. Two application-level copies of that footprint would be 948.8 TB, before overhead. Object-store durability does not automatically require two complete application-level raw-data copies; distinguish object-store redundancy, query replicas, caches, and backups when comparing designs.

An illustrative adjacency array with 32 or 64 four-byte references per vector adds **12.8 or 25.6 TB**. This is arithmetic, **not a measured HNSW or DiskANN index size**: real graph layouts, degree distributions, upper levels, ID widths, and compression differ. Quantizing the vectors does not remove graph or payload-index costs.

Compression of search codes normally leaves an original vector copy for refinement. There are deliberate alternatives: Qdrant 1.20 introduces `turbo8`/`turbo16` storage datatypes that replace originals with compressed representations. That changes the precision of later rescoring and export. Compare every candidate against the same original FP32 ground truth, and retain authoritative originals elsewhere if exact reconstruction or re-embedding workflows require them. [Qdrant datatype semantics](https://qdrant.tech/documentation/manage-data/vectors/#turboquant-datatypes); [1.20 release](https://github.com/qdrant/qdrant/releases/tag/v1.20.0).

### 3.2 Build rate and freshness

The append rate is `5e9 / 86,400` = **57,870 vectors/s** on average. Original-vector input alone is **237 MB/s** aggregate. This bandwidth floor is manageable for a cluster; distance computation, index construction, shuffle, write amplification, and failure recovery determine the real resource requirement.

| Operating scenario | Required average vector processing rate | FP32 input floor |
|---|---:|---:|
| Index each day's 5B within 24 hours | 57,870/s | 0.237 GB/s |
| Index the day's 5B in a 6-hour window | 231,481/s | 0.948 GB/s |
| Initial 20B in 24 hours, if that deadline is chosen | 231,481/s | 0.948 GB/s |
| Rebuild 100B in 24 hours, if required | 1,157,407/s | 4.741 GB/s |

These are planning examples, not measured throughput or confirmed build deadlines. A pipeline operating exactly at the daily average has no catch-up capacity. With six hours unavailable each day, the remaining 18 hours require **77,160 vectors/s** even before catching up an older backlog. A provisional 2× sustainable indexing target would be about 116,000 vectors/s; accept it only if the cost and recovery requirements justify it.

| Unindexed lag at the stated ingest rate | Unindexed vectors | FP32 tail |
|---|---:|---:|
| 1 minute | 3.47 million | 14.22 GB |
| 5 minutes | 17.36 million | 71.11 GB |
| 1 hour | 208.33 million | 853.33 GB |
| 24 hours | 5 billion | 20.48 TB |

Define **write acceptance, durable commit, query visibility, and ANN-index coverage separately**. Hiding an unindexed tail changes the searchable corpus; scanning it can change query cost sharply. If queries use a completed daily snapshot, record that contract and its age. If they require the latest committed corpus, the implementation must search the tail too. Minute-level query latency does not imply minute-level data freshness.

### 3.3 Full-corpus search does not mean exhaustive raw-vector scanning

For one 100B query, an exhaustive FP32 pass reads **409.6 TB** and evaluates approximately **102.4 trillion vector components**. Completing just that read in 60/180/300 seconds requires **6.83/2.28/1.37 TB/s** sustained aggregate bandwidth, before distance arithmetic, selection, and transfer. Sixteen independent concurrent scans multiply demand unless the implementation shares data reads or batches scoring.

A full scan of ideal RQ5 codes is still 64 TB/query; of one-bit codes, 12.8 TB/query. ANN partition or graph traversal is therefore essential to cost-effective execution. The key measurement is **bytes and candidates visited at the required global recall**, not the label “disk based” or “100B supported.” At 99% recall and k = 1M, wider probing and refinement may dominate.

Full-corpus eligibility allows similarity-based ANN pruning, but not arbitrary exclusion of old days or tenants. Establish an all-segment baseline before adding a learned/router layer. Any router that skips segments becomes another approximation stage whose missed neighbors count against end-to-end recall.

### 3.4 Segmentation and large-k merging

As an initial experiment, 100–250M vectors per logical build segment would give **1,000–400 segments at 100B** and **50–20 new segments/day**. Each segment contains 409.6 GB–1.024 TB of original FP32 vectors and 64–160 GB of ideal RQ5 codes. These are experimental task sizes, not prescribed engine defaults or worker counts; Milvus and Qdrant have their own physical segment units.

For k = 1M, a naive plan that returns k `(uint64 ID, float32 score)` pairs from each of 400–1,000 segments moves **4.8–12 GB/query** before protocol overhead. At concurrency 16 this is **76.8–192 GB** of candidate traffic across those executions. The final 1M pairs are only **12 MB**. A full-vector payload for those final results adds **4.096 GB/query**, before media.

Use hierarchical merging, bounded candidate streams, spill where necessary, and progressive candidate requests. **Do not allocate k/segment_count results to each segment:** all true nearest neighbors may lie in one segment. Exhaustive local top-k from every segment suffices for exact global top-k; reducing that exchange requires justified bounds or an approximate policy measured against global ground truth. Streaming a final result does not itself prove bounded memory inside the ANN operator.

Refinement also needs a global budget. Refining 10M total candidates reads at least **40.96 GB of FP32 payload**; refining 10M independently on each of 1,000 segments changes that floor to **40.96 TB**. Group reads by storage locality and retain original IDs to reconstruct score order. Report candidate generation, exact rescoring, merge, and payload delivery separately.

### 3.5 Compare complete cost

Use the same quality, freshness, failure tolerance, and result contract for every cost comparison:

`total cost = durable storage + replicas/backups + RAM/NVMe caches + build/maintenance compute + query compute + object requests/network + operational engineering`.

Record cost per billion vectors added/indexed, per successful full-corpus query at each k/recall, and per million selected training examples delivered. Include temporary coexistence of old/new indices during rebuilds. Do not quote a node count or currency estimate until measured per-node throughput, memory, I/O, headroom, deployment rates, and query duty cycle exist.

## 4. Lance: strongest fit for the shared mining dataset, with service work required

### Architecture and deliberate tradeoffs

Lance puts columnar data, versioned table metadata, and search indices on a common storage foundation. IVF narrows the search to selected partitions, compact codes reduce scoring work, and optional refinement reads originals. This makes the same dataset useful for projected scans, retrieval, and subsequent payload access. Public segment APIs support both shared and independently trained vector models. Independent models simplify isolated builds; physical merging requires compatible model artifacts. [Distributed indexing model](https://lance.org/guide/distributed_indexing/); [vector-search guide](../docs/src/quickstart/vector-search.md).

The central tradeoff is **open, reusable data and flexible compute versus responsibility for a production service**. Object storage is attractive for hundreds of TB, but small random reads, uncached refinement, model distribution, and compaction have real costs. The caller chooses scheduling, placement, failure handling, admission control, and distributed query contracts. A successful local query or distributed build API call does not solve those system concerns.

### Pros

- **A common dataset for search and training.** Version-pinned selections can feed Aura, Spark, Ray, and training readers without maintaining a separate authoritative vector database copy. Connector existence still needs runtime compatibility testing. [Integration inventory](https://lance.org/integrations/).
- **Selective access to large payloads.** Blob access and column projection let execution carry IDs/references until the final rows are chosen. This fits top-1M mining where returning every image is much more expensive than selecting IDs. [Blob guide](https://lance.org/guide/blob/).
- **Index choices and incremental building blocks already exist.** PQ, multi-bit RaBitQ, vector/scalar/text indices, segmented builds, and maintenance are foundations to integrate, not missing features to propose from scratch. The local checkout has RaBitQ bit-depth validation and segment merge/commit APIs. [RaBitQ implementation](../rust/lance-index/src/vector/bq.rs); [dataset APIs](../python/python/lance/dataset.py).
- **Durable selection identity.** Versions and tags support repeatable downstream use, provided cleanup protects all referenced data and indices. [Tags and branches](https://lance.org/guide/tags_and_branches/).

### Cons and 100B risks

- **Distributed execution remains a product obligation.** The Aura design primarily specifies index construction; full-corpus query dispatch, merging, retries, and resource isolation need a separate contract. Existing Lance segment fan-out is not equivalent to a distributed executor fleet. [Aura scope](aura-distributed-indexing.md).
- **Coverage can become a latency or correctness trap.** The local `fast_search` API deliberately searches indexed fragments only. For the confirmed full-corpus requirement, the engine must pin a clearly declared indexed snapshot or include newer rows; it cannot silently omit them to meet latency. [Scanner API](../python/python/lance/dataset.py).
- **Independent models and many deltas increase query work.** They avoid a global rebuild but accumulate routing, model-loading, and scoring costs. Model compatibility, drift monitoring, segment-size limits, and maintenance budgets matter at 50 new experimental segments/day.
- **99% recall and k = 1M need direct qualification.** Small-k tuning, a nominal refinement factor, or a single-core quantizer result is insufficient. Candidate loss from probing cannot be recovered by reranking candidates that were never retrieved.
- **Cross-engine operations add coordination.** Pin compatible runtimes, index formats, feature flags, and cleanup authority. Namespace/catalog integration does not by itself provide cross-system transactions or consistent authorization.

### When to choose it

Choose **Lance + Aura** when a shared lake dataset, large result sets, reproducible curation, and downstream SQL/ML are core requirements, and the team can operate the distributed layer. Minute-level latency makes controlled remote I/O and batched refinement more acceptable than in a strict millisecond serving service. Use **LanceDB Enterprise** as a separately priced/evaluated deployment alternative if buying that layer is preferable.

Avoid presenting **the Lance library alone** as the finished 100B solution. Its suitability is the strongest here when the surrounding platform is part of the proposal and tested with it.

## 5. Milvus: strongest packaged distributed-service baseline

### Architecture and deliberate tradeoffs

Milvus separates API access, coordination, streaming work, batch/query work, and durable storage. This supports independent resource allocation for ingestion, indexing, and querying. The tradeoff is a broader operational system: coordination, WAL/log configuration, object storage, worker fleets, segment management, and caches all need reliable operation. [Architecture](https://milvus.io/docs/architecture_overview.md).

For this workload, compare **compressed IVF first**, then disk graph alternatives at equal recall and large k. DiskANN uses disk-resident graph/raw data with compact codes and caching to limit RAM demand. It still needs fast local storage and enough bandwidth for traversal/refinement. It should not win by default simply because the corpus is large. [DiskANN](https://milvus.io/docs/diskann.md); [IVF_RABITQ](https://milvus.io/docs/ivf-rabitq.md).

### Pros

- **Distributed serving and index lifecycle are product capabilities.** There is less custom service machinery to build than with a storage engine alone.
- **Several algorithm families.** IVF, quantization, HNSW variants, and disk indices allow meaningful build-cost/search-cost experiments for the same corpus. This is useful when high recall and large k favor a different operating point from online top-10 search. [Index choices](https://milvus.io/solutions/distributed-vector-database).
- **Tiered storage.** Milvus can keep metadata resident and fetch cold data/index files from object storage. Field data is loaded in chunks; index loading is at segment granularity. This can reduce resident capacity but makes cache misses and segment sizes material to query latency. [Tiered storage](https://milvus.io/docs/tiered-storage-overview.md).
- **Filtering and visibility controls.** Standard/iterative filtering and strong/session/bounded/eventual consistency are documented. Bounded staleness is the default; ANN recall and read freshness are different properties. [Filtering](https://milvus.io/docs/filtered-search.md); [consistency](https://milvus.io/docs/consistency.md).
- **Lance can remain the source of truth.** Milvus 3.0 Storage V3 supports external `lance-table` collections. This creates a real integration option instead of requiring a forced choice between the two. [Storage V3](https://milvus.io/docs/storage-v3.md).

### Cons and 100B risks

- **Ordinary search is not the large-k path.** The documented single-search top-k maximum is 16,384. Use **SearchIterator** for top-100K/1M and measure total retrieval time, peak memory, recall, index compatibility, concurrent writes, and restart behavior. GPU/index support differs; verify the exact chosen path. [Limits](https://milvus.io/docs/limitations.md); [SearchIterator](https://milvus.io/docs/with-iterators.md).
- **Ingestion can outrun sealing/indexing/compaction.** A fast insert acknowledgment does not prove 5B/day become indexed while 16 global queries run. Monitor growing data, sealed-but-unindexed segments, index build queue, compaction work, and query resources separately.
- **Global queries reduce the value of cold-data tiering.** When every query can touch the entire collection, fewer segments remain reliably cold. Segment-level index fetches can amplify small logical requests; test cold starts and cache churn rather than treating object-store capacity as ready query capacity.
- **External access has a second lifecycle.** External collections are read-only from the Milvus data-write path, have their own manifests/indices, and require source refresh. They do not reuse Lance vector index files. Validate refresh work for a 5B daily append and protection of source files during Lance compaction/cleanup. A metadata-refresh timing example is not a 5B index-build result. [External collection workflow](https://milvus.io/docs/create-an-external-collection.md); [3.0 release](https://github.com/milvus-io/milvus/releases/tag/v3.0.0).
- **A vector service is not the whole training lake.** SQL joins, media processing, and reproducible sample delivery still need the surrounding data platform. This gap is narrower with external Lance access, but the integration must be qualified.

### When to choose it

Choose Milvus when a complete distributed retrieval service and independently managed build/query fleets are more valuable than owning those internals. It is the **first comparator** for Lance + Aura under the confirmed workload. Evaluate two concrete deployments: a Milvus-managed collection and a Milvus external collection over Lance; their copy cost, freshness, and maintenance behavior differ.

Prefer another architecture if the service's complexity is disproportionate to a small application, or if large-k iterator behavior cannot meet the required quality/cost envelope. Neither “unlimited entities” nor a documented ingestion-shard limit is a useful substitute for a measured cluster capacity plan.

## 6. Qdrant: capable distributed retrieval, with graph economics to prove

### Architecture and deliberate tradeoffs

Qdrant organizes persistent data into segments within shards, replicates shards across peers, and uses HNSW for dense ANN. Payload indices participate in filtered retrieval; compressed representations and placement controls reduce RAM pressure. Its focus is a coherent retrieval service. The tradeoff for this workload is that storage, graph building, optimization, and query work must be balanced across shard placements. [Storage](https://qdrant.tech/documentation/manage-data/storage/); [indexing](https://qdrant.tech/documentation/manage-data/indexing/).

### Pros

- **Native distributed deployment.** Shard replication and transfers are available in OSS. Raft coordinates cluster topology/collection metadata; point-write and read consistency have separate controls. [Deployment](https://qdrant.tech/documentation/scaling/distributed_deployment/); [consistency guarantees](https://qdrant.tech/documentation/scaling/consistency-guarantees/).
- **Strong filtered/hybrid retrieval toolkit.** Filterable HNSW and ACORN address difficult filter/graph interactions. Sparse retrieval and multivector workflows are useful for application search, even though the primary test here is unfiltered full-corpus dense retrieval. [Indexing](https://qdrant.tech/documentation/manage-data/indexing/).
- **More than a RAM-only HNSW service.** Per-structure memory tiers, introduced in 1.19, distinguish pinned, cached, and cold placement. Original vectors, graph data, and compressed copies can have different placement policies. Disk I/O and page-cache pressure still need measurement. [Memory tiers](https://qdrant.tech/documentation/ops-configuration/memory-tiers/).
- **Current compression choices are broad.** Scalar, binary, product, and TurboQuant quantization trade scoring precision, speed, and memory. TurboQuant is available from 1.18; 1.20 adds `turbo8`/`turbo16` storage datatypes and BM25 scoring on the full-text payload index. Test current releases rather than an old SQ8-only configuration. [Quantization](https://qdrant.tech/documentation/manage-data/quantization/); [1.20 release](https://github.com/qdrant/qdrant/releases/tag/v1.20.0).

### Cons and 100B risks

- **Graph costs persist after vector compression.** Code compression does not eliminate graph links, IDs, replicas, or metadata. Wide graph traversal at high k/recall can become expensive; this is a workload hypothesis to benchmark, not a claim that Qdrant cannot return 1M neighbors.
- **The main workload gives up a key optimization.** Tenant-scoped routing can make a huge aggregate corpus tractable, but the required query is global. Do not use tenant-only graph configurations or time-shard exclusion to claim compliance.
- **Growth needs a deployment-specific shard plan.** OSS shard movement is not online shard-count expansion. Cloud online resharding and automatic rebalancing may be attractive at 5B/day, but must be included explicitly in deployment scope and cost. [Cloud scaling](https://qdrant.tech/documentation/cloud/cluster-scaling/).
- **Large retrieval needs a real ranked-query test.** `scroll` enumerates points; it is not an iterator over global nearest neighbors. Large `limit`/offset queries, resource quotas, result serialization, and internal candidate merging must be tested directly. Prefer ID/score-only output and fetch payloads after selection. [Search operations](https://qdrant.tech/documentation/search/search/); [client scroll semantics](https://github.com/qdrant/qdrant-client/blob/master/qdrant_client/qdrant_client.py).
- **Visibility controls are not a lake snapshot contract.** Replica consistency and ordered writes do not automatically create an immutable, cross-shard corpus for a multi-minute export. Materialize selected IDs and source versions, or explicitly freeze/version the job's corpus.
- **A second data copy may remain useful.** Qdrant's service data layout is not a shared columnar dataset for Spark/Ray training. Keeping Lance or another lake as the authoritative source adds ingestion, reconciliation, and storage work, even when Qdrant is the better serving tier.

### When to choose it

Choose Qdrant for a product that prioritizes filtered semantic/hybrid retrieval, application APIs, and distributed operation with a focused service. For this exact 100B mining workload, retain it in the bake-off if large-k recall, sustained graph construction, and replication costs are competitive. **Its lower priority here reflects the full-corpus, very-large-k workload; it is not a general ranking below Milvus or Lance.**

## 7. PostgreSQL + pgvector: transactional strength, a different scaling burden

### Architecture and deliberate tradeoffs

pgvector runs inside PostgreSQL and uses its query executor, storage, transactions, and operational ecosystem. It offers exact search, HNSW, and IVFFlat; 1,024-dimensional vectors fit its documented dense ANN dimensional limit. Half-precision and binary index representations provide additional memory/quality options. The tradeoff is integration with a general relational engine rather than a purpose-built distributed vector storage/execution service. [pgvector v0.8.7](https://github.com/pgvector/pgvector/blob/v0.8.7/README.md).

### Pros

- **Transactions and relational expressiveness.** Joins, constraints, SQL filtering, and MVCC are valuable when vectors must change atomically with business records. At ordinary application scale this can remove a separate database and synchronization pipeline. [Concurrency control](https://www.postgresql.org/docs/18/mvcc.html).
- **Established operations and tooling.** Database drivers, backup/PITR, access controls, observability, and replication often already exist in an organization. [HA and replication](https://www.postgresql.org/docs/18/high-availability.html); [WAL](https://www.postgresql.org/docs/18/wal-intro.html).
- **Good control-plane and subset role.** Store ingestion jobs, model versions, permissions, selection metadata, and compact working sets relationally while the complete vector corpus lives in a distributed lake/search tier.

### Cons and 100B risks

- **Horizontal corpus sharding is additional architecture.** Replicas can distribute read traffic but reproduce the same data; they do not automatically split one global query's corpus. Citus, PgDog, or application sharding add their own routing, merge, failure, and consistency requirements. Do not attribute them to plain pgvector.
- **Partitioning is necessary planning, not a distributed ANN engine.** PostgreSQL documents a 32-TB relation limit with default 8-KiB blocks. This is **not a database-size limit**: partitioned tables and separate relations can exceed it. Global queries without partition-key restrictions cannot rely on partition pruning to avoid most data. [Limits](https://www.postgresql.org/docs/18/limits.html); [partitioning](https://www.postgresql.org/docs/18/ddl-partitioning.html).
- **Daily append has more than vector-write cost.** Budget table/TOAST storage, ANN indices, WAL generation and replay, checkpoints, replicas, and backup traffic. Updates/deletes additionally create dead-row cleanup work; append-only tables still require vacuum-related maintenance such as visibility/freeze management. [WAL](https://www.postgresql.org/docs/18/wal-intro.html); [vacuuming](https://www.postgresql.org/docs/18/routine-vacuuming.html).
- **ANN filtering requires care.** Filtering follows an approximate index scan; iterative scans can expand work to recover results. Selective relational access, partial indices, and partitioning are alternatives. For the main unfiltered workload, the larger problem is global scale, not this filtering behavior. [pgvector filtering](https://github.com/pgvector/pgvector/blob/v0.8.7/README.md#filtering).
- **SQL LIMIT does not make large-k cheap.** PostgreSQL can express and stream large results, but retaining enough global candidates, ANN maintenance, and cross-shard query execution dominate at 100B. A cursor changes client delivery, not the cost of computing those neighbors.

### When to choose it

Choose PostgreSQL + pgvector when transactional semantics and SQL integration dominate, or when the relevant searchable working set fits the database design. It can be the right first system for an application already on PostgreSQL. Avoid a universal “over X million vectors, migrate” rule: dimension, access patterns, hardware, and SLOs determine that boundary.

For this request, use it for **control metadata, relational workflows, or a serving subset**, unless a separately funded distributed-PostgreSQL architecture demonstrates the complete requirement. Do not credit pgvector with DiskANN/PQ capabilities from other PostgreSQL extensions that are outside the requested scope.

## 8. What the public scale evidence actually establishes

These are producer-published reports or datasets. They are not an independent head-to-head comparison. No benchmark was run as part of this research.

| Evidence | What is reported | What it does not prove |
|---|---|---|
| **LanceDB Enterprise, April 2026** | 10B random 1,536-d FP32 vectors, ten 1B segments on ten query nodes, k = 100; p50/p95/p99 = 18/20/21 ms. Ten build workers give about 5× wall-clock speedup. [Report](https://www.lancedb.com/blog/how-lancedb-accelerates-vector-search-at-10-billion-scale). | Not the standalone Lance engine, not 100B, not k = 1M, and insufficient recall/hardware detail for an equivalent SLA. |
| **LanceDB, September 2026** | 10M 768-d vectors repeated 1,000 times to create 10B entries; 18.05-ms p50, 21.61-ms p99; 951 QPS at concurrency 32 and 1,066 at 256. [Report](https://www.lancedb.com/blog/10-billion-vector-search). | Entry-count scaling with duplicated vectors is not equivalent to 10B distinct embeddings or the required global large-k recall. |
| **Milvus / Kling AI** | A vendor customer story reports 10B+ vectors in a single production cluster and 10,000+ peak QPS with concurrent writes. [Story](https://zilliz.com/customers/kling-ai). | It does not specify a matched full-corpus 100B × 1,024-d, k = 1M, 99%-recall test. Cluster size alone does not tell how much each query searches. |
| **Milvus / Meituan** | 10B+ across a platform; 2B+ in its largest collection. Separately, 8.1-ms p99, around 1,000 QPS, and >97% recall are reported for a **100M × 768-d** benchmark. [Story](https://zilliz.com/customers/meituan). | The 100M benchmark metrics must not be relabeled as 10B production performance. |
| **Qdrant-FineWeb-10B, September 2026** | About 10.07B dense and sparse entries, real text, exact top-1,000 reference results, and published generation/benchmark tooling. [Dataset announcement](https://qdrant.tech/blog/qdrant-fineweb-10b-release/). | Dataset release is not a Qdrant server capacity/latency result, and top-1,000 reference data does not validate top-1M. |
| **PostgreSQL + pgvector** | Documented indices, quantization options, and external sharding approaches. [Project documentation](https://github.com/pgvector/pgvector/blob/v0.8.7/README.md#scaling). | No reviewed reproducible result satisfies the stated full-corpus workload. This is an evidence gap, not a mathematical impossibility claim. |

Useful new evidence would include exact build/software versions, embedding distribution, distinct-vector count, metric, k, recall, all-corpus query scope, concurrent ingestion, node resources, cache state, complete retrieval time, and failure recovery. A generic billion-vector or millisecond claim without these fields cannot resolve the selection.

## 9. High-value improvements for Lance

The most important opportunities are **finishing the distributed system around existing Lance primitives and improving end-to-end large-k behavior**. The local source already includes segmentation, RaBitQ, optimization, observability hooks, and multiple integrations. Do not create duplicate work by describing all of those as absent.

| Priority / owner | Current foundation and remaining gap | Concrete improvement | Acceptance evidence for this workload |
|---|---|---|---|
| **P0 — Aura/execution layer** | Segment-local search and an index-build proposal exist; the proposal does not define the complete distributed query contract. | All-corpus dispatch, hierarchical global top-k, retry semantics, request admission, deadlines, cancellation, and per-query memory/I/O budgets. | Correct global results for k = 10K/100K/1M at concurrency 1/4/16; no silent partial results after worker failure; bounded peak memory. |
| **P0 — Aura + Lance maintenance** | Uncommitted segment build/merge/commit APIs exist; durable orchestration and sustainable coverage are integration work. | Reuse build work after failure, publish complete/disjoint coverage atomically, bound segment count and tail age, isolate build and query resources. | Sustain ≥57,870 indexed vectors/s over multiple days under query load, with measured catch-up headroom and no growing backlog. |
| **P0 — Lance query core + engine** | IVF/PQ/RQ and refinement exist; high-k global cost/quality is not established. | Large-k-aware probing, candidate selection and spill; progressive refinement; stable score comparison across segment models; locality-aware vector fetches. | 0.95 and 0.99 recall measured separately against original FP32 full-corpus ground truth; report bytes fetched and candidates processed per stage. |
| **P0 — Engine + metrics** | Lance exposes metrics and scan hooks; a complete operational view spans multiple components. | Query explanation with source snapshot, coverage, segments/partitions searched, candidate counts, cache hit/miss bytes, merge spill, and tail work. | Operators can identify why a query missed its minute budget or why indexing falls behind without ad hoc profiling. |
| **P1 — Storage/cache integration** | Selective column/blob reads and local caching foundations exist. | Budget RAM/NVMe, coalesce remote reads, prewarm only useful structures, prioritize foreground queries over bulk rebuild traffic. | Equal-recall cost and p95 improve in cold and warm runs; no OOM or starvation at concurrency 16. |
| **P1 — Index maintenance** | Shared and independent models are possible. | Drift/skew detection, selective retraining, bounded delta consolidation, and model-compatible merge planning. | New-day and old-day recall remain within targets after 16 growth days; maintenance finishes without full-table rebuilds every day. |
| **P1 — Mining workflow** | Versions, tags, Arrow batches, and Blob APIs exist. | Resumable selection tables with source version/model/metric/query identity; late payload materialization through SQL and training readers. | A restarted top-1M job yields the same selected identities at the pinned snapshot, with predictable export memory and payload cost. |
| **P1 — Connectors / platform** | Spark/Ray/Flink/catalog integrations exist with separate release lifecycles. | Certify read/write/index/cleanup behavior together; coordinate streaming checkpoints, idempotency, credential refresh, and schema evolution. | No duplicated/lost source IDs after commit/checkpoint failure; every engine reads the same pinned corpus and respects protected versions. |
| **P2 — Accelerators / specialists** | Dense clustering/encoding/reranking can be offloaded; the platform still pays transfer and I/O costs. | GPU/NPU offload only for profiled expensive stages; evaluate added algorithms against the same recall/cost envelope. | End-to-end build or query economics improve, including transfer and fallback, rather than only a distance-kernel benchmark. |

Foundation references: [distributed APIs](https://lance.org/guide/distributed_indexing/), [local Aura design](aura-distributed-indexing.md), [local metrics guide](../docs/src/guide/observability.md), [local RaBitQ implementation](../rust/lance-index/src/vector/bq.rs), [integrations](https://lance.org/integrations/), [existing Ascend investigation](lance-clustering-npu-ascend-research.md).

**Highest-impact product improvement:** a documented, observable, recoverable global mining job that ingests at the required rate and returns a versioned top-1M selection at measured recall. This combines several existing capabilities into a result users can rely on. Additional distance kernels are secondary until profiling establishes that they constrain this result.

## 10. Recommended deployment experiments

### A. Lance + Aura as the mining platform

```text
Source batches / stream (stable source IDs, model version)
|-- Durable ingestion + replay / deduplication contract
|-- Lance data in object storage (original vectors, metadata, blob references)
|   |-- Versioned manifests and protected source snapshots
|   |-- Bounded distributed vector-index segments
|   `-- Incremental build / merge / coverage publication
`-- Aura distributed query job
    |-- Pin corpus version and index coverage
    |-- Search every segment, or validate ANN pruning at global recall
    |-- Score / refine with bounded budgets and locality-aware reads
    |-- Hierarchical top-k merge and durable result selection
    `-- SQL / Ray / training readers fetch only selected payloads
```

The 100–250M experimental build units from Section 3 are starting hypotheses. Determine the actual size from RAM, build/retry duration, I/O, and skew. Preserve enough original data and model metadata to reproduce scores and rebuild indices. Keep query and build placement policies independent of the durable segment layout.

### B. Milvus as the distributed retrieval layer

Run both **managed collection** and **external Lance collection** experiments where feasible. Compare compressed IVF with DiskANN/disk alternatives at fixed recall. Use SearchIterator for the required result depth. Measure the complete source→refresh/import→indexed→queryable path, and retain original lake snapshots while any search job depends on them.

The external path can avoid duplicating authoritative source rows, but it still stores Milvus indices and caches and creates a second metadata/maintenance lifecycle. Treat this as an integration to validate, not “zero cost” or “zero I/O.”

### C. Qdrant as an alternative or complementary serving layer

Use a version-pinned HNSW/quantization configuration, explicit memory placement, and a sufficient initial shard plan. Measure OSS and commercial operational features separately. Test the full corpus without tenant-only shortcuts. If Qdrant's application-serving advantages matter but global large-k is costly, keep the full mining job in Lance/Aura and expose a workload-defined serving subset or derived collection in Qdrant.

### D. PostgreSQL for transactional/control work

Keep PostgreSQL where joins, transactions, and governance metadata are central. A pgvector serving subset can share those semantics. A full-corpus distributed-PostgreSQL prototype should proceed only if there is a strong organizational reason to fund the additional sharding and global-execution design.

## 11. Qualification plan and decision gates

### Test matrix

| Dimension | Required cases |
|---|---|
| Corpus size | 100M and 1B for development; 20B initial acceptance; growth checkpoints at 40B/60B/80B; **100B final acceptance**. A 10B checkpoint is useful but cannot substitute for 20B/100B. |
| Data | Real, representative 1,024-d embeddings; preserve duplicates/skew observed in production. Do not manufacture scale solely by repeating a small dataset. |
| Query | Full-corpus top-10K, top-100K, top-1M; normalized metric and stable tie handling; query vectors excluded from training/tuning where appropriate. |
| Recall | Separate **0.95 and 0.99** operating points at each k; report aggregate and per-query distributions plus cohorts for old/new data and difficult queries. |
| Load | Concurrency **1/4/16**; first quiescent, then with **5B/day append plus index maintenance**; include bursts and backlog recovery. |
| Cache | Warm, cold, restarted worker, and rebalanced/resharded data; record object/NVMe traffic and effective bandwidth. |
| Delivery | IDs/scores first; complete selection materialization next; original vectors/media as separately measured payload paths. |
| Failures | Build worker/coordinator loss, query-worker loss, object-store transient faults, replica/node loss, interrupted compaction, and client retry during commit/export. |
| Freshness | Explicit corpus cutoff, durable/visible/indexed timestamps, delete/update behavior if those operations are added. |

**Reference correctness:** compute exact full-corpus top-k for a bounded, fixed set of held-out queries using exhaustive sharded FP32 scoring and an exact global merge. Batch reference queries where useful to amortize reads. At 100B this is expensive but necessary to substantiate global recall; ground truth only over a sampled subcorpus cannot do so. Another ANN index is not exact ground truth. For ties/duplicates, use a documented identity or distance-tolerance convention consistently.

**Decision gates:**

1. **Quality:** reach 95% and characterize the additional cost to reach 99%, for each required k and data-age cohort. Reject tuning that silently omits fresh data or failed shards.
2. **Sustained maintenance:** index at least the average daily input over several days, under query load. Track backlog, temporary space, compaction debt, and catch-up time after failure. Also run the complete 20B→100B growth sequence before production sign-off.
3. **Latency and throughput:** report p50/p95/p99, time to first batch, time to final IDs, and complete export time. Compare 60/180/300-second bands; agree the actual SLO before procurement or launch. Closed-loop concurrency results must include completed queries/minute and queueing behavior.
4. **Resources and recovery:** remain within declared RAM/NVMe/object/network budgets; recover without missing or duplicate source IDs, corrupted indices, or silently incomplete queries. Restore and rebuild are different recovery paths and need separate timings.
5. **Economics:** choose the lowest complete cost that passes the same quality, freshness, latency, and recovery contract. Include engineering cost if building the service layer, and commercial services if relying on their operations features.

The first practical milestone should be a **20B full-corpus large-k test with concurrent daily ingestion**, using Lance + Aura and Milvus with the same source data. It matches the initial production scale and exposes the main integration risks before the 16-day growth run. Qdrant can then be retained or eliminated on measured large-k and maintenance economics; PostgreSQL should remain a control/subset option unless a distributed design is explicitly proposed.

## 12. Research baseline and source interpretation

| Component | Baseline used | Boundary |
|---|---|---|
| Lance | Released **14.0.2**; local source inspected at **3737eb20ef132c3287583c7a775441fb8ddf80b5** | Local-source observations are not assumed to be in the release. Pin the SDK, connector, table/index features, and file version separately. [Release](https://github.com/lance-format/lance/releases/tag/v14.0.2). |
| Lance data files | **2.2** as the stable evaluation baseline | **2.3 is marked unstable** in the inspected format guide; exclude it from a production compatibility commitment. [Local format guide](../docs/src/format/file/versioning.md). |
| Milvus | **3.0.5**, with current 3.0 documentation | Storage V3/external collections require explicit enablement and qualification; deployment mode matters. [Release](https://github.com/milvus-io/milvus/releases/tag/v3.0.5). |
| Qdrant | **1.20.0** | Current docs can include later additions; the baseline credits `turbo8`/`turbo16`, not an unverified later storage datatype. Cloud-only operations are identified separately. [Release](https://github.com/qdrant/qdrant/releases/tag/v1.20.0). |
| PostgreSQL + pgvector | **PostgreSQL 18 documentation + pgvector 0.8.7** | pgvector's master changelog lists 0.9.0 as unreleased; do not credit its asynchronous HNSW I/O to 0.8.7. [Changelog](https://github.com/pgvector/pgvector/blob/master/CHANGELOG.md); [PostgreSQL 18](https://www.postgresql.org/docs/18/). |

Sources throughout are upstream documentation, source/release pages, and producer-published technical/customer reports, accessed on the research date. **Documented behavior**, **published measurements/claims**, **calculated sizing**, and **proposed engineering gates** are different evidence classes. Architectural recommendations and risk assessments are this report's analysis. Product documentation changes over time; revalidate the pinned versions and critical limits when implementing the bake-off.
