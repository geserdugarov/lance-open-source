# AI DataLake: Lance 10.0.0 to 14.0.2 and the path to 100 billion vectors

**Research date:** 6 November 2026. **Workload:** 100 billion vectors, 1,024 dimensions. **Sizing assumption:** Float32 source embeddings; decimal TB/GB throughout. **Production format baseline:** explicitly pinned Lance file format `2.2`. **Deliverables:** [one-slide PowerPoint](aidatalake-lance-10-to-14-upgrade.pptx) and [HTML review](aidatalake-lance-10-to-14-upgrade.html).

This is a source-based engineering assessment, not a performance certification. Release dates and PR states were checked against GitHub; implementation boundaries were checked against the `v10.0.0` and `v14.0.2` tags. Public product claims, upstream measurements, calculations, and proposed acceptance criteria are distinguished below. The previous AI DataLake deployment's Lance 10.0 baseline is supplied by the user and is consistent with the [local Aura integration plan](aura-distributed-indexing.md); its deployed binaries were not inspected.

## 1. Recommendation

**Upgrade the integration baseline to Lance 14.0.2 while retaining file format 2.2. Prioritize distributed index maintenance, bounded execution, and selective retrieval. Evaluate the v15 improvements in a separate qualification track.** This is a useful upgrade for AI DataLake, but upgrading a library alone does not establish a working 100-billion-vector service.

The highest-value released changes are:

1. **Distributed `optimize_indices` in v14**, combined with v12's improved IVF partition split/join and training balance. These address the continuing cost of keeping a very large, changing index usable.
2. **Shared partition scans for batched ANN in v12, Java batch bindings in v13, and `search_effort`/larger-k auto-probing in v14.** These improve retrieval controls and amortize work where the supported execution path applies.
3. **Lower read amplification and memory pressure:** bounded shuffle/rebalancing reads, late materialization, projected-file opening, and better blob read budgeting. They matter when even one unnecessary full vector pass means hundreds of terabytes.
4. **Multimodal lifecycle improvements:** managed blob reuse, stronger update/compaction behavior, and opt-in stable spilled row lineage. These help recurring embedding jobs and long-lived datasets, subject to cross-engine capability checks.
5. **BM25F and filtering improvements** for multimodal dataset curation and hybrid retrieval. Experimental MinHash is useful for a separate text-deduplication evaluation.

For the next release, the strongest candidates are **bounded segment consolidation**, **quantized refinement that avoids original-vector reads**, **batched exact-refinement I/O reuse**, and **prefilter-based skipping of empty segments**. These are merged into the development line. Portable blob/snapshot references and adaptive shared-scan batches remain open work. See the precise status and limitations in section 5.

**Capacity anchor:** FP32 source vectors occupy **409.6 TB**. Packed five-bit RaBitQ codes alone occupy **64 TB**. Their sum is **473.6 TB before IDs, quantizer factors, metadata, media, retained versions, temporary build files, caches, or replication**. The solution must stream from OBS, maintain bounded segments, and control query fanout and refinement I/O.

**A material qualification item:** open [issue #10778](https://github.com/lance-format/lance/issues/10778) and [PR #10779](https://github.com/lance-format/lance/pull/10779) identify probabilistic pruning that can make multi-bit RQ results depend on k or batch execution order. The proposed fix removes that pruning and may slow searches. Do not freeze the RQ performance target before comparing the qualified behavior at matched recall. The reported reproduction is on upstream `main`; this research did not reproduce it on the AI DataLake deployment or v14.0.2.

## 2. AI DataLake architecture: what is confirmed and what needs qualification

Huawei describes AI DataLake as a separation of storage and compute, with Aura, Ray, Spark, and Flink above open lake formats and OBS. It advertises a single table with 100-billion-scale, thousand-dimensional vectors; top-10,000 retrieval in seconds; top-million retrieval in minutes; and building 10-billion-scale indices in hours. These are **vendor claims**, without sufficient hardware, recall, data-distribution, concurrency, or cache-state detail to use as acceptance results for this workload. [Product page](https://www.huaweicloud.com/product/aidatalake.html), [vector-search scenario](https://support.huaweicloud.com/productdesc-aidatalake/aidatalake_08_0032.html).

The 26.9.0 release bulletin lists **Aura 3.9.0, Ray 2.54.0, Spark 4.0.2, and Flink 1.20**. It also documents Aura job continuation, UDF fusion, and large-object reference passing. Those are existing service capabilities to reuse. [Release bulletin](https://support.huaweicloud.com/verspt-bulletin-aidatalake/aidatalake_bulletin_0003.html).

### 2.1 Data layer

| Component | Verified role | Implication for this solution |
|---|---|---|
| **Lance** | Versioned tables with native vector, scalar and text indices, selective row/column access, and blob support. | Primary candidate for the 100B embedding/media layer. The external engine still supplies distributed scheduling and service-level query execution. [Distributed-indexing contract](https://github.com/lance-format/lance/blob/v14.0.2/docs/src/guide/distributed_indexing.md). |
| **Paimon** | Upstream documents native vector and full-text search, including IVF families and a dedicated vector-index implementation. | The user's claim that Paimon can search vectors is supported. Verify the exact Paimon version, index backend, and AI DataLake integration before treating its capabilities as deployed service features. [Search APIs](https://paimon.apache.org/docs/master/pypaimon/multimodal-search/), [vector-index guide](https://paimon.apache.org/docs/vector-index/). |
| **Iceberg** | An open table format with snapshots, evolution, and extensible statistics/index storage through Puffin. Its reviewed core specifications do not define a standard native ANN query implementation. | Keep structured tables and analytical joins here where appropriate. An engine can add ANN or external index support; saying Iceberg can never support vector search is too broad. Iceberg deletion vectors are deletion bitmaps, not embedding indices. [Table specification](https://iceberg.apache.org/spec/), [Puffin specification](https://iceberg.apache.org/puffin-spec/). |

The reviewed Huawei 26.9.0 bulletin explicitly lists Lance and Iceberg formats but does not establish the user's Paimon deployment details. This report treats Paimon as part of the supplied architecture, rather than inventing a supported Huawei version or engine matrix.

### 2.2 Engine layer

| Engine | Main responsibility in AI DataLake | Work that belongs above Lance |
|---|---|---|
| **Aura** | Multimodal SQL/DataFrame pipelines, embedding orchestration, hybrid retrieval, large-result mining. | Distributed scan/search planning, filter pushdown, global top-k, candidate exchange, checkpointed index jobs, lazy media handling, workload admission. |
| **Ray** | Distributed Python/AI processing, GPU/NPU scheduling, embedding generation and backfill. | Model loading/batching, device placement, data-loader efficiency, retries, distributed task execution, snapshot-pinned training selections. |
| **Spark** | Batch SQL/ETL, joins, large scans and scheduled maintenance. | Certified connector/JNI integration and distributed job execution. |
| **Flink** | Stateful streaming, CDC, continuous enrichment, source-progress tracking. | Checkpoint-aware publication, replay/idempotency, update/delete semantics, and write-to-indexed freshness. |

These are workload emphases, not hard modality boundaries. Aura also uses heterogeneous compute. Spark can read binary media, and Flink can invoke external services through asynchronous I/O. They are not intrinsically limited to non-multimodal data. [Huawei product description](https://www.huaweicloud.com/product/aidatalake.html), [Spark binary source](https://spark.apache.org/docs/latest/sql-data-sources-binaryFile.html), [Flink async I/O](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/asyncio/).

**NPU boundary:** running a Ray task on an Ascend NPU does not make Lance's Rust ANN kernels run on that NPU. The tagged Lance accelerator helper validates CUDA/MPS devices; an Ascend/CANN backend requires separate integration. Use Ray/Aura for NPU embedding generation now, and qualify NPU assignment, quantization, and reranking independently. [v14.0.2 accelerator implementation](https://github.com/lance-format/lance/blob/v14.0.2/python/python/lance/vector.py), [local Ascend assessment](lance-clustering-npu-ascend-research.md).

## 3. Correct release baseline

| Release | Publication date, UTC | Relevant role |
|---|---|---|
| [10.0.0](https://github.com/lance-format/lance/releases/tag/v10.0.0) | 2026-08-07 | Comparison baseline; many important foundations already exist. |
| [11.0.0](https://github.com/lance-format/lance/releases/tag/v11.0.0) | 2026-08-30 | Cache extensibility, zone maps, Java controls/metrics, correctness fixes. |
| [12.0.0](https://github.com/lance-format/lance/releases/tag/v12.0.0) | 2026-09-17 | Shared batch IVF scans, Java segment search, improved IVF training and rebalancing. |
| [13.0.0](https://github.com/lance-format/lance/releases/tag/v13.0.0) | 2026-10-07 | BM25F, Java batch search, memory/read improvements, initial lineage and experimental deduplication work. |
| [14.0.0](https://github.com/lance-format/lance/releases/tag/v14.0.0) | 2026-10-26 | Distributed optimization, search effort, stable opt-in lineage spill, managed blobs, late materialization. |
| [14.0.1](https://github.com/lance-format/lance/releases/tag/v14.0.1) | 2026-10-28 | Removes a dependency on an unreleased `object_store` version. |
| [14.0.2](https://github.com/lance-format/lance/releases/tag/v14.0.2) | 2026-10-29 | Fixes sliced Arrow validity alignment; latest stable release observed. |
| [14.0.3-rc.1](https://github.com/lance-format/lance/releases/tag/v14.0.3-rc.1) | 2026-11-02 | Prerelease blob-promotion fixes; not stable at the research cutoff. |
| [15.0.0-beta.11](https://github.com/lance-format/lance/releases/tag/v15.0.0-beta.11) | 2026-11-05 | Latest beta observed; includes further ANN, maintenance and observability work. |

### What is already present in v10

Do not count the following as newly invented by v11–v14:

- **Multi-bit RaBitQ:** v10 source accepts **1–9 bits**, has split-code scoring, reusable rotation state, and `approx_mode`. Its Python docstring says the default is one bit, but the core's defaults and Huawei's wrapper are separate contracts; set the intended bit count explicitly. [v10 RQ parameters](https://github.com/lance-format/lance/blob/v10.0.0/rust/lance-index/src/vector/bq.rs), [v10 scanner/builder APIs](https://github.com/lance-format/lance/blob/v10.0.0/python/python/lance/dataset.py).
- **Distributed initial index construction and multiple index segments.** The major new maintenance facility is distributed optimization. [v10 distributed guide](https://github.com/lance-format/lance/blob/v10.0.0/docs/src/guide/distributed_indexing.md).
- **Batch vector-search APIs.** Sharing physical partition scans across the batch is a subsequent execution improvement. [Shared-batch PR #7640](https://github.com/lance-format/lance/pull/7640).
- **Blob v2, partial blob reads, versioning/tags, and streaming coreset training.** The upgrade improves functionality, memory use and correctness around these foundations. [v10 blob guide](https://github.com/lance-format/lance/blob/v10.0.0/docs/src/guide/blob.md), [v10 dataset implementation](https://github.com/lance-format/lance/blob/v10.0.0/python/python/lance/dataset.py).
- **File format 2.2.** It is already documented at v10; this is not a file-format migration from “10” to “14.” [v10 format versions](https://github.com/lance-format/lance/blob/v10.0.0/docs/src/format/file/versioning.md).

Huawei's detailed Aura index guide currently describes `ivfrq` as one-bit only, while its newer scenario description mentions multi-bit quantization. This documentation discrepancy should be resolved against the actual Aura build and SQL parameter handling. Upgrading upstream Lance does not automatically expose every upstream option through Aura. [Aura index parameters](https://support.huaweicloud.com/devg-aidatalake/aidatalake_05_0047.html), [scenario description](https://support.huaweicloud.com/productdesc-aidatalake/aidatalake_08_0032.html).

## 4. Released changes that matter most

**Priority:** P0 = necessary to qualify a 100B service; P1 = strong workflow improvement; P2 = targeted experiment. Release labels identify the first stable release containing the listed change, verified using tagged history.

| Priority / capability | Change after v10 | Benefit to AI DataLake | Important boundary |
|---|---|---|---|
| **P0 — Distributed maintenance** | v14: plan → execute → merge → commit for `optimize_indices`, with task/segment row controls. | Spread append catch-up and eligible merge/rebuild work across Aura/Ray/Spark workers; prevent an unbounded unindexed tail. | Scheduler and recovery remain external. Some training occurs in the planner. Partial results can leave coverage unindexed. [#9871](https://github.com/lance-format/lance/pull/9871), [tagged implementation](https://github.com/lance-format/lance/blob/v14.0.2/rust/lance/src/index/optimize.rs). |
| **P0 — Better IVF partition balance** | v12: proportional hierarchical k-means; target-driven multiway split and multi-partition join in one optimize pass. | Reduce skewed partitions and repeated raw-vector reads during maintenance. | Refinement adds training cost. Split/join does not eliminate the need to retrain under distribution drift. [#9050](https://github.com/lance-format/lance/pull/9050), [#9051](https://github.com/lance-format/lance/pull/9051). |
| **P0 — Controlled retrieval effort** | v13: dot-product calibration; v14: `search_effort` and calibrated auto-probing through k = 100,000. | A common control for small-k retrieval and larger dataset selections; explicit probe ceilings help admission control. | Routing effort is not a recall guarantee. k = 1,000,000 is outside the calibrated range. [#9467](https://github.com/lance-format/lance/pull/9467), [#9798](https://github.com/lance-format/lance/pull/9798). |
| **P0 — Shared batch ANN execution** | v12: load each overlapping IVF partition once for multiple queries; Java segment search in v12 and batch binding in v13. | Amortize object reads, decoding and prefilter work for evaluation/mining batches. | Stable fast path is restricted; adaptive probing, refinement, HNSW and incomplete coverage can fall back to per-query execution. [#7640](https://github.com/lance-format/lance/pull/7640), [#7169](https://github.com/lance-format/lance/pull/7169), [#9475](https://github.com/lance-format/lance/pull/9475). |
| **P0 — Bounded memory and less I/O** | v12–v14: blob read budgets, shuffle offset budget, streamed partition work, faster shuffle, bounded merge prefetch; late materialization and projected-file opening in v14. | Keep workers alive and avoid fetching unrelated media or embedding columns during filtering and metadata operations. | A per-operation budget is not a process-wide memory cap. Aura must account for concurrent tasks, caches, decoding and output. [#9434](https://github.com/lance-format/lance/pull/9434), [#9761](https://github.com/lance-format/lance/pull/9761), [#9750](https://github.com/lance-format/lance/pull/9750), [#10178](https://github.com/lance-format/lance/pull/10178). |
| **P0 — RQ correctness and build efficiency** | v11 cosine-factor fix; v12 wider FastScan accumulation above 1,024 dimensions; v13 dot-scale fix; v14 bounded scratch/fused rotation and encoding. | Better metric correctness and less CPU/allocation overhead when building compressed indices. | These are improvements to existing multi-bit RQ. Validate the open pruning issue separately; the >1,024-d fix is not itself a claimed speedup for exactly 1,024 dimensions. [#8393](https://github.com/lance-format/lance/pull/8393), [#8842](https://github.com/lance-format/lance/pull/8842), [#9452](https://github.com/lance-format/lance/pull/9452), [#9465](https://github.com/lance-format/lance/pull/9465), [#9466](https://github.com/lance-format/lance/pull/9466). |
| **P1 — Multimodal reuse** | v11–v13 blob API/budget/buffering improvements; v14 reusable managed payloads and descriptors, with explicit capability opt-in. | Re-embed or curate data while preserving heavy media objects; support deferred range reads. | All cleanup clients must understand ownership. External media immutability/lifetime remains the application's responsibility. [Blob guide](https://github.com/lance-format/lance/blob/v14.0.2/docs/src/guide/blob.md), [#9293](https://github.com/lance-format/lance/pull/9293), [#9891](https://github.com/lance-format/lance/pull/9891), [#10176](https://github.com/lance-format/lance/pull/10176). |
| **P1 — Metadata scalability** | v13 groundwork and v14 stable spilled row lineage; demand-loaded row-ID structures and manifest-related performance work. | Reduce inline lineage growth and unnecessary metadata work on large, frequently rewritten tables. | Spill is off by default; the default inline budget is 200 KiB. It does not make every part of the manifest constant-sized or supply a CDC service. [#9802](https://github.com/lance-format/lance/pull/9802), [#10070](https://github.com/lance-format/lance/pull/10070). |
| **P1 — Better training inputs** | v14 public IVF training from caller-supplied samples; v12 balanced hierarchical training. | Aura/Ray can collect representative samples and reuse the core validation/training path. | Supplied-sample training is an in-memory entry point, not distributed centroid training. [#10188](https://github.com/lance-format/lance/pull/10188). |
| **P1 — Hybrid retrieval** | v11 wider zone-map support; v13 BM25F with Python/Java APIs; v14 FTS cold-read and memory improvements. | Search captions/OCR/transcripts with field weights, then combine with vector and structured filters. | BM25F is field-aware lexical scoring. Global vector/text fusion and cross-shard ranking remain engine responsibilities. [#8017](https://github.com/lance-format/lance/pull/8017), [#7905](https://github.com/lance-format/lance/pull/7905), [#10198](https://github.com/lance-format/lance/pull/10198). |
| **P1 — Cache and operations integration** | v11 pluggable cache backends, Java scan/memory options and OpenTelemetry bridge; v14 additional cache inspection and I/O controls. | Integrate Lance with AI DataLake's cache placement, worker budgets and metrics. | Extensible cache APIs are not a delivered distributed RAM/SSD cache service. [#7683](https://github.com/lance-format/lance/pull/7683), [#8288](https://github.com/lance-format/lance/pull/8288), [#8064](https://github.com/lance-format/lance/pull/8064), [#9833](https://github.com/lance-format/lance/pull/9833). |
| **P2 — Curation experiments** | v13 MinHash LSH, with bindings in v14; streaming duplicate-pair work; v14 experimental PCA model/pre-transform foundation. | Text near-duplicate removal, embedding deduplication, potential lower-dimensional retrieval. | MinHash format is experimental. PCA foundation is not the completed IVF_RQ integration. [MinHash contract](https://github.com/lance-format/lance/blob/v14.0.2/docs/src/format/index/scalar/minhash_lsh.md), [#9493](https://github.com/lance-format/lance/pull/9493), [#9967](https://github.com/lance-format/lance/pull/9967). |

### 4.1 Why distributed maintenance is the first integration priority

Initial construction is only one phase. Continuous ingestion produces unindexed fragments; updates/deletes leave obsolete entries; many small segments increase search work; changing embeddings require retraining or replacement. A sustainable service must make indexed throughput exceed the sustained ingest rate while allocating resources to queries.

The v14 implementation exposes serializable work units anchored to a read version. Persist the plan, model identity, fragment coverage and completed artifact metadata in Aura's existing job-recovery system. Workers should exchange metadata and write durable index artifacts to shared storage. Publish a complete, validated result through the designated commit authority. Monitor coverage explicitly because the core permits partial-result publication. [Implementation](https://github.com/lance-format/lance/blob/v14.0.2/rust/lance/src/index/optimize.rs).

The source also exposes important limits:

- Retraining for a split plan can execute in the planner; distributing encoding does not distribute every training step.
- Fragment/task row limits can be exceeded by an indivisible oversized input. Choose data-fragment sizes before relying on those limits for memory planning.
- The split workflow excludes particular combinations, including tagged fragment-reuse history and legacy vector indices. An explicitly named unsupported index errors; otherwise work can be skipped with a warning.
- Split construction does not perform every single-process partition-rebalancing operation. Qualify the exact maintenance mode rather than assuming all knobs compose.

These limits should be reflected in an Aura maintenance capability matrix and job-status reporting. They are especially relevant if physical clustering or deferred index remapping is enabled later.

### 4.2 Why the I/O path can matter more than a faster distance kernel

For top-million retrieval, exact refinement with factor 10 can fetch **10 million × 4,096 bytes = 40.96 GB** of source vectors for one query, even before range-read amplification. Returning one million IDs and FP32 scores requires only about **12 MB** of logical payload. Fetching full embeddings, images or videos too early can dominate the job.

Carry IDs, scores, predicates and blob descriptors through selection and joins. Fetch projected metadata next; materialize media after deduplication/sampling. Group source-vector reads by fragment/range where semantics allow. Use the v14 late-materialization and projection improvements as building blocks, while verifying that Aura's physical operators preserve their benefit. [Late materialization #9750](https://github.com/lance-format/lance/pull/9750), [projected files #10178](https://github.com/lance-format/lance/pull/10178).

### 4.3 Correctness fixes are part of the upgrade value

At this scale, a silent retrieval error is more expensive than a visible job failure. The release range fixes stale segment rows entering top-k, multi-vector distance aggregation, indexed-filter candidate preservation, and Arrow validity alignment. The last is the specific v14.0.2 patch. Include these behaviors in cross-engine regression fixtures with nulls, deletes, updates and multiple fragments. [#8351](https://github.com/lance-format/lance/pull/8351), [#8710](https://github.com/lance-format/lance/pull/8710), [#9789](https://github.com/lance-format/lance/pull/9789), [#10412](https://github.com/lance-format/lance/pull/10412).

### 4.4 Streaming writes: substantial MemWAL work, still experimental

The release range also develops the MemTable/WAL path: v11 exposes flush/backpressure statistics and index catch-up progress; v12 adds injectable write backpressure and index-memory accounting; v13 improves fresh-tier filtering/text search; v14 adds live schema transitions, pluggable maintained indices, blob-write improvements and recovery fixes. This is relevant to continuous ingestion and fresh searchable data. [Backpressure #7831](https://github.com/lance-format/lance/pull/7831), [live schema changes #9712](https://github.com/lance-format/lance/pull/9712), [index plugins #9803](https://github.com/lance-format/lance/pull/9803), [blob writes #9717](https://github.com/lance-format/lance/pull/9717).

However, the **v14.0.2 MemWAL specification is explicitly experimental**. Keep it in a separate streaming prototype, not a dependency of the initial format-2.2 production rollout. Its shard model requires one active writer epoch per shard; primary-key updates must route a given key consistently to the same shard. WAL recovery, source checkpoint coordination, shard ownership and fresh/base-tier read semantics need a complete service implementation and failure testing. These primitives do not establish a replicated ingestion service or Flink exactly-once behavior. [Tagged MemWAL contract](https://github.com/lance-format/lance/blob/v14.0.2/docs/src/format/table/mem_wal.md).

## 5. Near-term upstream work

**“Merged / beta” means available in the development line, not stable production support. “Open” and “draft” have no verified merge date.** Inclusion in a recent beta is stronger evidence of near-term delivery than an active PR. None of the open features below is assumed in the production baseline.

| Priority / feature | Verified status at cutoff | Expected AI DataLake benefit | Qualification / delivery boundary |
|---|---|---|---|
| **P0 — Bounded segment consolidation** | [#10335](https://github.com/lance-format/lance/pull/10335) merged 27 Oct; Java [#10755](https://github.com/lance-format/lance/pull/10755) merged 5 Nov and in beta. Python [#10762](https://github.com/lance-format/lance/pull/10762) open. | Limit maintenance work to a budgeted segment suffix without retraining the complete index. Shared models can merge directly; other selected models are re-encoded. | Recall may change when re-encoding against a different model. Requires complete surviving coverage and has clone/FRI/legacy limitations. It does not catch up unindexed data. |
| **P0 — Quantized refinement** | [#8741](https://github.com/lance-format/lance/pull/8741) merged 1 Nov; v15 development line. | Overfetch using fast RQ scoring, then rerank with stored multi-bit codes; avoid original-vector reads for indexed candidates when no exact-refinement stage is requested. | Opt-in, initially flat IVF_RQ. Distances remain quantized estimates. Query projections, unindexed tails, or optional exact refinement can still require base reads. Validate together with #10779. |
| **P0 — Shared exact-refinement reads** | [#9353](https://github.com/lance-format/lance/pull/9353) merged 2 Nov; v15 development line. | Deduplicate overlapping candidate-vector reads across query batches. | Fixed probes and supported flat-style IVF only; several cases retain fallback. Benefits depend on candidate overlap. |
| **P0 — Skip segments excluded by a prefilter** | [#10485](https://github.com/lance-format/lance/pull/10485) merged 30 Oct; v15 development line. | Avoid ANN/FTS scoring when a segment has no selectable rows, useful for tenant/time/class filters. | In ANN the change preserves initial routing and loading; it is not complete remote-I/O pruning. Custom RQ sign stages can still run. |
| **P0 — Multi-bit RQ pruning correction** | [#10779](https://github.com/lance-format/lance/pull/10779) open; updated 5 Nov, following [#10778](https://github.com/lance-format/lance/issues/10778). | Improve consistency of reported multi-bit candidates across k and execution order. | Proposed removal of probabilistic pruning can increase CPU and ex-plane I/O. Reproduce on the target version; benchmark the accepted fix before rollout. No release ETA verified. |
| **P1 — Calibrated effort tiers** | [#10695](https://github.com/lance-format/lance/pull/10695) merged 5 Nov; beta. | More interpretable routing-effort tiers by metric and k, useful for cost/quality policies. | Empirical mean routing targets, not per-query or final recall guarantees. Calibration still ends at k = 100,000. |
| **P1 — IVF_HNSW_RQ** | [#10522](https://github.com/lance-format/lance/pull/10522) merged 5 Nov; beta. | Graph search within IVF partitions plus compressed RQ storage; attractive for small-k serving. | Adds graph/resident-memory and build costs. New vector-index version 3 is skipped by older readers, causing flat-search fallback. No 100B or top-million result is established. |
| **P1 — Process memory visibility** | [#10451](https://github.com/lance-format/lance/pull/10451) merged 5 Nov; beta. Runtime cache resizing [#10657](https://github.com/lance-format/lance/pull/10657) open. | Observe RSS/cgroup limits and stage peaks; later allow cache budgets to track elastic workloads. | Metrics are opt-in and have operation-coverage limits; they do not enforce a total memory budget or prevent OOM. |
| **P1 — Portable blob and exact-snapshot references** | [#10266](https://github.com/lance-format/lance/pull/10266), bindings/retention [#10336](https://github.com/lance-format/lance/pull/10336), nested/conformance [#10407](https://github.com/lance-format/lance/pull/10407) open. | Carry lazy media references across SQL exchange, spill and retries while tying reads to an exact snapshot. | A multi-PR integration stack. A reference alone does not retain files; cleanup clients must participate in the retention protocol. Worker manifests are still loaded. |
| **P1 — Adaptive shared-scan batches** | [#10443](https://github.com/lance-format/lance/pull/10443) open; updated 5 Nov. | Extend partition sharing to adaptive IVF_FLAT/PQ/SQ requests instead of falling back to a loop. | Adaptive RQ and adaptive exact refinement retain fallback in the described change. Do not apply its benefit to every batch query. |
| **P1 — Selected old-segment consolidation** | [#10698](https://github.com/lance-format/lance/pull/10698) open. | Reclaim stale entries in older segments on high-churn tables, beyond newest-suffix maintenance. | Caller-selected work has no consolidation row budget; selection must itself be bounded. Re-encoding can alter recall. |
| **P2 — Clustering provider integration** | Metadata runtime [#10383](https://github.com/lance-format/lance/pull/10383), output properties [#10409](https://github.com/lance-format/lance/pull/10409), explicit groups [#10445](https://github.com/lance-format/lance/pull/10445) merged 28–29 Oct. Run fencing [#10630](https://github.com/lance-format/lance/pull/10630) draft. | Enable an external provider to improve predicate skipping and source-vector/media locality through physical layout. | These are metadata/compaction interfaces, not a complete clustering algorithm or scheduler. Base-row clustering is different from building an IVF index. |
| **P2 — PCA + IVF_RQ** | Foundation in v14; index integration [#9970](https://github.com/lance-format/lance/pull/9970) open. | Potentially reduce code size and scoring work for compressible embedding distributions. | Requires measured recall in the original metric and model-version management; no universal 1,024→256 quality guarantee. |
| **P2 — Reuse coarse routing across segments** | [#8966](https://github.com/lance-format/lance/pull/8966) draft. | Rank identical shared centroids once across compatible segments. | Attractive at high segment counts, but draft status and pending validation do not support a delivery commitment. |
| **P2 — Float-vector lossless compression** | [#10618](https://github.com/lance-format/lance/pull/10618) open; targets format 2.3. | Potential base-vector storage savings, separate from lossy ANN quantization. | Opt-in, data-dependent compression and decode costs; **2.3 remains unstable**. Keep outside the format-2.2 production plan. |

The **14.0.3 RC** deserves a separate, smaller upgrade track if old blob pages are promoted during updates: it contains fixes for reading promoted legacy blob pages in 2.2 and for promotion through fragment/update writes. Exercise historical datasets before approving that path. [RC notes](https://github.com/lance-format/lance/releases/tag/v14.0.3-rc.1).

### Published measurements: what they do and do not establish

| Evidence | Reported result | Valid interpretation |
|---|---|---|
| Proportional hierarchical IVF training [#9050](https://github.com/lance-format/lance/pull/9050) | On 10M-row DEEP and text-to-image datasets, a roughly 1.5–1.6× faster tree and substantially better partition balance; dimensions 96/200. | Supports reducing partition skew. It is not a 100B × 1,024-d training result. |
| Batch refinement [#9353](https://github.com/lance-format/lance/pull/9353) | On 32,768 synthetic 1,024-d vectors, nearby-query batches improved about 1.8–2.3×; small independent batches were about 2% slower. | Overlap can reduce redundant reads. Do not advertise a universal batch speedup or infer OBS performance. |
| IVF_HNSW_RQ [#10522](https://github.com/lance-format/lance/pull/10522) | Author reports roughly 2.2–3.1× higher QPS at comparable recall for one-bit graph walking versus full multi-bit scoring on each hop, using 1M-vector cases. | Compares two graph-scoring strategies. It does not establish that HNSW_RQ beats every IVF alternative or that graphs are best for top-million mining. |
| Effort calibration [#10695](https://github.com/lance-format/lance/pull/10695) | Held-out routing results on several 10M–35M corpora; some low-effort queries become faster and others slower as routing quality changes. | A cost/quality policy improvement. Same effort is not the same as equal recall, and the default has documented misses of its nominal target. |

No reviewed evidence establishes a reproducible, matched-recall **100B × 1,024-d** SLA for the proposed release combination. Use these results to choose experiments, not to extrapolate a latency commitment.

## 6. Capacity and scaling analysis

### 6.1 Persistent payload

All rows below are arithmetic lower bounds. Quantized codes generally supplement the source vectors; they do not automatically replace them.

| Representation | Payload bytes/vector | Payload at 100B | Use / omitted costs |
|---|---:|---:|---|
| FP32 source | 4,096 | **409.6 TB** | Original metric/reference vectors; excludes media and scalar columns. |
| FP16 source | 2,048 | 204.8 TB | Alternative precision requiring quality qualification. |
| SQ8 codes | 1,024 | 102.4 TB | Additional index metadata/IDs omitted. |
| RQ5 packed codes | 640 | **64.0 TB** | Factors, row IDs, centroids, rotation state and layout overhead omitted. |
| RQ1 packed codes | 128 | 12.8 TB | Much stronger compression; different recall/refinement costs. |
| PQ128, 8-bit subcodes | 128 | 12.8 TB | 128 subspaces of 8 dimensions; codebooks and IDs omitted. |
| PQ256, 8-bit subcodes | 256 | 25.6 TB | 256 subspaces of 4 dimensions; test at matched recall. |
| One 64-bit ID | 8 | 0.8 TB | One component of metadata, not its total size. |

For a graph illustration, **32 four-byte neighbor references per vector add 12.8 TB** before other levels, headers or allocation overhead. This is not an exact Lance HNSW footprint. A graph's code compression ratio does not describe its complete memory demand.

Media can dominate these numbers: 100B independently stored 100-KB objects would be another **10 PB**. Do not assume one distinct media object per vector; define the mapping among source assets, chunks, frames and embeddings, and reuse descriptors where ownership permits.

### 6.2 Segments, workers and training are different sizing decisions

A useful **initial experiment**, not a hard recommendation, is 100M–250M rows per physical index segment:

| Segment rows | Segments for 100B | FP32 payload/segment | RQ5 code payload/segment |
|---|---:|---:|---:|
| 100M | 1,000 | 409.6 GB | 64 GB |
| 250M | 400 | 1,024 GB | 160 GB |

Segments need not be resident in RAM and do not imply one worker per segment. Tasks can be much smaller. Determine segment size from measured index-open cost, cache locality, merge cost, candidate fanout and recovery time. A 64-GB code payload does not make a 64-GB worker sufficient for its entire build.

There is also a centroid/training trap. If a single global IVF model targeted 4,096 rows per partition, it would need roughly **24.4 million centroids**, about **100 GB** of FP32 centroid coordinates. A conventional sample of 256 rows per centroid would occupy approximately **25.6 TB**. This calculation explains why a naïve global in-memory training job is unsuitable. Use representative bounded samples, streaming/hierarchical approaches, segment/model groups, or separately engineered distributed training. The supplied-sample API itself does not solve that memory problem.

Shared models simplify physical merging and may enable future routing reuse. Independent segment models make local training practical, but increase routing work and complicate consolidation. Record model identity and evaluate both choices. [Model-scope contract](https://github.com/lance-format/lance/blob/v14.0.2/docs/src/guide/distributed_indexing.md).

### 6.3 Data movement lower bounds

| Operation / assumption | Calculated lower bound | Consequence |
|---|---:|---|
| One complete FP32 pass at aggregate 100 GB/s | 4,096 s ≈ **68.3 min** | Raw scan only: no training, shuffle, output or contention included. |
| Ingest/index all 100B rows in 24 h | **1.157M vectors/s**, at least **4.74 GB/s** source payload | Set cluster throughput and catch-up objectives explicitly. |
| Optional local-plan scenario: 5B new rows/day | **57,870 vectors/s**, **20.48 TB/day** of FP32 | This is a scenario from the existing planning context, not a requirement inferred from “100B.” |
| Return 1M IDs + FP32 scores | **12 MB** | Excludes serialization, routing identifiers and output metadata. |
| Naïvely return 1M candidates from all 400–1,000 segments | **4.8–12 GB** | Candidate traffic can dwarf the final result. |
| Refine 10M original 1,024-d vectors | **40.96 GB/query** | Range-read amplification and repeated shard candidates can increase this. |

Use pipelined hierarchical merges, bounded candidate streams and valid pruning to control exchange. **Do not simply ask each of S segments for k/S rows:** the true global nearest neighbors may be concentrated in one segment. Any adaptive candidate allocation must preserve its stated retrieval-quality contract.

### 6.4 Metadata and index health at 100B

Track manifest bytes and cold-open time, number of fragments, segments per logical index, live/deleted index entries, index coverage, unindexed rows, pending remaps, and query-time fallback work. A table with 1M rows per data fragment would already have 100,000 fragments; at 100K rows it would have 1M fragments. These are examples illustrating why fragment sizing and metadata behavior need full-scale testing.

A newly committed batch can be visible before it is indexed. Define both **write-to-visible latency** and **write-to-indexed latency**. A large brute-force tail can break an otherwise good ANN SLA. Avoid a query mode that silently omits unindexed rows unless the application explicitly accepts that completeness tradeoff.

## 7. Proposed implementation for AI DataLake

```text
LakeFormation / catalog + version and model registry
|
+-- Aura / Ray ingestion and embedding jobs
|   +-- CPU decoding and preparation
|   +-- GPU / NPU model inference
|   +-- Bounded Arrow batches -> versioned Lance fragments on OBS
|
+-- Index maintenance coordinator
|   +-- Snapshot + fragment coverage + model identity
|   +-- Representative sampling / model training
|   +-- Plan -> distributed build -> bounded merge -> commit
|   +-- Durable checkpoints, retries, coverage and backlog metrics
|
+-- Aura query service
|   +-- Resolve exact snapshot and embedding model
|   +-- Apply tenant/time/scalar filters and eligible routing
|   +-- Parallel segment ANN / text retrieval
|   +-- Bounded global candidate merge and optional refinement
|   +-- Join, deduplicate, sample; fetch selected metadata/media
|
+-- OBS durable data and indices
    +-- File format 2.2, capability-controlled writes
    +-- Source vectors and media references
    +-- Compressed IVF segments and scalar/text indices
    +-- Near-compute RAM/SSD cache managed by the platform
```

This is a **proposed architecture assembled from the verified primitives**, not a claim that upgrading Lance automatically installs these services.

### 7.1 Index choices

Start with **IVF_PQ and IVF_RQ5 as competing compressed baselines**, retaining SQ8 as a higher-footprint quality baseline. Evaluate RQ3/5/8 where useful. Compare recall, bytes read, CPU, build throughput, cache footprint and total elapsed time; compression alone is insufficient. Retain original vectors for reference evaluation and selected exact refinement.

Use separate retrieval policies:

- **Small-k serving, k = 10–100:** evaluate graph-based indices, warm caches, batching deadlines and tail latency. Beta IVF_HNSW_RQ is a candidate, not the production default.
- **Dataset mining, k = 10K–1M:** prioritize streaming flat-style IVF scans, compressed scoring, bounded top-k, selective refinement and export throughput. Measure total completion time and output quality.
- **Selective predicates:** test scalar-first exact search against filtered ANN when the eligible set is small. At 0.1% selectivity, a 100B table still leaves 100M rows, so “selective” is not synonymous with small.
- **Hybrid curation:** produce lexical and vector candidate sets with explicit fusion/deduplication semantics. Record the corpus snapshot and embedding model used by each component.

ANN search over an index must use the same metric, normalization and embedding-model identity as the build. Re-embedding into a new model should use a new version/column and a staged index publication, rather than mixing incompatible spaces.

### 7.2 Maintenance policy

Run frequent bounded catch-up jobs and less frequent consolidation/rebalancing. Set independent budgets for training, encoding, local spill, object-store writes, merge and queries. Persist completed segment artifacts so worker failures do not restart the full build. Use compatible shared quantizer artifacts within groups intended for direct physical merging.

Retrain based on measured partition imbalance, recall drift and data-distribution changes. Keep the previous index available through qualification where the API and storage budget permit. Version retention protects rollback only if cleanup also preserves every referenced object.

Do not schedule a full 100B rewrite for routine catch-up. Conversely, do not allow thousands of small delta segments to accumulate indefinitely. Bounded consolidation in v15 is particularly useful for balancing those costs; the open selected-segment extension matters for older data with frequent updates.

### 7.3 Integration responsibilities and connector gaps

| Surface | Evidence / current foundation | Required AI DataLake work |
|---|---|---|
| **Aura** | Public guide already supports distributed vector creation, segment/global layouts and multiple index families. | Map the new core parameters into SQL/SDK plans; preserve snapshot and segment ownership through distributed execution; report fallback and coverage. [Aura guide](https://support.huaweicloud.com/devg-aidatalake/aidatalake_05_0047.html). |
| **Spark** | Connector v0.8.0 includes distributed index operations; its tagged POM pins Lance **12.0.0**. | Certify a connector/JNI build using the chosen Lance runtime; do not assume the latest connector already provides v14 behavior. [Release](https://github.com/lance-format/lance-spark/releases/tag/v0.8.0), [POM](https://github.com/lance-format/lance-spark/blob/v0.8.0/pom.xml). |
| **Ray** | Connector v0.5.0 adds distributed column addition, segment-owned search and distributed RQ builds. | Reuse it for embedding backfill and distributed tasks, then qualify versions, model distribution, recovery and memory budgets. [Release](https://github.com/lance-format/lance-ray/releases/tag/v0.5.0). |
| **Flink** | Connector v0.1.0 supports Flink 1.18/1.19/1.20 and pins Lance **7.0.0**. | Qualify upgraded bindings and actual streaming commit guarantees. A connector's existence is not evidence of checkpoint-consistent upserts/deletes or complete CDC. [Release](https://github.com/lance-format/lance-flink/releases/tag/v0.1.0). |
| **LakeFormation and cross-format joins** | Shared governance/catalog is part of the service architecture. | Bind stable business IDs and explicit source snapshot IDs across Lance/Iceberg/Paimon. A shared catalog does not provide an atomic transaction across all three tables. |

At runtime, observe partitions and segments touched, candidate counts, cache hit/miss and bytes, remote GET count, refinement bytes, queue time, stage memory, and unindexed fallback. Report scale-from-zero separately from warm-cache latency; a cached serverless service has different economics under cold and hot workloads.

## 8. Compatibility and rollout

**Library version, data-file version, table capabilities, vector-index version, and connector version are separate contracts.** File format 2.2 alone is not a complete mixed-client compatibility declaration. The tagged version guide explicitly warns that 2.3/`next` is unstable and documents a special all-null FixedSizeList reader caveat. [Format contract](https://github.com/lance-format/lance/blob/v14.0.2/docs/src/format/file/versioning.md).

Use the following rollout order:

1. **Inventory the actual deployment.** Record Aura build, Lance Rust/Python/Java versions, engine/connector versions, data-file versions, index versions, table flags, blob layouts, row-ID mode and cleanup owners.
2. **Upgrade and certify readers first.** Run each engine against historical fixtures and the same pinned snapshots, covering nested values, null vectors, deletes, tags and blobs. Verify that expected indices are actually used.
3. **Pin new writes to `2.2`.** Upgrading the library need not rewrite existing stable-format data. Rebuild indices only for a chosen algorithm/model change or measured benefit.
4. **Enable advanced table features separately.** Managed blobs and spilled lineage require capability-aware readers and maintenance clients. Test rollback boundaries before emitting new capabilities.
5. **Canary the distributed optimization integration.** Cover concurrent appends/deletes, worker failure, coordinator restart, duplicate retry outputs, stale plans, and partial coverage. Do not combine unsupported FRI/maintenance modes.
6. **Promote performance features after quality validation.** Compare v10 and v14 using both unchanged indices and rebuilt indices, so runtime benefits are distinguished from model/layout changes.
7. **Evaluate beta work on isolated datasets.** A v15 binary does not require using unstable format 2.3. IVF_HNSW_RQ's new index version still has a reader/fallback implication independent of the data-file version.

Do not make production data depend on experimental MemWAL, MinHash, PCA integration or format-2.3 encodings before their own acceptance gates. Keep the current stable source data recoverable; unstable experimental indices can be rebuilt.

## 9. Acceptance plan for a credible 100B solution

The workload still needs a chosen distance metric, recall requirement, query rate, latency objective, update/delete rate, filter distribution, precision and hardware budget. The values below are **proposed test targets**, not measured results or assumptions about a contracted SLA.

| Gate | Proposed experiment | Required evidence |
|---|---|---|
| **Quality** | Real 1,024-d embeddings; L2/cosine/dot as applicable; k = 10, 100, 10K, 100K, 1M. Start with recall@k ≥ 0.95 and evaluate 0.99 cost. | Global recall against an exhaustive reference for a fixed query set; tie-aware evaluation; filters and model identity held constant. Average per-shard recall is insufficient. |
| **Scale** | 100M → 1B → 10B → 100B. | Full-size storage, metadata, maintenance and query runs before 100B sign-off. Smaller runs cannot establish final fanout/cold-start behavior. |
| **Filters and skew** | Unfiltered, 10%, 1%, 0.1% eligible sets; tenant/time/class concentration; skewed and duplicate-heavy partitions. | Correct eligible results and recall; bytes/partitions/scoring work; no assumptions of uniform distribution. |
| **Serving** | Representative concurrency 1/16/64; warm, cold and scale-from-zero cases. | p50/p95/p99, throughput, queueing, OOM/retries and cost/query at matched quality. |
| **Large-k mining** | Top-10K/100K/1M with ID/score output, metadata output and selected-media export measured separately. | Complete-result time, candidate exchange bytes, peak coordinator/worker memory, refinement bytes and sampling reproducibility. |
| **Maintenance** | Continuous append plus realistic updates/deletes while queries run. | Stable backlog and segment count; measured write-to-indexed lag; steady-state build/merge cost and recovery headroom. |
| **Recovery** | Kill workers/coordinator around artifact writes and commit; replay work; expire credentials; overlap cleanup. | No lost live rows, duplicate publication or invalid references; completed work reused; source snapshots retained for retries. |
| **Cross-engine correctness** | Aura/Ray/Spark/Flink read the same snapshots; exercise new table/index capabilities. | Agreement on IDs, types, nulls, filters and counts; explicit unsupported-client behavior; no accidental brute-force fallback. |
| **RQ regression** | Fixed probes; compare single/batch paths, k prefixes, approx modes, filters and proposed pruning fix. | Recall and ranking-consistency report for the actual release; performance recomputed after accepted correctness changes. |
| **Accelerators** | Compare CPU and GPU/NPU build/rerank stages under identical quality and total resource cost. | End-to-end gain including device transfer, data reads, quantization, spill, upload and idle capacity. |

For full-size ground truth, use distributed exact scoring for a fixed, affordable query set and preserve those outputs. Small sampled corpora help debugging, but their nearest neighbors are not ground truth for the 100B corpus. For large k, report set recall and relevant downstream curation metrics, not just recall@10.

The rollout decision should be based on **cost per indexed billion vectors, cost per million maintained rows, cost per qualified query/export, and training samples delivered per second**, alongside recall and reliability. These connect low-level improvements to AI DataLake's actual product value.

## 10. Delivery priorities

| Order | Deliverable | Exit condition |
|---|---|---|
| **1 — Production baseline** | v14.0.2 + explicit format 2.2; version/capability matrix; metric/null/delete regressions. | All core engines certified; required operations use the expected index path. |
| **2 — Sustainable maintenance** | Aura/Ray execution of v14 optimize plans, durable checkpoints, coverage dashboard and bounded jobs. | No growing backlog at the agreed ingest rate; recovery preserves completed work. |
| **3 — Efficient retrieval** | Shared batches where supported, calibrated effort controls, scalar filtering, deferred payloads, global top-k. | Recall and end-to-end cost targets met for both small-k and large-k profiles. |
| **4 — Next-release candidates** | Bounded consolidation, quantized/batched refinement, segment scoring skips, RQ correction. | Version-specific correctness, compatibility and equal-quality benchmarks pass. |
| **5 — Longer integration track** | Portable blobs/snapshots, physical clustering provider, Ascend kernels, PCA and experimental compression. | A demonstrated bottleneck and measurable workflow benefit justify each dependency. |

**Decision:** the v10→v14 upgrade is primarily a scale, maintenance, execution and correctness upgrade to an already capable vector/multimodal layer. The most consequential next step is combining those primitives with Aura's distributed execution and lifecycle management, then proving the complete system at 100B × 1,024 dimensions.

## Evidence and review notes

- Primary evidence consists of Huawei documentation, upstream release records, tagged Lance source, and PR/issue descriptions linked next to the claims. PR benchmarks are author-reported and were not rerun.
- Stable release membership was checked with local Git ancestry/history. Active-work status was checked through GitHub's public API on 6 November 2026; an open/draft PR is not a release commitment.
- The [existing ecosystem assessment](aidatalake-lance-100b-industry-ecosystem-overview.md), [Aura integration plan](aura-distributed-indexing.md), and [Ascend study](lance-clustering-npu-ascend-research.md) provide local context. This report independently checks the v10 baseline and does not treat prior summaries as release evidence.
- The slide uses the references' 16:9 dimensions, Arial typography, navy headings, red emphasis, and restrained comparison-table layout. Its content is a decision summary; this report retains the limitations and source detail.
- No production deployment, repository source code, or external service configuration was changed. No 100B benchmark was run as part of this research.
