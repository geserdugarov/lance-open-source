**Title:** `feature: implement an optional RAM/SSD index cache for segmented indices`

I propose implementing an optional two-level index cache in Lance, using bounded RAM for frequently accessed index state and bounded local SSD for reusable serialized index entries.

The objective is to support index working sets larger than available RAM, reduce repeated reads from object storage after memory eviction, and reuse cached payloads across process restarts when the local volume survives. Applications would configure the backend through the existing Session interfaces.

There is already substantial support for this direction:

- **An established architectural precedent.** LanceDB Enterprise documents a [tiered index cache](https://docs.lancedb.com/enterprise/v1-12-0/enterprise/tiered-index-cache) that stores index structures across memory and local disk. Its [distributed-search architecture](https://www.lancedb.com/blog/how-lancedb-accelerates-vector-search-at-10-billion-scale) explicitly places independently indexed segments near executors backed by local SSD caches. These capabilities are documented for Enterprise; this proposal concerns the open-source Lance library.
- **Upstream foundations intended for persistence.** [#7160](https://github.com/lance-format/lance/issues/7160) explicitly identifies surviving node restarts as a purpose of cache serialization. Merged [#7163](https://github.com/lance-format/lance/pull/7163) supplies versioned serialization envelopes, while merged [#7683](https://github.com/lance-format/lance/pull/7683) provides backend registration and configuration without adding a concrete persistent backend.
- **Continued investment across index families.** Recent merged codecs for [NGram](https://github.com/lance-format/lance/pull/10423), [RTree](https://github.com/lance-format/lance/pull/10424), and [MinHash](https://github.com/lance-format/lance/pull/10425) make more index entries persistable without changing their canonical index formats.
- **A documented workload problem.** Lance's [storage benchmarking discussion](https://www.lancedb.com/blog/one-million-iops) identifies index loading as a likely dominant cost for cold searches. Existing requests for [SSD caching](https://github.com/lance-format/lance/issues/6483) and [configurable index-cache backends](https://github.com/lance-format/lance/discussions/7575) show demand for more deployment choices.

The proposed implementation would turn these foundations into an optional backend with the following behavior:

| Component | Proposed contract |
|---|---|
| RAM tier | Separately budget memory-only index state and eligible decoded payloads. Payload admission can be disabled. |
| SSD tier | Store eligible entries through existing cache codecs, with a bounded capacity and eviction policy. |
| Read path | Check RAM, then SSD, then invoke the existing source loader. Coalesce concurrent loads of the same entry. |
| Persistence | Use exclusive directory ownership, atomic publication, integrity validation, and compatible-entry recovery after restart. |
| Failure handling | Treat missing, incompatible, or corrupt cache entries as misses. Ordinary queries retain source-loader fallback when cache persistence fails. |
| Resource accounting | Bound loading and warmup buffers as well as resident cache contents; expose RAM, SSD, and in-flight usage separately. |

This should be designed around the segmented index lifecycle from [#6309](https://github.com/lance-format/lance/issues/6309). A logical index contains immutable physical segments, making segment identity a natural boundary for reuse:

| Operation | Expected cache behavior |
|---|---|
| Append new segments | Reuse existing segment payloads and populate entries for the additions. |
| Merge or rebuild segments | Populate new physical artifacts under new identities; retained older snapshots continue using their original segments. |
| Switch snapshots or branches | Reuse identical physical payloads while refreshing snapshot-dependent views. |
| Change deletions, overlays, or row mappings | Apply native snapshot semantics even when physical index bytes remain cached. |
| Evict cache entries | Reclaim local capacity independently of canonical dataset retention and garbage collection. |

For example, changing a snapshot's segment set from `[A, B]` to `[A, C]` should preserve cached payloads for `A`. Preparing `C` should not require reloading the entire logical index.

**Segments should define identity and lifecycle; smaller entries should define admission and eviction.** A segment need not fit entirely in RAM. Keys must distinguish physical storage, segment identity, entry representation, and partition or component identity. Snapshot-dependent views require their own dependencies, and invalidation must prevent older in-flight loads from republishing invalidated state.

This connects directly to the segmented-index, resource-bounding, and branch-support priorities in the [LanceDB-maintainer roadmap, #7290](https://github.com/lance-format/lance/issues/7290).

The relationship with raw-data caching also needs an explicit implementation boundary. The [review of PR #6487](https://github.com/lance-format/lance/pull/6487#pullrequestreview-4172662164) raises maintenance concerns and suggests an `object_store` wrapper. Such a wrapper caches canonical file bytes. An index-cache backend stores the representations exposed by Lance's typed cache and codecs, with index-aware warmup and lifecycle handling. LanceDB's documentation likewise distinguishes [data-page caching](https://docs.lancedb.com/enterprise/v1-12-0/enterprise/data-page-cache) from index caching.

I propose reusing the existing backend registry and codecs, keeping storage policy replaceable, and measuring the additional benefit over a raw-file cache. This avoids introducing another configuration or serialization framework.

Implementation could proceed through three reviewable stages:

1. **Local backend:** bounded RAM/SSD storage, eviction, restart recovery, diagnostics, and integration with explicitly supported current-format IVF entries.
2. **Lifecycle and warmup:** selected segment/partition preparation, capacity validation, and completion reports. Warmup would establish residency at completion; continued retention remains governed by eviction. Rust would own validation and execution, with thin bindings.
3. **Qualification and expansion:** correctness and performance evaluation, followed by additional codec-supported index families. Lance-Ray could subsequently consume these native capabilities for worker readiness and placement; the local backend would be independently useful.

The performance case should be demonstrated under equal CPU and total-memory budgets, including OS page cache. Compare the default memory backend, a raw-file cache, and the proposed index backend across cold, RAM-resident, SSD-resident, restart, and over-capacity workloads. Report throughput, p95/p99 latency, recall, origin index reads, SSD traffic, peak memory, and population cost.

Segment count and consolidation must also vary. [#6860](https://github.com/lance-format/lance/issues/6860) illustrates why per-segment query work needs measurement alongside cache I/O. Related [FTS research in #11064](https://github.com/lance-format/lance/pull/11064) reports benefits from changing how existing cache bytes are read under a fixed memory limit; it supports investigating native cache representations, while the proposed backend still needs its own measurements.

I'm requesting maintainer agreement on this implementation scope: an optional native RAM/SSD index backend that builds on existing interfaces, follows the segmented lifecycle, and is qualified through explicit correctness, resource, and performance tests.
