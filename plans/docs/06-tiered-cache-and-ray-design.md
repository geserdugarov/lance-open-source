# Tiered index cache and Ray orchestration

Status: **design for review; no feature implementation in this change**.  
Date: 2026-11-05. Companion: [interactive HTML review](06-tiered-cache-and-ray-design.html).

## 1. Recommended architecture

Keep a **local, optional RAM/SSD index cache in Lance**. Implement the distributed
service in **`../lance-ray-open-source`**, using the existing runtime from
**`../ray-open-source` without modifying Ray**. Lance remains usable without Ray.

The source branch implements a local cache despite its `DistributedCacheBackend`
name. Its design assumes an external partition coordinator, but neither that
coordinator nor distributed query execution is implemented there. Consequently,
this is two workstreams: adapt the local cache to current Lance, and build the
missing orchestration in Lance-Ray. A cherry-pick alone cannot supply the latter.

Deliver a replicated service first: one actor executes each complete native
query with its own cache. Then add partition-sharded execution using explicit,
runtime-independent Lance query primitives. Sharding is a separate delivery gate,
not a capability implied by successful cache prewarming.

```text
 Application / optional existing Ray Serve ingress
                         |
                         v
 +---------------- lance-ray-open-source ---------------------+
 | Service controller: snapshot, placement, readiness, epochs |
 | Query router: choose replica OR dispatch partition work   |
 | Recovery, admission limits, aggregate metrics             |
 +-----------------------+-----------------------------------+
                         | public Ray actor APIs
             +-----------+-----------+
             |                       |
       Ray actor A              Ray actor B
 +----------------------+ +----------------------+
 | Lance-Ray worker     | | Lance-Ray worker     |
 | +------------------+ | | +------------------+ |
 | | native Lance     | | | | native Lance     | |
 | | pinned Dataset   | | | | pinned Dataset   | |
 | | Session + search | | | | Session + search | |
 | | RAM -> local SSD | | | | RAM -> local SSD | |
 | +---------+--------+ | | +---------+--------+ |
 +-----------|----------+ +-----------|----------+
             +-----------+-----------+
                         | misses / metadata / raw rows
                         v
          Canonical object storage: Lance dataset

 Ray (unchanged): processes, scheduling, RPC, ObjectRefs, faults
 Lance-Ray owns distributed policy. Lance owns storage and query meaning.
```

Read sections 2-4 for the scope and ownership decision, 6-8 for query and recovery
correctness, and 11-12 for implementation gates and review decisions.

## 2. Evidence and exact baselines

These are local source snapshots, not claims about the newest published packages.

| Project | Inspected revision | What it establishes |
|---|---|---|
| Feature branch | `private-12.0-cache`, `a57e30147f8f08a15463e25bfad9ae7cf0772de0` | Cache implementation in `bf6fa8d75`, statistics in `4a2d58863`, subsequent design/report documents |
| Feature base | Lance v12.0.0, `cbeec97cb` | Compare feature changes against this base, not the entire v12-to-main difference |
| Target Lance | `two-level-cache`, `65c3325b7f47189f0c29172c9fe7525fdc818c54` | Architecture notes over source `042ccc837`; workspace `15.0.0-beta.10` |
| Lance-Ray | `two-level-cache`, `fd56f0156eb5718aca07df90968c8e25dca845b7` | Package `0.4.2`; notes describe implementation baseline `3a26760f` |
| Ray | `master-described`, `067653bb73a2134e952d13e954145d8b0e1aa4ac` | Notes over runtime source `ad7245ffb7`, Ray `2.60.0` |

### What the source branch actually contains

| Implemented in v12 branch | Adaptation decision |
|---|---|
| Separate always-on index-state RAM and optional decoded-partition RAM | Retain separate budgets; distinguish the session's dataset/file metadata budget |
| Local files containing serialized current-format IVF partitions | Retain local persistence; cover the current branch's additional eligible entry types |
| L1, then L2, then existing source loader; concurrent-load deduplication | Preserve, including ordinary-query fallback when SSD persistence fails |
| Exclusive directory lock, temporary write, sync, atomic publication | Preserve as local mechanisms |
| Exact index invalidation, generation fences, persistent failure tombstones | Preserve the contracts through current cache interfaces |
| Strict selected-partition prewarm and per-tier counters | Adapt APIs and diagnostics to the target rather than replace newer interfaces |
| `Session.with_distributed_cache(...)` | Use the existing backend registry with a locally named `tiered` backend |

**Not implemented by that branch:** routing, assignment, actor lifecycle,
replication, global result reduction, bounded SSD eviction, or policy selection.
`plans/tiered-ram-ssd-cache-policy.md` on that branch is explicitly a proposal.
It is not evidence that an eviction policy already exists.

The branch's adoption report describes a four-actor, one-host benchmark using
IVF_RQ, returning IDs/distances without exact refinement or result payloads. It
supports investigating local partition reuse; it does not validate distributed
partition search or predict full production query speedups. No performance
experiments were rerun for this design.

### Current-branch differences that must shape the port

* `Session` already accepts backend dictionaries/URIs and exposes
  `cache_diagnostics()`. Preserve those surfaces and memory-only defaults.
* `CacheBackend` now has immediate/resident reads, tier probes, no-promotion
  reads, row gathers, load outcomes, and richer diagnostics. A v12 replacement
  would lose important behavior.
* Native IVF_RQ may cache whole partitions, `PartitionCodes`, or sign/extended
  planes. The v12 `PartitionEntry.*` allowlist misses the latter two.
* Resident row IDs/factors and prepared query state have lifetimes distinct
  from persisted codes. Persisting codes does not eliminate those RAM or open-time
  source-read costs.
* Current queries include multiple physical segments, independent models,
  adaptive probing, quantized refinement, batch queries, fragment reuse, and
  overlays. Distributed execution must preserve or explicitly decline them.
* Existing `prewarm_index` selects physical segments, but has no public
  partition selection on this target. `scanner(index_segments=...)` selects
  segments, not IVF partitions. Fragment splitting is also a different operation.

Source: [cache backend](../../rust/lance-core/src/cache/backend.rs),
[Session bindings](../../python/python/lance/lance/__init__.pyi),
[IVF implementation](../../rust/lance/src/index/vector/ivf/v2.rs), and
[current search architecture](03-index-on-disk-and-search.md).

## 3. Scope and project ownership

The cached unit is an **immutable index payload**: search structures and encoded
vectors, or the current native code/plane equivalents. Raw vectors in `data/`,
result columns, full query results, manifests, scalar payload persistence, and
Ray Data blocks are outside this SSD cache's initial scope.

| Functionality | Project / intended location | Boundary |
|---|---|---|
| Local RAM/SSD backend, locking, capacity, atomic files, integrity, eviction | Lance: proposed `rust/lance-core/src/cache/tiered.rs` | Generic local storage; no actor IDs, Ray imports, cluster membership, or routing |
| Entry persistence eligibility, codecs, decoded weights, plane semantics | Lance: existing index codecs in `rust/lance/` and `rust/lance-index/` | Index owners declare what can be persisted; core does not match IVF type-name prefixes |
| Opaque scope propagation and generation fencing | Lance: cache wrappers and `session/index_caches.rs` | Cache identity and local invalidation |
| Selected strict warmup, index capabilities, native query primitives | Lance: index APIs and scanner/execution code | Single-process operations, callable by any orchestrator |
| Python exposure | Lance: `python/src/` and `python/python/lance/` | Thin bindings; Rust validates and executes |
| Service configuration, controller, worker actors, routing and retries | Lance-Ray: proposed `lance_ray/cache/` and `lance_ray/serving/` | All distribution policy lives here |
| Snapshot rollout, placement, replication, readiness and drain | Lance-Ray | Calls native operations; does not interpret index file layouts |
| Distributed candidate collection and coverage validation | Lance-Ray | Chooses where/when to execute; invokes Lance's native reducer |
| Candidate ranking, deduplication, refinement and final projection | Lance | Same metric/row/filter semantics as local queries |
| Cluster execution, actor RPC, object transport, resource scheduling | Existing Ray | Use public APIs; **zero Ray source or connector changes** |
| SSD mounts, credentials, dataset retention, service entry point | Deployment/application configuration | Supply infrastructure consumed by Lance-Ray; no cache files under the dataset |

Lance-Ray should remain a Python integration package. It should not acquire a
second native storage engine to manipulate Arrow buffers or cache files. Moving
all caching into Python would add FFI callbacks to the hot path and duplicate
Lance's existing codec and load-coalescing mechanisms.

Alternatives considered:

| Alternative | Why it is not the initial design |
|---|---|
| Store index partitions as long-lived Ray ObjectRefs | Couples reuse to object ownership, shared-memory pressure and runtime spilling; does not provide the native directory/restart contract. Use ObjectRefs for bounded query results and transient plans. |
| Build a node-wide cache actor or peer-cache RPC service | Can reduce duplicate SSD copies, but adds a network/IPC hop, a second failure boundary and shared ownership. Measure actor-local duplication before adding it. |
| Put the native backend in a new Lance-Ray binary extension | Splits codec/backend releases and introduces another native packaging/ABI boundary without a distributed responsibility to justify it. |
| Put routing inside the Rust cache loader | Hides remote execution in a storage lookup, couples core to cluster lifetime and makes partial-query failures harder to reason about. |
| Use Ray Data fragment fan-out for online ANN | Fragment work is not IVF partition work; concatenation does not supply native global query semantics. |

Suggested source organization, all proposed:

```text
lance-open-source/
  rust/lance-core/src/cache/
    tiered.rs                   local backend and capacity
    backend.rs, mod.rs          additive scope/capability integration
    codec.rs, diagnostics.rs    eligibility, weights, tier diagnostics
  rust/lance/src/
    session/                    scope and index identity
    index/                      strict warmup and capabilities
    dataset/scanner/            native query decomposition, later phase
  rust/lance-index/src/vector/  existing code and plane codecs
  python/                      bindings and API docs

lance-ray-open-source/
  lance_ray/cache/
    config.py                   serializable local-cache configuration
    worker.py                   actor-local Session and warmup lifecycle
  lance_ray/serving/
    service.py                  public lifecycle and query facade
    controller.py               snapshots, assignments, readiness, recovery
    router.py                   replica selection / partition dispatch
  tests/                        exact-version integration and fault cases
  benchmarks/                   full-query and restart measurements

ray-open-source/
  (no changes)
```

Keep these modules proportional to the implementation; the layout is an ownership
map, not a requirement to create empty wrappers or a plugin framework.

## 4. Local cache contract in Lance

### 4.1 Configuration and cache contents

Register a `tiered` backend through the current `BackendConfig`/URI machinery.
Proposed backend option names:

| Option | Meaning |
|---|---|
| `path` | Existing local directory, exclusively owned by one live backend |
| `index_metadata_l1_capacity_bytes` | Positive budget for index state and other memory-only index entries |
| `l1_partition_capacity_bytes` | Nonnegative payload RAM budget; `0` disables decoded payload admission |
| `l2_capacity_bytes` | Required positive serialized-file byte budget |

`metadata_cache_size_bytes` stays a separate existing Session argument. Do not
copy v12's implicit coupling of two metadata budgets. Existing size/backend
mutual-exclusion checks remain in force. Configuration is immutable for a backend
lifetime. Use a fresh backend to change policy or capacity.

Use the existing Moka admission behavior for ordinary L1 entries and preserve
current plane-priority behavior where applicable. Begin with LRU for SSD. A
general policy-selection API, TTL, peer transfers and long-lived pins are deferred.
Bounded SSD is a recommended addition to the port because production actors
otherwise have unbounded disk growth; it is new work, not a v12 feature.

Add an explicit, runtime-only persistence hint to codec metadata, defaulting to
memory-only, through an additive builder method. Eligible index codecs opt in.
Provide a decoded-size callback for those codecs so a cold L2 decode can be
weighted by decoded memory, not serialized byte length. Existing codec
constructors remain callable. These annotations do not alter index files.

| Entry | Initial treatment |
|---|---|
| Current `PartitionEntry` variants | Optional payload L1 + SSD, using existing codecs |
| Native `lance.vector.rq.partition-codes` | Optional payload L1 + SSD; reattach resident rows through native code |
| Native `lance.vector.rabit.plane` | Payload L1 + SSD when plane strategy is enabled; honor priority, no-promotion and gather contracts |
| Index reconstruction state, scalar entries, live readers | Index-state L1 only |
| Dataset/file metadata | Existing separate metadata cache |
| Legacy index paths | Existing read behavior; no new persistent feature or refactor |

Supporting current IVF_RQ means testing actual current cache shapes. Falling
back to an old writer or forcing all native scans through whole partitions is
not an acceptable shortcut.

Partition assignment does not automatically shard resident state. Current
IVF_RQ resident row/factor stores can span a whole index file and remain held
by an open index handle. Actors serving different partitions of the same segment
may therefore duplicate that state. Budget it during placement and report its
actual size. Preserve Rust's existing `ResidentColumnsSetting::{Auto, On, Off}`
policy; expose a matching additive Python option if the service needs to choose
it. `Auto` must evaluate the state tier's admission limit, not the sum of both
L1 budgets. If residency is disabled, use the current native representation
selected by that policy and report any unsupported plane strategy; do not silently
claim code-only residency or revert to a legacy format. Partition-scoped resident
stores would be a separate native optimization, not a Ray routing feature.

### 4.2 Read path and resource bounds

```text
 Lookup immutable entry
          |
          v
     payload RAM ---- hit --------------------> native search
          |
         miss
          v
 per-key load coalescing (within one backend)
          |
          v
      local SSD ----- valid --> decode/gather -> native search
          |                         |
     miss / invalid                 +--> optional RAM admission
          v
 canonical index loader --> usable value ------> native search
          |
          +--> generation check --> bounded SSD admission
                                     (query may succeed if this fails)

 Later: optional raw-vector refinement and result reads use data/ files.
```

Immediate reads return `Deferred` when lower-tier work is needed. `peek_tier`
uses in-memory inventory without I/O or hit-count changes. No-promotion reads
must not populate RAM. Row gathers use the existing codec range-reader contract
when supported; an unsupported gather returns a miss to the existing source
path, not a fabricated full-entry hit. A tier probe is advisory under eviction.

Keep trait changes additive with conservative defaults for existing backends.
Strict persistence must report an unsupported capability on memory-only/custom
backends that cannot provide it; an ordinary successful load is not proof of
persistence. Preserve native tie ordering, metric normalization and prepared
query state when loading any representation from SSD.

Account SSD bytes before publication, including reservations, temporary files,
replacement overlap and failed deletions. Evict unprotected LRU entries to make
room. An oversized entry may still answer a query but is rejected for SSD
admission; strict warmup returns a capacity error. A RAM hit refreshes SSD recency
only if an SSD copy exists. SSD eviction does not invalidate the index or its
RAM entries.

Rebuild inventory at startup, remove incomplete artifacts, and recover within
the configured budget before advertising readiness. Recency may reset on restart;
file modification time is not access history. Keep serialization, decode, disk
I/O, and in-flight native queries bounded. Use the established blocking-I/O
execution mechanisms; do not block Tokio's executor with filesystem calls.

```text
 Approximate worker memory demand
   = index-state RAM + payload RAM + dataset/file metadata
   + mandatory resident index columns + active native handles
   + query/decode/serialization buffers + Python/Ray overhead
   + OS file pages (separately controlled/accounted)

 Node memory also includes Ray's shared object store and other processes.
```

Eviction weights and Ray resource requests are not RSS limits. In-flight `Arc`
references can retain evicted data. Bound query concurrency independently.
The formula describes distinct allocations: count resident stores already charged
to the index-state cache only once, and account separately for stores retained
by active handles after eviction. Expose per-class admission limits where current
open/prewarm decisions need them; a combined capacity is insufficient for a split
backend.
Filesystem allocation includes block rounding/control files beyond the serialized
budget; reserve free-space headroom at deployment time.

### 4.3 Identity, compatibility and integrity

Reuse current canonical key construction. Carry an opaque namespace separately
through additive context/scoped operations; do not recover strings from hashes
or change all existing key schemas merely to implement invalidation.

```text
 Native logical identity
   storage/security domain (isolated backend directory)
     dataset URI
       physical index segment UUID
         optional fragment-reuse UUID
           entry kind + partition ID [+ plane/layout fields]

 Proposed disposable local layout
   <path>/
     tiered.lock
     <CACHE_KEY_FORMAT>/
       <local-layout-and-codec-revision>/
         tombstones
         <opaque-namespace>/<opaque-key>.bin
```

A partition ID is local to one physical segment. It is not a fragment ID or a
global partition across all segments. Dataset version belongs in the query
snapshot; it need not invalidate immutable segment payloads shared by multiple
versions. Filter/deletion/overlay state remains snapshot-sensitive and is not
stored as a reusable search result.

Use distinct cache roots/backends for different tenants or storage authorities.
The same URI string with a different S3 endpoint or object-store registry must
not share a cache accidentally. Lance-Ray passes a stable, non-secret storage
identity and constructs the appropriate native registry; credentials are not
cache-key material or logged configuration. Hashes are not authorization.

Keep codec payloads in their current encoding. A new disposable local container
can carry the expected key, length and integrity metadata. Validate complete
payloads on full reads and touched blocks on range reads, using existing hash
dependencies; protect the header/checksum table as well. This catches corruption
that could otherwise remain syntactically decodable. Invalid files are removed
or quarantined and treated as misses.

Do not migrate v12 cache directories. Use a fresh local-format namespace and
refill. Preserve stable canonical Lance formats and existing released readers;
do not add compatibility paths for earlier unstable cache revisions. The current
`CacheCodecImpl` explicitly describes its serialized format as experimental.
Rolling workers use separately compatible local cache revisions.

### 4.4 Invalidation and strict warmup

Keep exact invalidation of `(dataset URI, physical segment UUID, optional FRI
UUID)`. It fences publishers, removes the namespace's local payloads and related
dataset-level reconstruction keys, and preserves unrelated indices. Ordinary
eviction is distinct from invalidation.

Scope invalidation and global `clear()` both need generation fences. Capture the
generation before starting a load, then atomically check it at publication under
the same ordering used by invalidation. A racing request may finish on its pinned
snapshot, but must not republish into the invalidated generation. Failed cleanup
leaves a persistent tombstone; fail closed on unusable tombstone state or rotate
to a fresh empty root. Do not let restart resurrect files from failed invalidation.

Preserve the current best-effort `prewarm_index(...)`. Add a separately named
**proposed** `prewarm_index_partitions(name, *, selections)` operation. Each
selection identifies a physical segment and either explicit IDs or `all`:
an explicit empty list means no work. This avoids the v12 `[] = all` behavior
turning an empty worker assignment into an accidental whole-index load.

Strict warmup validates all segments and IDs before payload work, maps each
partition to the eligible native entries for its active layout, ensures they
are durably present together at a completion checkpoint, and returns a report.
It must persist RAM hits too. Temporarily protect the selected set against local
eviction during the operation; fail if it cannot coexist in the budget. Errors
and cancellation release reservations/protections, although partial files may
remain. Recheck generation at completion.

Concurrent warmups must fail with an actionable capacity/busy result if each
would otherwise wait on the other's protected entries. Specify one lock order
for publication, eviction and invalidation, and never hold capacity bookkeeping
locks across filesystem I/O. Readers retain valid file handles or read guards
while eviction unlinks an entry; replacement cannot change bytes under a gather.

The report contains segment/partition coverage, representation, local format,
generation, bytes and completion status. It distinguishes entries already valid
from newly written ones. Residency may change after return: **strict warmup is
a checkpoint, not a lease or a guarantee of no future source reads**. Mandatory
resident state and metadata are reported separately. Legacy or unsupported
strict-persistence requests fail explicitly.

## 5. Lance-Ray control and worker lifecycle

Lance-Ray passes serializable configuration to actors. Each actor creates and
retains its own native Session and Dataset. Do not serialize Session objects or
open file handles through Ray. Namespace clients and credentials are reconstructed
inside the actor using existing Lance-Ray utilities.

A deployment descriptor records the resolved dataset location, namespace/table
identity where applicable, numeric version and manifest identity, logical index
name, physical segment identities, model/layout capabilities, storage identity,
cache budgets, serving mode and worker assignments. Capability checks inspect
the actual wheel and index; a package-version comparison alone is insufficient.

Resolve "latest" once per deployment/refresh, never independently in workers or
per query shard. Query requests carry the resulting snapshot identity. Application
retention must keep that snapshot's source files available while it is served.

Warmup scope is explicit service configuration. The proposed `warmup="all"`
replicated mode requires the full eligible payload set to fit on each worker's
SSD. Partitioned mode warms each worker's assignment. A deliberate
`warmup="none"` mode can serve with demand loading; its READY state asserts
snapshot/capability validity without claiming any payload is warm. An explicit
selection can support a smaller replicated working set. Do not silently reduce
the requested warm set to make startup succeed.

```text
 Desired deployment (durable application configuration)
                 |
                 v
              PLANNED
                 |
        allocate actor + cache root
                 v
       OPENING -> WARMING -> READY -> DRAINING -> CLOSED
          |          |         |
          +----------+---------+--> FAILED / recover with new incarnation

 READY requires: expected snapshot + required capabilities + warm report.
 Routing starts only after READY. Every incarnation starts unready.
```

### Placement and recovery policy

Use one directory per logical worker slot and storage/security domain on a node,
under a mount outside Ray's temporary session directory. Keep its name stable
across same-node process replacement. Only the exclusive native lock permits
reuse; never remove a live lock file to force takeover. A replacement waits for
the old process to close, or starts with a different empty root.

Default to explicit controller-managed worker replacement with actor automatic
restart/task retries disabled. This gives Lance-Ray one owner of recovery policy
and avoids multiplication of runtime retries with request retries. Configure
CPU requests explicitly and bound pending queries. A small, serialized control
loop should remain responsive while native searches execute in a bounded worker
executor; do not run synchronous Lance calls directly on an async actor's event
loop. Session/index concurrency must be verified under that model.

Use public scheduling strategies. Prefer spread placement for new replicas, and
node affinity when attempting reuse of a known node-local SSD. Even soft affinity
can wait for resources on a live feasible node; apply a startup deadline and let
the controller choose a new node/root when appropriate. `SPREAD` is best effort,
so validate actual placement before declaring replicas to tolerate node loss.
These constraints follow the existing [Ray scheduling API](https://docs.ray.io/en/latest/ray-core/scheduling/index.html)
and the [checked-out implementation](../../../ray-open-source/python/ray/util/scheduling_strategies.py).

After a process failure on the same node, reopen the locked directory, validate
files, rebuild metadata/resident state, and re-establish readiness. After node
loss, use a ready replica or cold-load on another node. Node-local SSD is not
replicated durable storage. Partition replication means another worker warms
the same immutable source entries, initially without peer-to-peer copying.

Use a single named controller per deployment. Persist desired state and active
snapshot/assignment revisions outside ephemeral actor memory; workers and file
inventories are rediscovered/revalidated after recovery. A controller restart
gets a fresh fencing token, reconciles worker incarnations, and only then publishes
routes. A shared durable active pointer must use conditional updates when external
supervisors can compete. This uses deployment storage, not modifications to GCS
or the Lance dataset manifest. There is no new consensus service in this design.

Detached/named actors can outlive the creating job, but are not durable records.
Ray restarts reconstruct actors; they do not restore application state. An
acknowledgement can be lost after work completed, so warmup/reconciliation must
be idempotent. See [Ray actor fault semantics](https://docs.ray.io/en/latest/ray-core/fault_tolerance/actors.html)
and the [local lifecycle notes](../../../ray-open-source/plans/docs/01-architecture.md).

## 6. Query execution: two explicit modes

### 6.1 Replicated mode: initial service

Every serving actor opens the same pinned dataset snapshot. Each query goes to
one READY actor, chosen by availability, load and optional cache affinity.
That actor runs the normal Lance scanner end to end. It may read any partition.
Warmup can prioritize a subset, but it does not restrict query coverage.

```text
 request(version V, q, k, filters, refinement, columns)
            |
     Lance-Ray router
            |
       one READY actor
            |
     native Lance scanner
       select segments and partitions
       -> cached index reads -> search
       -> delta/overlay/filter handling
       -> refinement -> take result columns
            |
       one complete result
```

This mode supplies distributed serving, independent cache lifetimes and
recovery without a new distributed ANN algorithm. It supports native query
semantics, subject to the deployed Lance APIs. Repeated partitions can be stored
on multiple actors; cache affinity is a performance hint, not a correctness rule.

### 6.2 Partitioned mode: target architecture, additional native APIs required

Do not implement this as `ray.data.read_lance(..., scanner_options={nearest: ...})`
over fragment tasks followed by concatenation, or by giving every actor the
same ordinary nearest-neighbor query. Neither constrains an actor to assigned
IVF partitions or supplies the native global semantics.

Add runtime-independent primitives in Lance, exposed through thin Python
bindings. Names below are **proposed contracts**, not existing APIs:

| Primitive | Native responsibility | Lance-Ray responsibility |
|---|---|---|
| `plan_vector_query(request)` | Validate snapshot/query, choose segments, apply per-model query preparation, select ordered probes, define candidate budgets and supported shape | Receive a serializable plan and group its work by assignment |
| `search_vector_partitions(plan, work_units)` | Validate exact segment/partition scope, execute native searches, apply relevant row validity and return bounded candidates plus completed work IDs | Dispatch, cap concurrency, retry the same immutable work on another actor |
| `finish_vector_query(plan, candidates, completed_work)` | Validate completeness, rank/deduplicate with native distance semantics, perform global refinement and projection | Gather bounded results and invoke the reducer on a selected actor |

These are semantic seams, not a generic distributed execution framework in
Lance. Core plans contain no actor handles or cluster layout. Reuse native search
implementation rather than write NumPy centroid selection or a second Python
distance/reduction implementation. `RabitScanFactory` is a session-scoped native
strategy hook, not a general-purpose Ray transport interface.

```text
 Pinned query Q @ snapshot V
             |
 native plan: segment S0 -> partitions [1, 4]
              segment S1 -> partitions [2, 6]  (its own centroid model)
             |
 Lance-Ray: assignment lookup by (segment UUID, partition ID)
       +-----+-------------------+
       |                         |
 worker A                    worker B
 S0/1, S1/2                 S0/4, S1/6
 local Lance search         local Lance search
       |                         |
 bounded candidates + explicit completed work IDs
       +------------+------------+
                    |
      native finish on a selected actor
      coverage -> global candidates -> refine -> take
                    |
            one result for V
```

Each work item is identified by request ID, snapshot digest, native plan digest,
segment UUID, partition ID and probe ordinal. The transport envelope additionally
contains deployment epoch, worker incarnation and attempt ID. Accept one winning
attempt per work item, not one result per replica. A valid empty result still
acknowledges a searched partition. Reject wrong-version or wrong-plan responses;
missing work is a retry/error, never an implicitly successful partial result.

Candidate batches retain native distance representation and row identity plus
enough segment/query provenance for the reducer. Plans and batches are transient,
versioned execution artifacts. Validate plan protocol and native build capabilities
across workers; do not pickle live native execution state or promise cross-release
plan serialization compatibility. Keep large filter masks resident/native where
possible; they are not controller metadata.

For fixed-probe single-vector search, if native execution needs `B = k * r`
approximate candidates before exact refinement, each disjoint partition work
unit must be allowed to return up to `B`, not just `k`. A candidate outside a
partition's top `B` cannot enter the union's top `B` under the same ordering.
The native reducer then chooses the global budget and refines once. This
argument does not cover multivector aggregation, adaptive stopping or quantized
reranking; those require their actual native algorithms. It is an ANN candidate
equivalence condition, not a claim of exact nearest-neighbor recall.

Example: `k=10`, `refine_factor=4`, four selected partitions implies up to 160
transferred candidates, then native reduction to the same global refinement
budget as a single-process query. Returning only ten from each worker and
refining locally can lose candidates the native plan would have retained.

### 6.3 Initial partitioned support and explicit fallback

Start partitioned execution with fixed positive probe counts, one fixed-size
query vector, fully covered current-format flat IVF segments, no overlays,
no user filter, and no quantized reranking. Include exact refinement only once
the native finalizer is parity-tested. Each segment uses its own model and
metric scaling; identical integer partition IDs do not imply shared centroids.

| Query shape | Initial partitioned behavior |
|---|---|
| Fixed probes, supported FLAT/PQ/SQ/RQ layout, complete index coverage | Partition work + native finalizer after parity gate |
| Filters, HNSW, adaptive probing/search effort, multivectors, batch-query special paths | Complete native query on one capable actor, or explicit unsupported error according to configured mode |
| Unindexed fragments or newer overlay vectors | Complete native query, preserving native delta/overlay behavior |
| Legacy index segment or unsupported cache representation | Existing native query path; no new legacy persistence |
| Missing shard response | Bounded retry, or whole native query at the same snapshot; never return an incomplete reduction |

Expose one `unsupported_query` mode (`native` or `error`), not conflicting boolean
flags. Make fallback observable in the response/metrics. A full native fallback
may cold-load unassigned partitions and requires a capable actor with suitable
working memory. Discard all partial distributed results before that fallback.
Do not silently enable `fast_search`, drop filters, fix adaptive probes, or read
latest to get an answer.

Later native decomposition can add global probe waves, prefilter masks,
snapshot-correct delta work, and overlay reconciliation. Lance decides the next
wave or stopping condition; Lance-Ray only dispatches it. Until then those query
shapes remain fully supported by replicated execution, not by partial sharding.

## 7. Assignment, snapshot rollout and failure behavior

Track three independent identities: **dataset snapshot** (query meaning),
**deployment/assignment epoch** (routing), and **local cache generation**
(publication fencing). Do not use one number for all three.

Partition assignments are hints about where to keep immutable payloads warm.
Balance estimated encoded bytes and observed work, not only partition counts.
All eligible source entries remain readable elsewhere. Replication factor and
failure-domain placement are Lance-Ray settings, with verified actual nodes.

```text
 Active: epoch E, snapshot V, segments [A, B]
                        |
       discover committed snapshot V+1, segments [A, C]
                        |
       prepare new assignments; reuse compatible A payloads
                        |
       strict warm C; verify required READY coverage
                        |
       atomically publish E+1 / V+1 for new requests
                        |
       drain requests pinned to E / V
                        |
       release B only when no served snapshot references it
```

Refresh revalidates all changed metadata even when segment payloads are reused.
New deletes or overlays can change results without replacing every index UUID.
Cache invalidation is not a dataset refresh mechanism. Retiring B is local
reclamation; it does not delete canonical index files. Coordinate source-version
cleanup through the application's existing retention policy.

Warmup plus epoch publication is not a permanent residency lease. Eviction after
READY can cause a later cold load while preserving correctness. If an assignment
must remain fully SSD-resident for its whole lifetime, add reserved-capacity
leases as a separately reviewed feature; do not claim that guarantee here.

| Event | Required behavior |
|---|---|
| SSD miss | Native source load, bounded local admission |
| Corrupt/incompatible local entry | Discard it, count it, reload canonical data |
| Disk full / failed write | Ordinary query can succeed; strict warm fails and new assignment stays unready |
| Strict warm exceeds capacity | Descriptive failure; do not evict early selections and report success |
| Invalidation races with load | Request may finish at its snapshot; stale publisher cannot repopulate |
| Same-node actor crash | Close old owner, reopen directory, reconstruct Session, revalidate readiness |
| Node lost | Use another ready copy or rebuild from source on a new node |
| Lost RPC acknowledgement | Retry idempotent work; discard duplicate attempt results |
| Controller lost | Reconcile durable deployment descriptor and fencing token before routing |
| Timeout during shard query | Retry within request deadline, full native fallback, or explicit error |
| Canonical files unavailable/cleaned | Return the native failure if needed data is absent; SSD is not a durability guarantee |
| Rolling binary change | Capability handshake; compatible execution protocol or separate deployment epoch and cold local revision |

Cancellation stops issuing work and releases query reservations. Remote work
already running may finish, but stale completions cannot change routes or become
accepted results. Late cache writes remain subject to native generation fencing.

## 8. API sketches and worked examples

All new names/options in this section are **proposed**. They document the desired
interface and must be finalized with implementation; these examples are not
runnable against the current checkouts.

### 8.1 Standalone Lance: local caching without Ray

The existing `Session(index_cache_backend=...)` shape is reused; `tiered` and its
options are new. Numbers are illustrative, not sizing guidance.

```python
from pathlib import Path
import lance
from lance.dataset import Session

cache_path = Path("/mnt/nvme/lance-cache/standalone")
cache_path.mkdir(parents=True, exist_ok=True)
session = Session(
    index_cache_backend={
        "kind": "tiered",  # proposed registered backend
        "options": {
            "path": str(cache_path),
            "index_metadata_l1_capacity_bytes": str(64 * 1024**2),
            "l1_partition_capacity_bytes": str(512 * 1024**2),
            "l2_capacity_bytes": str(8 * 1024**3),
        },
    },
    metadata_cache_size_bytes=64 * 1024**2,
)
ds = lance.dataset("s3://example/vectors.lance", version=42, session=session)
ds.prewarm_index_partitions(  # proposed strict API
    "embedding_idx",
    selections=[{"segment_id": segment_uuid, "partition_ids": [1, 4]}],
)
result = ds.to_table(
    nearest={"column": "embedding", "q": query_vector, "k": 10, "nprobes": 8},
    columns=["title"],
)
diagnostics = session.cache_diagnostics()  # existing API, extended tier details
```

`segment_uuid` comes from the pinned dataset's index description and
`query_vector` is supplied by the caller. Selecting all partitions is explicit:
`{"segment_id": segment_uuid, "partition_ids": "all"}`. An empty selection does
no warmup work. The subsequent ordinary query is free to probe other partitions.

### 8.2 Distributed serving configured in Lance-Ray

```python
import ray
from lance_ray.serving import CacheConfig, VectorSearchService  # proposed

ray.init(address="auto")
service = VectorSearchService.start(
    name="catalog-vectors",
    control_uri="s3://example/service-state/catalog-vectors/",
    uri="s3://example/vectors.lance",
    version=42,
    index_name="embedding_idx",
    mode="replicated",
    warmup="all",
    workers=3,
    cache=CacheConfig(
        root="/mnt/nvme/lance-cache/catalog",
        index_metadata_l1_capacity_bytes=64 * 1024**2,
        l1_partition_capacity_bytes=512 * 1024**2,
        metadata_cache_size_bytes=64 * 1024**2,
        l2_capacity_bytes=8 * 1024**3,
    ),
)
service.wait_ready(timeout_s=120)
result = service.search(
    query_vector, k=10, nprobes=8,
    refine_factor=4, columns=["title", "category"],
)
service.refresh(version=43)  # prepare, warm, publish, drain
service.close()              # drain and stop actors; retain reusable SSD files
```

Lance-Ray derives unique worker roots under `root`, resolves and persists the
deployment descriptor under the separate `control_uri`, and uses public Ray APIs
internally. This proposed control-store location needs conditional-write support
for active-pointer fencing and is outside the Lance dataset. The same actor
interface later supports `mode="partitioned"`, an explicit partition replication
factor, and `unsupported_query="native"`; requests outside its advertised
capability set run a complete native query. No `ray.data` monkey patch is involved.

### 8.3 Read-cost example

Suppose a query selects S0/1 and S0/4. The first request misses RAM and SSD, loads
those entries from `_indices/`, and writes local copies. A repeat can hit RAM.
After the actor process restarts on the same SSD, those payloads can be decoded
from local files. After node loss, another warm replica can serve them; a new node
with no copy reads the source again.

Even in the warm cases, opening the snapshot and loading required index state
can read the source. Exact refinement may fetch roughly `k * refine_factor` raw
vectors, and requested result columns still come from `data/`. This cache does
not make a full query independent of object storage.

## 9. Observability and performance acceptance

Extend current `cache_diagnostics()` rather than introduce the v12 statistics
API as the primary surface. Preserve existing field meanings; add optional
named tier diagnostics with unsupported measurements represented as unavailable.
Keep aggregate resident bytes distinct from SSD bytes, and keep per-query
`ScanStatistics` distinct from lifetime Session counters.

| Lance measurements | Lance-Ray measurements |
|---|---|
| State/payload RAM hits; local SSD hits/misses; source loads | Complete-query p50/p95/p99 and throughput |
| Coalesced waits separately from physical hits | Queue time, RPC time, retries, full-native fallbacks |
| SSD occupied/reserved bytes; evictions; rejected admissions | Assignment bytes, warm duration, readiness, actual node placement |
| Decode/range-read/write bytes and time; integrity errors | Per-epoch/actor-incarnation recovery and drain time |
| Strict warm failures; fenced publishers; invalidation errors | Completed/missing/duplicate work counts and candidate-transfer bytes |

Label normal demand and prewarm separately. Sample cheap counters outside timed
queries; refreshed/per-type diagnostics may be expensive. Avoid unbounded metric
labels such as row IDs, arbitrary queries, or raw dataset URIs.

Measure the exact target builds, index layouts and query shapes. Include current
IVF_RQ whole-entry/code/plane configurations and other supported index families,
multi-fragment data, multiple segments, realistic filters and payloads. Compare
memory-only and tiered sessions with identical query options and recall targets.

Benchmark: cold start; warm RAM; working set larger than RAM but fitting SSD;
same-node process restart with warm and cold OS file pages; node loss; snapshot
rollout; and all-RAM workloads to reveal added overhead. Record source index I/O
separately from raw-vector/payload I/O, SSD bytes, RSS, OS pages and startup cost.
Use held-out queries and assert recall of at least 0.5, plus the workload's stricter
threshold where applicable. Result equality alone is not recall evidence.

Illustrative Amdahl bound: if partition loading is 10% of serial query time,
eliminating it entirely gives at most `1 / (1 - 0.10) = 1.11x`. This is a model,
not a prediction; concurrent pipeline timings cannot simply be added as serial
components. Sharding also introduces RPC and reduction costs.

## 10. Compatibility with unchanged Ray

Use Ray Core actors/tasks and public scheduling APIs from the checked-out Ray
runtime. The service does not rely on Ray's built-in Lance datasource/datasink.
Their fragment-task reads do not supply global ANN reduction, and the checked-out
datasink still passes the old `storage_options_provider` keyword where target
Lance expects `namespace_client`/`table_id`.

Implement any required adapter behavior in Lance-Ray using its existing
namespace utilities and native Lance calls. Do not change Ray connectors,
requirements, raylet, object manager, scheduler, or GCS. Do not import Ray private
APIs into the new serving path.

Lance-Ray's existing lockfile records Ray `2.53.0` and pylance `7.0.0b7`, while Ray's
development requirements describe a different old Lance stack. Neither proves
compatibility with the requested checkouts. Later integration work must assemble
a separate, explicitly recorded environment using the local Ray build, target
Lance wheel, matching namespace package (`>=0.13.0,<0.14` for this Lance source),
and an Arrow version that passes both native boundaries. Record wheel hashes and
runtime imports on the driver and every worker. Do not equate a source checkout
with the binary actually executing.

Package/lockfile updates needed for this service belong in Lance-Ray and Lance's
own normal release process. Leave Ray's source and lockfiles untouched. Retain
stable dataset-format compatibility and frozen legacy readers; pre-release
capability tests belong in ordinary integration tests, not the stable-release
cross-version matrix.

## 11. Implementation sequence and acceptance gates

| Step | Owner | Concrete exit condition |
|---|---|---|
| 1. Native local cache | Lance | Registry construction; bounded SSD; restart/corruption behavior; correct decoded weights; no Ray dependency |
| 2. Warmup and current-layout parity | Lance | Exact scoped invalidation and clear fences; strict set checkpoint; actual current IVF_RQ entries supported; existing diagnostics preserved |
| 3. Replicated service | Lance-Ray | Actor-local Sessions; pinned snapshots; READY barrier; same-node and node-loss recovery; full-query parity |
| 4. Native query decomposition | Lance | Fixed-probe plan/search/finish API; multi-segment identity; candidate-budget and result parity without Ray |
| 5. Partitioned orchestration | Lance-Ray | Coverage-checked fan-out, duplicate-attempt handling, replica recovery and explicit native fallback |
| 6. Rollout qualification | Both; Ray runtime unchanged | Exact wheel compatibility, recall and full-query benchmarks, disk/memory and update/failure results reviewed |

Steps 1-3 deliver the adapted cache as a distributed serving feature. They do
not claim partition-sharded execution. Steps 4-5 deliver the sharded target;
advanced query-shape decomposition follows separate parity gates.

Native tests should cover invalid configuration, directory exclusion, miss/hit
and load coalescing, disabled payload L1, cold restart, integrity failure,
oversized entries, concurrent reservations, deletion failures, and cancellation.
Verify selected warmup, empty versus `all`, invalid IDs before I/O, RAM-hit
persistence, generation races, clear races, related metadata and neighboring
index isolation. Exercise full-entry, code-only and enabled plane paths,
including no-promotion and partial reads.

Where a shared wrapper change necessarily touches a frozen legacy read boundary,
isolate it and use released historical fixtures for regression coverage. Do not
extend legacy writers or introduce new legacy cache behavior.

Integration tests need multiple fragments and segments, independent centroid
models, reused partition numbers, deletes, fragment reuse, overlays and unindexed
rows. For unsupported sharded cases, assert explicit native fallback or rejection,
including null query/data cases, empty results and filter behavior. Fault tests
must lose acknowledgements, duplicate completions, restart actors/controllers,
lose nodes, and refresh snapshots during queries. A real multi-node test is
needed to establish node-loss behavior; multiple local actors are insufficient.

Reuse existing test suites where they fit, add meaningful fault coverage, and
keep local unit cases small. Run touched-language lint and required checks under
each repository's documented workflow during implementation. This documentation
change does not claim native tests or runtime benchmarks have passed.

## 12. Decisions to review

| Decision | Recommendation | Main cost/tradeoff |
|---|---|---|
| Where the local backend lives | Lance, named `tiered` | Some native API work; avoids Python hot-path storage and Ray coupling |
| Where distribution lives | Entirely Lance-Ray | A new serving layer alongside its existing batch/build APIs |
| Initial topology | Replicated native queries, then explicit sharding | Faster correctness path; replication duplicates cached bytes |
| SSD budget | Required, bounded LRU from initial production delivery | More local concurrency/accounting work than a direct v12 port |
| Strict warm guarantee | Simultaneous persisted completion checkpoint | Later eviction allowed; guaranteed long-lived residency is deferred |
| Native IVF_RQ coverage | Current whole/code/plane representations | More work than v12's type-prefix allowlist; preserves target optimizations |
| Unsupported sharded query | Complete native query by default, observable; optional error mode | Potential latency jump; no silent semantic loss |
| Cache sharing | One backend/root per live actor, no peer cache initially | Duplicate local bytes; simpler ownership and failure isolation |
| Ray integration | Existing public Ray Core APIs only | Environment verification belongs in Lance-Ray; no connector fixes in Ray |

## Source map

Feature-branch documents must be read at that branch; they are not copied into
the target as implemented code. Useful local commands:

```bash
git show private-12.0-cache:plans/distributed-cache.md
git show private-12.0-cache:plans/task-distributed-cache.md
git show private-12.0-cache:plans/tiered-ram-ssd-cache-policy.md
git show private-12.0-cache:plans/docs/06-two-level-cache.md
git diff cbeec97cb..private-12.0-cache -- rust/lance-core/src/cache
```

| Subject | Inspected source or companion notes |
|---|---|
| Lance layering and current version | [overview](00-overview.md), [versions](04-lance-versions.md) |
| Query semantics and segment boundaries | [search](03-index-on-disk-and-search.md), [distributed index creation](05-distributed-vector-index-creation.md) |
| Generic backend and registry | [backend.rs](../../rust/lance-core/src/cache/backend.rs), [registry.rs](../../rust/lance-core/src/cache/registry.rs) |
| Codecs, namespaces and diagnostics | [codec.rs](../../rust/lance-core/src/cache/codec.rs), [key.rs](../../rust/lance-core/src/cache/key.rs), [diagnostics.rs](../../rust/lance-core/src/cache/diagnostics.rs) |
| Sessions and index scopes | [session.rs](../../rust/lance/src/session.rs), [index_caches.rs](../../rust/lance/src/session/index_caches.rs) |
| Current payloads | [partition_serde.rs](../../rust/lance/src/index/vector/ivf/partition_serde.rs), [partition_codes.rs](../../rust/lance-index/src/vector/bq/partition_codes.rs), [plane_cache.rs](../../rust/lance-index/src/vector/bq/plane_cache.rs) |
| Native query/strategy boundaries | [scanner/vector.rs](../../rust/lance/src/dataset/scanner/vector.rs), [rabit_scan.rs](../../rust/lance/src/index/vector/ivf/v2/rabit_scan.rs) |
| Lance-Ray existing responsibilities | [overview](../../../lance-ray-open-source/plans/docs/00-overview.md), [vector indices](../../../lance-ray-open-source/plans/docs/02-vector-indexes.md), [compatibility](../../../lance-ray-open-source/plans/docs/04-versions-and-compatibility.md) |
| Lance-Ray worker reconstruction | [datasource.py](../../../lance-ray-open-source/lance_ray/datasource.py), [utils.py](../../../lance-ray-open-source/lance_ray/utils.py) |
| Ray execution and integration limits | [architecture](../../../ray-open-source/plans/docs/01-architecture.md), [Lance integration](../../../ray-open-source/plans/docs/05-ray-lance-integration.md) |
| Actual Ray connector | [lance_datasink.py](../../../ray-open-source/python/ray/data/_internal/datasource/lance_datasink.py) |

Sibling links assume the three repositories retain their current relative layout.
