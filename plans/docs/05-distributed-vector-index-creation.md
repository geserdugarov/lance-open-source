# Distributed Vector Index Creation

> **Status snapshot:** 2026-10-29
>
> **Source baseline:** Lance `14.0.2`, code commit `40f15ed1b` (2026-10-29)
>
> **Scope:** current architecture, public APIs, operational invariants, known
> gaps, and the GitHub work to monitor. Query execution and the general vector
> index algorithms are covered by
> [`03-index-on-disk-and-search.md`](03-index-on-disk-and-search.md) and
> [`02-vector-indexes.md`](02-vector-indexes.md).

Distributed vector index creation in Lance is a **storage protocol driven by an
external orchestrator**. Lance supplies fragment-scoped build, physical segment
merge, and atomic manifest commit APIs. v14 also exposes a Rust optimization
plan with serializable build and merge tasks (section 9). It does not supply a
Spark, Ray, Dask, Kubernetes, or other distributed scheduler; job ownership, retries,
checkpointing, model broadcast, and source-snapshot validation belong to the
caller.

The canonical lifecycle is:

```text
pin source snapshot
        │
        ├── train/broadcast shared model artifacts (when needed)
        │
        └── assign disjoint fragment-id groups
                    │
          ┌─────────┼─────────┐
          ▼         ▼         ▼
       worker 0  worker 1  worker N
       build one uncommitted segment each
          └─────────┬─────────┘
                    ▼
        validate returned segment metadata
                    │
          ┌─────────┴──────────┐
          ▼                    ▼
      commit directly      merge caller-defined
      as N segments        groups into M segments
          └─────────┬──────────┘
                    ▼
       one atomic manifest commit publishes
       the physical segments as one logical index
```

The safest default is:

- use **shared IVF/model artifacts** when segments may ever be physically
  merged;
- commit worker outputs directly when independent per-segment models are
  intentional;
- keep the number of committed segments bounded, because query work fans out
  across every segment;
- never give concurrent workers the same physical segment UUID.

---

## 1. Terms and ownership

| Term | Meaning |
|---|---|
| **Source snapshot** | The dataset version whose schema, fragments, and vector values a build reads. |
| **Fragment assignment** | A non-empty, disjoint set of Lance fragment IDs assigned to one worker attempt. |
| **Model artifacts** | IVF centroids and, depending on the index, PQ codebook or RaBitQ rotation shared with workers. |
| **Worker segment** | One uncommitted output under `_indices/<uuid>/`, plus the returned `IndexMetadata` / `Index`. |
| **Physical segment** | A queryable index directory referenced by one manifest `IndexMetadata`. A worker segment is already a physical segment if committed without merging. |
| **Physical merge** | Rewriting compatible input segments into one new `_indices/<uuid>/` directory. This is more than metadata concatenation. |
| **Logical index** | All compatible manifest index entries with the same user-visible name. It may contain many physical segments. |
| **Coverage** | The `fragment_bitmap` carried by a segment, translated through FRI when deferred compaction requires it. The effective union determines indexed coverage. |
| **Delta segment** | A segment added later for newly appended or otherwise unindexed fragments. |

There are three distinct owners:

| Owner | Responsibilities |
|---|---|
| **External orchestrator** | Pin a source version; choose fragments and worker grouping; train and distribute model artifacts; launch, retry, and cancel tasks; persist job state; validate results; choose merge groups; decide when to commit and clean up. |
| **Lance worker API** | Validate the worker request against its opened dataset snapshot, scan the assigned fragments, build files, and return segment metadata without modifying the manifest. |
| **Lance coordinator API** | Validate a supplied segment set, optionally rewrite merge groups, and atomically publish the final physical segments in a new manifest. |

There is no Lance-owned lease, task queue, barrier, or recovery coordinator.
Creation hands segment metadata to the coordinator. Optimization additionally
provides a plan ID, read version, and grouped task/result records; the caller
still owns durable job state and scheduling.

---

## 2. End-to-end protocol

### 2.1 Freeze the source contract

Before launching workers, record at least:

- dataset URI, branch/tag if applicable, and numeric source version;
- vector column name, Lance field ID, data type, dimension, and nullability;
- distance metric and the complete ordered index stage configuration;
- target fragment IDs and the intended coverage policy;
- shared model artifacts and hashes of their serialized bytes;
- one logical index name and one external build/job ID.

Every worker should open the same numeric source version. In Python that can be
done with `lance.dataset(uri, version=source_version)`. Opening "latest" on
each worker at different times can mix schemas, fragments, or vector values in
one build.

Concurrent dataset activity needs an explicit policy:

- an append can be allowed if new fragments are intentionally left uncovered;
  normal search will flat-scan that delta;
- row deletions are filtered by Lance at query time, but the orchestrator should
  still account for the version change;
- a rewrite, compaction, overwrite, or schema change can make source coverage
  stale. Commit compares historical and current indexed-field file paths and
  prunes retired or rewritten coverage, but an exact-coverage workflow should
  normally abort and rebuild instead of accepting that fallback;
- an in-place vector overlay can keep the fragment ID stable while changing
  indexed values. Because the segment retains its source `dataset_version`,
  queries mask newer overlay rows from ANN and re-score their current values.
  Correctness is preserved, but rebuild if that fallback is operationally
  unacceptable.

The current commit API validates physical field and numeric source-version
provenance in addition to segment-set and replacement invariants. It is still
not a complete provenance barrier for the hours or days between worker launch
and commit: it does not know the external job, model hashes, or intended full
coverage. The coordinator must refresh to the latest permitted version and
confirm the source contract before publication.

### 2.2 Train model artifacts

Two model scopes are supported.

**Shared model scope** trains once and broadcasts artifacts to every worker.
Use this for physical merge compatibility and stable partition semantics across
segments. IVF-PQ needs both IVF centroids and a PQ codebook. IVF-RQ additionally
needs the same RaBitQ rotation. HNSW parameters must also agree when HNSW
segments will be merged.

IVF-RQ defaults to **five bits per dimension** across Rust, Python, and
Java. Record `num_bits` explicitly in the build contract and create the shared
rotation model with the same value. Existing one-bit models remain one-bit;
mixing defaults from different library releases is not a model-sharing policy.

**Independent model scope** lets every worker train on its fragment subset.
Those segments are valid when committed separately: query execution opens each
segment, uses that segment's centroids and quantizer, and merges candidates by
distance. Do not assume independently trained segments can be physically
merged.

"Independent" applies to learned model contents, not to the index contract.
All segments in one logical vector index should still use the same vector field,
dimension, distance metric, stage families, and compatible tuning parameters.
The current direct-commit validation does not compare that entire contract, so
the orchestrator must enforce it.

Python exposes the most convenient shared IVF-PQ preparation helper:

```python
from lance.indices import IndicesBuilder

models = IndicesBuilder(source, "vector").prepare_global_ivf_pq(
    num_partitions=4096,
    num_subvectors=32,
    distance_type="l2",
)

ivf_centroids = models["ivf_centroids"]
pq_codebook = models["pq_codebook"]
```

This helper delegates to process-local training paths. Proportional hierarchical
k-means and streaming coreset controls bound or improve work within that
process; they do not distribute Lloyd rounds across external workers. Rust's
`train_ivf_model_from_sample` also accepts an externally collected sample
without scanning a dataset. The open work for scheduler-neutral
distributed centroid training is tracked in
[#7321](https://github.com/lance-format/lance/pull/7321).

Java has an important exception at this snapshot. `VectorTrainer.trainPqCodebook`
calls the native PQ trainer without the IVF model. For L2 and Cosine, however,
the worker build quantizes residuals relative to the selected IVF centroid.
Consequently the Java helper fits its codebook to normalized/raw vectors and
then applies that codebook to residual vectors. The index remains structurally
readable, but the codebook was not optimized for the values it encodes and
recall can degrade. Closed, unmerged
[#6363](https://github.com/lance-format/lance/pull/6363) implemented an
IVF-aware, fragment-scoped Java trainer; revive or replace that work before
treating the Java-only helper as equivalent to the Rust/Python shared IVF-PQ
path. Until then, train the residual codebook through an IVF-aware Rust/Python
path, flatten and broadcast it to Java workers, or make recall measurement an
explicit acceptance gate.

### 2.3 Assign fragments

Partition the target fragment IDs into disjoint, non-empty worker groups. A
fragment must not appear in two successful outputs in the same final set;
`commit_existing_index_segments` rejects overlapping coverage.

The grouping unit is a fragment, not an arbitrary row range. Group sizing is an
orchestration decision:

- more, smaller groups increase build parallelism and reduce retry scope;
- fewer, larger groups reduce object count, coordinator metadata, later merge
  cost, and query fanout if outputs are committed directly;
- groups should be balanced by rows or bytes, not merely fragment count, when
  fragments have uneven sizes.

### 2.4 Build one uncommitted segment per worker

The Rust dispatch has an important distinction:

| Request | Internal path | Result |
|---|---|---|
| True fragment subset **with precomputed IVF centroids** | `build_distributed_vector_index` | A shared-model, merge-oriented worker segment. PQ codes remain row-major and RQ codes unpacked until final merge. |
| True fragment subset **without precomputed IVF centroids** | `build_filtered_vector_index` | A standalone segment that trains its own IVF/quantizer model over the requested fragments. |
| Full dataset coverage, even if all IDs were explicitly supplied | Normal `build_vector_index` | A normal full-coverage segment. `execute_uncommitted` still leaves it unpublished. |

The worker scans only its supplied fragment IDs. The specialized distributed
path currently supports:

- `IVF_FLAT`;
- `IVF_PQ` (V3 index format);
- `IVF_SQ`;
- `IVF_RQ`;
- `IVF_HNSW_FLAT`;
- `IVF_HNSW_PQ`;
- `IVF_HNSW_SQ`.

Each successful call writes a new directory and returns metadata containing the
UUID, field ID, source dataset version, exact fragment bitmap, index details,
index version, and file information.

For normal whole-table builds, insufficient trainable data can instead produce
a definition-only index with no trained files or coverage. True fragment-subset
builds do not use that fallback. A coordinator expecting worker payloads must
validate coverage and files, not just the existence of an index definition.

### 2.5 Validate the worker barrier

Before merging or committing, the orchestrator should prove:

```text
all successful UUIDs are unique
union(segment.fragment_ids) == intended target fragment set
sum(len(segment.fragment_ids)) == len(the union)    # no overlap
every segment was built for the expected field ID
every segment belongs to the same source/build contract
every referenced _indices/<uuid>/ directory is complete
```

Lance repeats several structural checks, but it does not know the external job
ID, intended full target set, model artifact hash, or whether a returned segment
came from an obsolete retry attempt.

### 2.6 Choose the final physical topology

The worker boundary and committed segment boundary do not have to match.

| Topology | Model requirement | Extra I/O | Query effect | Best use |
|---|---|---:|---|---|
| **Direct commit** of all worker segments | Models may be independent; the orchestrator must still enforce one field/dimension/distance/stage contract | None | Search fans out over every worker segment | Fast publication, modest segment count, independent model scope |
| **One physical merge** | Shared compatible models | Reads all source auxiliary files and writes one new segment | Minimum fanout | A bounded one-level finalization when coordinator I/O is acceptable |
| **Hybrid grouping** into several merged segments | Shared compatible models within every merge group | Rewrite per group | Bounded fanout while retaining merge parallelism | Large builds where one final merge is a bottleneck |

Calling `merge_existing_index_segments` with one input returns the same
physical segment and UUID without rewriting its files, including its field and
source-version provenance. With multiple inputs it creates a new UUID whose
bitmap is the union of the inputs. The source directories remain unreferenced
after the new segment is committed and are reclaimed only by a later cleanup
policy that explicitly permits deleting unverified files, or by an external
garbage collector.

In Lance v14.0.2, do **not** form a recursive PQ or RQ merge tree through
`merge_existing_index_segments`. A first merge transposes PQ codes or packs
RQ codes into final query layout, while
the current merge kernel accepts only row-major/unpacked worker inputs. The
proposal [#7730](https://github.com/lance-format/lance/pull/7730) closed
without merge. The Rust optimizer introduced by
[#9871](https://github.com/lance-format/lance/pull/9871) uses its own vector
merge path and can consume normal query-layout segments; it does not remove
this restriction from the creation merger. See section 9.

### 2.7 Commit exactly once

Open or refresh a coordinator dataset handle to the latest allowed version,
repeat the source-validity checks, and call
`commit_existing_index_segments(index_name, column, final_segments)`.

The commit creates one transaction and one new manifest version. Readers see
either the old logical index set or the complete new set; they do not see a
partially published distributed build.

After an ambiguous client failure, inspect the latest manifest by logical name
and expected UUIDs before retrying. Do not assume that a timeout means the
commit failed.

---

## 3. Direct segment sets versus physical merge

### 3.1 Why independent models work when segments stay separate

A logical vector index is a set of same-name physical segments. At query time,
Lance:

1. opens each compatible segment by UUID;
2. ranks that segment's IVF partitions using its own centroids;
3. decodes/searches with that segment's own quantizer metadata;
4. merges row candidates from all segments by `_distance`;
5. flat-scans any uncovered fragments unless the caller requests indexed-only
   search behavior.

IVF partition numbers therefore have only segment-local meaning. Independent
training is not inherently incorrect as long as model state stays attached to
the segment that encoded the rows.

The cost is fanout. `nprobes` applies to each searched segment, so segment count
multiplies partition ranking, partition reads, candidate merging, cache entries,
and object-store requests. The scale problem is visible in
[#6860](https://github.com/lance-format/lance/issues/6860), which reports severe
ANN latency with hundreds of distributed IVF-PQ delta segments.

### 3.2 Why physical merge requires shared semantics

Physical merge writes one auxiliary store and one root model. Codes copied from
every input must have the same meaning under that one model. The merge path
checks, as applicable:

- index type, distance metric, dimension, and IVF partition count;
- IVF centroids (bitwise equality or a small `1e-5` numeric tolerance);
- PQ shape, bit width, subvector count, and codebook equality;
- RQ bit width, rotated dimension, rotation type, and rotation data;
- HNSW index metadata and build parameters.

The merge also requires disjoint input fragment bitmaps, one field ID, and
unique UUIDs.

Each source is filtered to rows it still owns before the new auxiliary store
is written. This prevents stale rows left in immutable segment files after
column updates or coverage pruning from being copied back into a merged index.
For stable row IDs, ownership is resolved using the source fragments' live
row-ID sequences. This filtering does not replace shared-model validation.

### 3.3 Compatibility matrix by vector index type

| Index type | Direct multi-segment commit | Requirements for physical merge | Current caveat |
|---|---|---|---|
| `IVF_FLAT` | Independent IVF models are valid | Same IVF centroids, distance, dimension, and partition count | Final merge copies raw vectors/row IDs into one partitioned store. |
| `IVF_PQ` | Independent IVF and PQ models are valid | Same IVF centroids and PQ codebook/configuration | Specialized distributed build is V3. Current final PQ layout cannot be fed into a second merge. |
| `IVF_SQ` | Independent IVF and SQ models are valid as separate segments | Same IVF centroids **and identical learned SQ bounds** | There is no public shared/pretrained SQ-bounds input. The current merger checks SQ dimension but reuses the first segment's bounds, so physically merging independently trained SQ segments can silently misdecode other segments' codes. Keep them separate. |
| `IVF_RQ` | Independent rotations are valid as separate segments | Same IVF centroids, `num_bits`, and exact RaBitQ rotation model | Broadcast Python `rabitq_model` or Rust rotation state. Default `num_bits` is 5. Current final packed RQ layout cannot be fed into a second creation merge. Java has no shared-rotation setter in v14.0.2. |
| `IVF_HNSW_FLAT` | Independent segment models/graphs are valid | Same IVF and HNSW parameters | Merge discards per-worker graph topology and rebuilds one HNSW graph per merged partition. |
| `IVF_HNSW_PQ` | Independent models/graphs are valid | Same IVF, PQ, and HNSW parameters | Pays PQ rewrite plus HNSW graph rebuild; no recursive PQ creation merge in v14.0.2. |
| `IVF_HNSW_SQ` | Independent models/graphs are valid as separate segments | Same IVF, SQ bounds, and HNSW parameters | Has the same unsafe SQ-bounds gap, plus graph rebuild. Keep independently trained outputs separate. |

This matrix describes `merge_existing_index_segments`. The SQ behavior is an
implementation caveat, not a recommended model scope; shared-bounds input and
validation remain absent from this creation merger. The optimizer's separate
vector path and compatibility checks are described in section 9.

---

## 4. Public API surface

### 4.1 Rust

The canonical Rust API is the `DatasetIndexExt` trait:

| Phase | API | Result |
|---|---|---|
| Worker build | `create_index_builder(...).name(...).fragments(ids).execute_uncommitted()` | `IndexMetadata` without a manifest commit |
| Optional merge | `merge_existing_index_segments(Vec<IndexMetadata>)` | One new `IndexMetadata`, or the same physical UUID without a rewrite for a one-element input |
| Publish | `commit_existing_index_segments(name, column, segments)` | Atomic dataset manifest update |
| Optimize existing indices | `lance::index::optimize::{plan_optimize_indices, commit_optimize_indices}` with task/merge `execute` methods | Serializable Rust plan and result records; see section 9 |

For a shared IVF-PQ worker build, construct the same parameters on every
worker with `IvfBuildParams::try_with_centroids(...)` and
`PQBuildParams::with_codebook(...)`, then use
`VectorIndexParams::with_ivf_pq_params(...)`.

The returned `IndexMetadata` is the durable coordinator input. Do not replace
it with a hand-built UUID/fragment tuple unless all `index_details`, version,
and file-format invariants are also preserved.

### 4.2 Python

| Phase | API | Notes |
|---|---|---|
| Shared IVF | `IndicesBuilder.train_ivf(...)` | Returns an `IvfModel`; supports selecting training fragments on the CPU path. |
| Shared IVF-PQ | `IndicesBuilder.prepare_global_ivf_pq(...)` | Returns Arrow `ivf_centroids` and `pq_codebook`. |
| Shared RQ rotation | `lance.lance.indices.build_rq_model(...)` | Returns the JSON `rabitq_model` to broadcast. |
| Worker build | `LanceDataset.create_index_uncommitted(..., fragment_ids=...)` | Public distributed-build API; returns the picklable `Index` dataclass. |
| Optional merge | `LanceDataset.merge_existing_index_segments(segments)` | Rewrites one caller-selected group. |
| Publish | `LanceDataset.commit_existing_index_segments(name, column, segments)` | Accepts `Index` or `IndexSegment` objects. |

An illustrative IVF-PQ orchestration skeleton is:

```python
import lance
from lance.indices import IndicesBuilder

uri = "s3://bucket/table.lance"
column = "vector"
index_name = "vector_ivf_pq"
num_partitions = 4096
num_subvectors = 32

# Coordinator: pin one source contract.
source = lance.dataset(uri)
source_version = source.version
target_ids = {fragment.fragment_id for fragment in source.get_fragments()}
fragment_groups = partition_disjointly(target_ids)  # external scheduler logic

models = IndicesBuilder(source, column).prepare_global_ivf_pq(
    num_partitions=num_partitions,
    num_subvectors=num_subvectors,
    distance_type="l2",
)

def build_segment(fragment_ids):
    worker = lance.dataset(uri, version=source_version)
    return worker.create_index_uncommitted(
        column=column,
        index_type="IVF_PQ",
        name=index_name,
        metric="L2",
        num_partitions=num_partitions,
        num_sub_vectors=num_subvectors,
        ivf_centroids=models["ivf_centroids"],
        pq_codebook=models["pq_codebook"],
        fragment_ids=fragment_ids,
    )

# Spark/Ray/Dask/etc. supplies this map and transports the returned Index values.
worker_segments = distributed_map(build_segment, fragment_groups)

# External barrier checks.
covered = set().union(*(segment.fragment_ids for segment in worker_segments))
assert covered == target_ids
assert sum(len(segment.fragment_ids) for segment in worker_segments) == len(covered)
assert len({segment.uuid for segment in worker_segments}) == len(worker_segments)

coordinator = lance.dataset(uri)  # refresh latest before validating and committing
live_ids = {fragment.fragment_id for fragment in coordinator.get_fragments()}
if not target_ids <= live_ids:
    raise RuntimeError("source fragments changed while the index was building")

# Option A: commit worker segments directly.
final_segments = worker_segments

# Option B: merge bounded caller-defined groups. Do not recursively merge the
# resulting IVF_PQ segments through this creation merger in Lance v14.0.2.
# final_segments = [
#     coordinator.merge_existing_index_segments(group)
#     for group in choose_merge_groups(worker_segments)
# ]

coordinator.commit_existing_index_segments(index_name, column, final_segments)
```

`partition_disjointly`, `distributed_map`, source-change detection, and
`choose_merge_groups` are intentionally external. A production implementation
must persist their decisions rather than reconstructing them after failures.

Advanced Python inputs such as `ivf_centroids_file` and
`precomputed_partition_dataset` can reduce repeated work, but they do not
change the segment/commit protocol or compatibility requirements.

### 4.3 Java

Java uses the same native protocol, with slightly different naming:

| Phase | API | Notes |
|---|---|---|
| Shared IVF training | `VectorTrainer.trainIvfCentroids(...)` | Broadcast flattened `float[]` centroids. The distance type used for training must match the worker build. |
| Shared PQ training | `VectorTrainer.trainPqCodebook(...)` | Broadcasts a flattened `float[]`, but is not IVF-residual-aware at this snapshot. For L2/Cosine IVF-PQ, use an externally trained residual codebook or gate deployment on recall; monitor #6363. |
| Worker parameters | `IvfBuildParams.Builder.setCentroids(...)`, `PQBuildParams.Builder.setCodebook(...)` | Reconstruct identical `VectorIndexParams` on every worker. |
| Worker build | `Dataset.createIndex(IndexOptions...withFragmentIds(...))` | Supplying fragment IDs selects uncommitted fragment-level behavior and returns `Index`. There is no separately named Java `createIndexUncommitted` method. |
| Worker progress | `Dataset.createIndex(options, IndexBuildProgress)` | Reports native build stages, including fragment-scoped builds; the callback must be thread-safe. |
| Optional merge | `Dataset.mergeExistingIndexSegments(List<Index>)` | Rewrites one group. |
| Publish | `Dataset.commitExistingIndexSegments(name, column, segments)` | Returns the committed metadata list. |

In Lance v14.0.2 Java `RQBuildParams` exposes `numBits` (default 5) but not a prebuilt
rotation, so separate Java-built IVF-RQ segments should be committed without a
physical merge. For IVF-PQ, supplying an externally trained residual codebook
through `setCodebook` is safe; the gap is specifically in the Java training
helper. Also ignore the stale comment in `IndexOptions.withIndexUUID` that says
workers should share a UUID: current segment semantics require a unique UUID
per physical worker output.

Java can restrict vector search to selected segment UUIDs through
`ScanOptions.Builder.indexSegments(List<UUID>)` (#7169). This supports external
query routing; it does not change worker grouping or the publication protocol.

### 4.4 Legacy APIs and terminology to avoid

Older code and issues may refer to staging directories, partial indices,
`merge_index_metadata`, `mergeIndexMetadata`, one shared UUID, or an
`IndexSegmentBuilder`. The canonical current protocol is
**create uncommitted → optionally merge existing segments → commit existing
segments**. The old shared-directory finalize model was intentionally removed.

Draft [#7053](https://github.com/lance-format/lance/pull/7053), which proposed
rejecting caller-provided UUIDs for segmented builds more broadly, is closed
without merge. [#7032](https://github.com/lance-format/lance/issues/7032) remains
open. Omit `index_uuid` or allocate one unique UUID per attempt; never let
concurrent attempts write the same directory.

---

## 5. Files written by workers and mergers

A worker output is self-contained:

```text
dataset.lance/
└── _indices/
    └── <worker-segment-uuid>/
        ├── index.idx       # IVF/root/sub-index metadata
        └── auxiliary.idx   # row IDs and vectors or quantized codes by partition
```

`IndexMetadata.fragment_bitmap` associates that directory with source
fragments. The directory is invisible to readers until a manifest references
it.

A multi-input physical merge writes a different directory:

```text
_indices/
├── <worker-uuid-0>/        # source, later unreferenced
├── <worker-uuid-1>/        # source, later unreferenced
└── <merged-uuid>/          # candidate final segment
    ├── index.idx
    └── auxiliary.idx
```

Internally, the merge proceeds through these progress stages:

1. `read_shard_metadata`: open every source, identify storage type, read IVF
   lengths/models, and validate compatibility;
2. `merge_partitions`: filter each source to its owned rows, then stream the
   surviving rows/codes for each IVF partition into a unified auxiliary writer;
3. `write_auxiliary_index`: finish the new `auxiliary.idx`;
4. `rebuild_hnsw_graph`: for HNSW composites, build a new graph for each merged
   partition from the combined storage;
5. `write_root_index`: write the unified IVF/root `index.idx`.

For PQ, distributed workers deliberately store row-major codes and the merge
transposes them once into query layout. For RQ, workers store unpacked codes and
the merge packs them once. Those one-way transforms explain the current
non-recursive merge restriction.

The auxiliary writer preserves the first source file's exact Lance container
version. That identity is separate from the vector index's layout version;
the new-dataset default of 2.2 does not silently upgrade merge inputs.

PQ/RQ merge reads are partition-windowed to bound memory and request setup:

| Environment variable | Default | Effect |
|---|---:|---|
| `LANCE_IVF_PQ_MERGE_PARTITION_WINDOW_SIZE` | `512` | IVF partitions read per window |
| `LANCE_IVF_PQ_MERGE_PARTITION_PREFETCH_WINDOW_COUNT` | `2` | Windows prefetched concurrently |

The variable names predate RQ support. Flat and SQ (including their HNSW
variants) stream one source partition at a time, keeping ownership filtering
from materializing a multi-partition window. The proposal to window those reads,
[#7761](https://github.com/lance-format/lance/pull/7761), closed without merge;
do not assume that optimization is present in this API.

Worker shuffle uses local temporary storage while durable outputs go through
the dataset's object store. Every worker and coordinator therefore needs
consistent access to the dataset URI and storage options, but they do not need
a shared local filesystem.

---

## 6. Validation and commit semantics

### 6.1 Checks performed by Lance

The current segment APIs enforce the following layers.

**Worker build:**

- one indexed vector column with a supported type/dimension;
- supplied fragment IDs exist in the worker's opened snapshot;
- precomputed IVF/PQ/RQ artifacts have compatible shapes and parameters;
- the fragment filter is applied during both training and encoding for a
  subset-trained segment.

**Physical merge:**

- non-empty input, unique UUIDs, and disjoint fragment coverage;
- identical one-field target and one supported index family;
- compatible IVF/model/storage metadata as described in section 3;
- per-source row ownership filtering before rows enter the merged storage;
- expected intermediate PQ/RQ code layout.

**Commit:**

- non-empty final set, unique UUIDs, disjoint coverage, and present details;
- the requested column exists, every segment's preserved keyed field matches
  that column's field ID, and an existing same-name index on another keyed
  field is rejected;
- a segment's preserved `dataset_version` is not in the future, and historical
  versions needed for validation can be opened;
- coverage for retired fragments or fragments whose indexed-field base-file
  paths changed since the build version is pruned before publication;
- all direct-commit segments have the same index-details type URL;
- keyed/carried field metadata is well-formed and consistent across incoming
  and retained segments; vector builds have one keyed field and no carried fields;
- the segment files can be listed concurrently and recorded in the manifest.

`IndexSegment` carries UUID, coverage, `fields`, `covering_fields`, index details,
index version, and `dataset_version`. Rust conversion from `IndexMetadata` and the
Python/Java conversions from returned `Index` objects preserve all of that
physical provenance. Commit supplies only the logical name and requested
column, validates the preserved field against that column, and retains the
segment's source version in the new manifest entry.

The numeric source version is useful but not an external job identity. It does
not prove model-artifact hashes, intended coverage, or which retry produced an
otherwise valid directory. Newer overlay values are intentionally handled by
query-time masking rather than pretending the old segment incorporated them.
Field/version validation therefore complements rather than replaces the
external semantic-provenance barrier.

### 6.2 Atomic replacement rules

Committing under an existing logical index name supports delta addition and
whole-segment replacement:

- existing same-name segments disjoint from the incoming coverage are kept;
- an existing segment that overlaps incoming coverage is removed only when the
  incoming set covers **all** fragments in that existing segment;
- partial overlap is rejected because removing the old segment would orphan its
  uncovered fragments;
- a legacy segment with unknown coverage can be replaced only by full-dataset
  incoming coverage;
- a zero-fragment placeholder can be replaced by real incoming segments;
- changing the index-details type under an existing name requires full current
  fragment coverage and replaces every old segment; partial type changes are
  rejected, including when the old type cannot be recognized by this reader.

This means a wide physical segment is the unit of replacement. Rebuilding one
fragment inside a 100-fragment segment requires rebuilding/replacing coverage
for all 100 fragments or first using a supported optimization workflow.

### 6.3 What Lance does not currently prove

The external coordinator must still check:

- that the union equals the orchestrator's intended coverage, rather than just
  being internally disjoint;
- that the pinned source version is the intended build source and remains
  available for commit validation;
- that any coverage pruned because a fragment retired or its indexed file path
  changed is handled according to the job's exact-coverage policy;
- that vector values and schema changes during the long-running job are
  acceptable under the chosen rebuild/overlay-fallback policy;
- that all outputs belong to the same external job and parameter/model hashes;
- that vector dimension, distance metric, and ordered stage configuration agree
  across direct-commit segments; the shared vector-details type URL does not
  prove this;
- that an SQ physical merge uses identical learned bounds;
- that a retry did not leave an obsolete but otherwise structurally valid
  segment in the candidate list.

Direct commit intentionally does not demand equal centroids or codebooks: that
would prohibit the supported independent-segment model. Deeper equality belongs
to physical merge, where model state is collapsed into one segment.

---

## 7. Failure handling, retries, and garbage collection

### 7.1 Persist an external job manifest

A recoverable orchestrator should persist a record similar to:

```text
build_id
dataset URI + branch + source version
logical index name + field ID + column + dimension
metric + complete index parameters
serialized model artifact locations + hashes
target fragment IDs
worker groups
worker attempt -> status, UUID, returned metadata, timestamps
selected successful attempt per group
merge groups -> source UUIDs, result UUID, status
commit intent -> final UUIDs
committed dataset version (when known)
```

Without this record, uncommitted directories have no owner information and are
indistinguishable from abandoned work by looking only at object storage.

### 7.2 Retry rules

- Prefer a new UUID for every worker or merge attempt. If an earlier attempt
  later succeeds, select exactly one result and leave the other unreferenced.
- Never run concurrent retries against the same UUID; they can race while
  writing the same `index.idx` or `auxiliary.idx` objects.
- A failed or cancelled call does not roll back already written objects.
- Do not include two retry outputs for the same fragment group; overlap
  validation will reject them.
- Treat merge as another retryable task with its own new UUID and persisted
  input UUID list.
- After a commit timeout, inspect `describe_indices` / manifest metadata for the
  expected UUID set before issuing another commit.

### 7.3 Cleanup

Uncommitted worker outputs, superseded retries, source directories of a merge,
and merged-but-never-committed outputs remain under `_indices/` without a
manifest reference. Lance classifies objects that never appeared in a manifest
as **unverified**, because storage inspection cannot distinguish abandoned
output from an in-progress operation. `cleanup_old_versions(...)` preserves
unverified files by default. They become deletion candidates only when the
caller opts into unverified-file deletion (for example,
`delete_unverified=True`) and the age policy permits it, or when an external
garbage collector removes them.

Operational rules:

- set the cleanup age comfortably longer than the longest build and recovery
  window;
- enable unverified-file deletion only after proving that no active worker or
  merger can still depend on an eligible uncommitted directory;
- mark a job terminal and preserve its final UUID list before allowing cleanup;
- remember that cleanup also affects dataset version recovery, not only index
  scratch data.

There is no built-in lease that protects an active uncommitted segment from an
overly aggressive external cleanup policy.

---

## 8. Scale, memory, and topology choices

### 8.1 Worker scaling

Worker parallelism scales the expensive scan, assignment, quantization, and
sub-index construction over disjoint fragment groups. Shared model training can
remain a centralized front-end bottleneck. Open
[#7321](https://github.com/lance-format/lance/pull/7321) is the main work to
distribute k-means rounds without coupling Lance to a scheduler.

Worker memory is affected by training samples, shuffle buffering,
`shuffle_partition_batches`, `shuffle_partition_concurrency`, partition size,
and HNSW construction. Streaming IVF controls bound training chunks/coresets,
and `LANCE_SHUFFLE_MAX_PRELOADED_OFFSETS_BYTES` bounds shuffle offset
preloading. Neither is a memory limit for the entire build. The broader
memory-pool effort is tracked by
[#7301](https://github.com/lance-format/lance/issues/7301), vector-specific
[#7305](https://github.com/lance-format/lance/issues/7305). Draft
[#7312](https://github.com/lance-format/lance/pull/7312) closed without merge.
Do not assume every stage is accounted by one pool or spillable; HNSW has
distinct memory behavior.

### 8.2 Merge scaling

A one-segment final merge can become a new serial bottleneck after a highly
parallel worker phase. It reads all source auxiliary data, writes it again, and
for HNSW rebuilds graphs. Hybrid grouping is often a better intermediate
topology, provided the resulting segment count satisfies query latency goals.

Monitor at least:

- worker rows/bytes per second and retry rate;
- temporary-disk high-water mark and memory high-water mark;
- source and destination object-store bytes/requests;
- merge duration by progress stage;
- final segment count and rows/bytes per segment;
- ANN recall against an exact sample;
- query partitions read, cold-cache latency, and cache footprint.

### 8.3 Query cost is part of the build decision

A successful build is not complete if its topology makes queries unusable.
Because every segment independently applies `nprobes`, hundreds of small delta
segments can turn one ANN query into millions of partition comparisons and
many object-store reads. Choose a segment budget up front and trigger
optimization before the budget is exceeded.

Lance can share partition reads across eligible fixed-probe batch queries.
v14 automatic probing covers L2/cosine/Dot through `k=100000` across IVF
types, and `search_effort` tunes the initial budget. Neither
removes segment fanout; measure the actual fixed/automatic probe policy and
batch shape you plan to serve. See `03-index-on-disk-and-search.md` §4.

There is no universal best segment size. It depends on partition count, object
store latency, update rate, retry cost, model drift, and whether HNSW graph
rebuild is affordable. Benchmark the expected steady-state segment count, not
only a freshly consolidated index.

[Discussion #6189](https://github.com/lance-format/lance/discussions/6189)
explores a target segment-byte budget and LSM-like consolidation of fragments
and indices. It also debates concepts such as active versus sealed segments.
Those are useful scheduler design directions, not fields or guarantees in the
current public create/merge/commit contract.

---

## 9. Incremental creation and distributed optimization

For externally orchestrated appended data, build uncommitted segments only for
uncovered fragments and commit them under the existing logical name. Disjoint
existing segments remain in the manifest. Queries search the old and new
segments together.

There are two model choices:

- train an independent delta model and keep the delta as its own segment;
- reuse the committed index's global model so the delta can later be physically
  merged with it.

The second choice needs public access to the committed IVF centroids and PQ
codebook. That API gap is tracked by RFC
[#7319](https://github.com/lance-format/lance/issues/7319), motivated in part by
distributed Spark indexing.

For the unbounded built-in path, `OptimizeOptions::append()` is
segment-set-native. It builds one new physical segment over only the unindexed
fragments, using the complete model of the deterministic last manifest segment,
and preserves every existing segment and its coverage. Valid heterogeneous
segment sets can append even when their IVF or quantizer models differ. An
explicit merge validates only the requested
compatible suffix, while explicit retrain source-rebuilds the whole current
coverage and unifies it into one model. This landed in
[#8047](https://github.com/lance-format/lance/pull/8047), closing
[#6398](https://github.com/lance-format/lance/issues/6398).

Default optimization also adjusts partition sizes using the persisted
`target_partition_size`, or the index-type default when that value is absent.
Lance can split one oversized partition into multiple pieces toward the target,
or join undersized partitions when no split is performed (#9051). These
operations may change centroids, re-encode affected rows, and consolidate
compatible segments. Explicit append skips this adjustment. An explicit
positive merge count skips it by default, but Rust's
`OptimizeOptions::merge(n).with_partition_rebalancing()` enables adjustment
within that suffix (#10076). Earlier segments remain untouched. This option
rejects a zero merge count and cannot be combined with bounded task/segment
sizes, because distributed builds must keep the reference model unchanged.

### 9.1 Rust plan/build/merge/commit optimization

[#9871](https://github.com/lance-format/lance/pull/9871), included in v14,
adds `lance::index::optimize`. The ordinary `optimize_indices` call runs the
same stages locally; an external engine can distribute them:

```text
driver:  plan_optimize_indices(dataset, options)
             -> OptimizeIndicesPlan { plan_id, read_version, tasks, merges }
workers: task.execute(dataset)
             -> Option<IndexSegmentResult>
workers: merge.execute(dataset, task_results)
             -> Option<IndexSegmentResult>
driver:  commit_optimize_indices(dataset, merged_results, options)
```

`OptimizeIndexTask`, `OptimizeIndexMerge`, and `IndexSegmentResult` are
serializable Rust records. They retain the plan ID, read version, group, model
reference or resulting segment, and replacement UUIDs as appropriate.
Tasks reopen the planned `read_version`; merges reject results from another
plan. Commit validates a shared read version and replacement/coverage
invariants and publishes one `CreateIndex` transaction anchored to that
version. The external barrier should also enforce one plan ID and select
one successful attempt per task before committing results.

The split controls live on Rust `OptimizeOptions`:

| Option | Effect |
|---|---|
| `max_rows_per_task` | Packs unindexed fragments into bounded build tasks. |
| `max_rows_per_segment` | Packs existing segments and new task outputs into bounded output groups; also limits task grouping. |
| `num_indices_to_merge` | Selects the latest compatible committed segments to absorb; unset on a split vector build means append only. |
| `retrain` | Rebuilds source coverage using a newly trained model. |

Fragments and existing segments are indivisible. An input larger than a row
target occupies its own group and can exceed it; these are grouping controls,
not hard memory or byte limits. With neither row bound set, each logical
index is one task and one pass-through merge. Split vector tasks encode
against a reference segment's model. A retrain, definition-only index, or
fully dormant index may train a model during planning and write a temporary
model segment outside the manifest. Account for that work and its cleanup.

Vector, BTree, Bitmap, NGram, and Inverted indices have merge primitives for
this planning framework. Kinds without such a primitive, and Inverted indices
on stable-row-ID tables, stay in one task even when bounds are supplied.
For otherwise splittable indices, legacy-v1 vector layouts, scalar indices
requiring a full-column rebuild, and tagged FRI histories are skipped with a
warning or rejected when explicitly named. Unknown tagged transitions can
cause optimization to be skipped entirely. Preserve those restrictions
rather than constructing tasks that bypass them.

Results may be partial: coverage that no committed result supplies remains
unindexed and is eligible for a later optimize. For retraining, this can
remove old segments and leave missing task coverage on the flat path. An
exact-coverage job must enforce its stronger barrier before commit.

### 9.2 The two vector merge paths

Optimization opens normal segment storage and copies partitions through
`optimize_vector_indices`. It can merge committed query-layout PQ/RQ segments
and its own task outputs; it does not call the creation merger's one-way
transpose/pack kernel. The selected segments must share a compatible model,
including quantizer state. Append can preserve heterogeneous segments;
retrain unifies them.

By contrast, `merge_existing_index_segments` still expects row-major PQ or
unpacked RQ worker inputs. Preserve original worker outputs for that API and
use only one physical merge level. PR #7730 closed without merge; #9871
provides distributed optimization, not recursive creation-merge support.
The plan/task/merge/commit API and row-bound controls are Rust-only at
v14.0.2; Python and Java retain their existing optimize and creation APIs.

---

## 10. Progress, verification, and benchmarks

### 10.1 Progress reporting

`IndexBuildProgress` is threaded through build/merge internals. Worker builds
can surface it through binding callbacks. Java exposes
`Dataset.createIndex(options, progress)` for regular and fragment-scoped vector
builds; callbacks receive stage start, progress, and completion events and must
be thread-safe. Relevant internal
stage names include:

| Phase | Stages |
|---|---|
| Worker build | `train_ivf`, `train_quantizer`, `shuffle`, `merge_partitions` |
| Physical merge internals | `read_shard_metadata`, `merge_partitions`, `write_auxiliary_index`, `rebuild_hnsw_graph`, `write_root_index` |

The canonical `merge_existing_index_segments` API currently invokes the vector
merge with a no-op progress reporter and does not expose a merge callback in
Rust, Python, or Java. The stage names are useful for profiling and future
instrumentation, but an orchestrator currently sees only task-level merge
status unless it adds instrumentation below the public API. The legacy
`merge_index_metadata` callback is not a substitute for the segment-native
vector workflow.

Even where callbacks are exposed, they report work inside one process.
Aggregating worker progress, heartbeats, timeouts, and task state remains the
scheduler's job.

Public optimizer task/merge `execute` methods also use no-op progress;
their callback-aware variants are crate-private. Serializing a plan does
not provide cross-worker progress aggregation.

### 10.2 Required acceptance checks

A production distributed build should verify:

- exact intended fragment coverage and no overlap;
- expected number and type of committed segments via `describe_indices`;
- all final UUIDs are referenced and non-final source/retry UUIDs are not
  accidentally referenced;
- row count and filtered-query correctness;
- recall against brute-force neighbors on a representative sample (repository
  tests require a meaningful recall assertion, commonly at least `0.5`);
- multiple-fragment, NULL-vector, NaN/filtering, empty-partition, and deletion
  cases relevant to the chosen index;
- latency and I/O at the intended final segment count;
- restart behavior after worker failure, merge failure, and ambiguous commit.

### 10.3 Repository coverage

| Coverage | Location |
|---|---|
| Python distributed vector variants, direct commit, physical merge, recall, shared model artifacts, layout windows | `python/python/tests/test_vector_index.py` |
| Rust segment validation, replacement semantics, logical multi-segment query, vector merge variants, HNSW rebuild, progress | `rust/lance/src/index.rs`, `rust/lance/src/index/vector.rs`, `rust/lance/src/index/vector/ivf.rs` |
| Merge-kernel format and compatibility tests | `rust/lance-index/src/vector/distributed/index_merger.rs` |
| Rust optimization plans, task grouping, serializable records, partial commit, and vector retrain | `rust/lance/src/index/optimize.rs` |
| Java IVF-FLAT/PQ/SQ fragment builds, direct multi-segment commit, build callbacks, and segment-selected search | `java/src/test/java/org/lance/index/VectorIndexTest.java`, `java/src/test/java/org/lance/ScannerTest.java` |
| Merge-only IVF-PQ scaling across shard and partition counts | `rust/lance/benches/distributed_vector_build.rs` |

The benchmark isolates finalization; it does not measure scheduler overhead,
worker scan/build time, commit latency, or steady-state multi-segment query
cost. Record those separately in an end-to-end environment.

---

## 11. Current limitations and sharp edges

| Area | Current state in the v14.0.2 snapshot | Operational response |
|---|---|---|
| Scheduler | Lance has no end-to-end distributed scheduler | Integrate the segment protocol into an external engine and persist a job manifest. |
| Direct-commit compatibility | Commit compares the details type URL, not the complete vector dimension/metric/stage contract | Enforce the pinned contract and parameter hashes at the external barrier. |
| Shared IVF/PQ training | Convenient but centralized helper paths | Use them for correctness now; monitor #7321 for distributed k-means. |
| Java IVF-PQ training | `trainPqCodebook` does not receive IVF centroids, although L2/Cosine PQ encodes residuals | Train the residual codebook through an IVF-aware surface or require recall validation; revive/replace closed #6363. |
| SQ creation merge | Merger reuses first bounds without equality validation; no shared-bounds public input | Keep independently trained IVF-SQ/HNSW-SQ segments separate; the optimizer has separate model validation. |
| Recursive PQ/RQ creation merge | Final-layout inputs are rejected by `merge_existing_index_segments` | Use one creation merge level. #7730 closed unmerged; the separate Rust optimizer supports normal segment layouts. |
| Distributed optimization | Rust plan/task/merge/commit API is available; bounded builds preserve the reference model and restrict FRI/layout support | Persist the plan, enforce the task barrier, and account for planning-time training and partial coverage. Python/Java do not expose these stages. |
| Flat/SQ/HNSW creation merge I/O | Per-partition/shard setup can be expensive; #7761 closed unmerged | Benchmark high shard/partition counts against the actual release implementation. |
| HNSW merge | Correctness rebuild exists, but graph construction is expensive | Budget the rebuild explicitly; do not rely on old tracker text saying the graph is discarded. |
| Source provenance at commit | Field and numeric dataset-version provenance are preserved; obvious stale coverage is pruned, but external job/model identity and policy remain caller-owned | Pin a version, inspect intervening changes, refresh the coordinator, and enforce the intended coverage/model contract. |
| UUID safety | Some bindings still permit caller UUIDs and Java has a stale shared-UUID comment; #7053 closed without merge | Use a unique UUID per attempt or omit it; monitor #7032 for replacement work. |
| Existing-model reuse | Public committed IVF/PQ extraction is incomplete | Monitor #7319; otherwise keep an external copy of original model artifacts. |
| Many committed segments | Correct but query fanout can dominate | Set a segment budget and consolidate before reaching it; monitor #6860/#7731. |
| Artifact ownership | Uncommitted directories contain no job/lease metadata and default cleanup preserves them as unverified | Track ownership externally; use conservative, explicit unverified-file cleanup only after excluding active jobs. |
| Java RQ merge | No prebuilt rotation input in Java v14.0.2; default bit width is 5 | Commit RQ worker outputs separately or use a language surface that can broadcast rotation state. |

---

## 12. GitHub progress tracker

Issue and PR states below were checked through the GitHub API on
**2026-10-29**. Landed capabilities were checked against `40f15ed1b`; an open
proposal is not part of the v14.0.2 API. Re-check states before using this document
as a release plan.

### 12.1 Primary design and implementation work

| Item | Snapshot status | Why it matters / next signal |
|---|---|---|
| [Discussion #6189 — vector index multi-segment final state](https://github.com/lance-format/lance/discussions/6189) | Discussion | Foundational move away from many-to-one finalize toward segment-native manifests and queries. |
| [Issue #6309 — distributed indexes search tracker](https://github.com/lance-format/lance/issues/6309) | Open/reopened | Canonical umbrella for segment creation/search. Its checklist contains stale details; compare with landed PRs below. |
| [Issue #6399 — segmented index reader](https://github.com/lance-format/lance/issues/6399) | Open | Reader-side segment abstraction; relevant to query fanout and logical index behavior. |
| [Issue #7731 — distributed optimization of vector delta segments](https://github.com/lance-format/lance/issues/7731) | Open | Tracks the many-delta problem; compare its original proposal with the Rust optimizer shipped in #9871. |
| [PR #7730 — distributed segment merge planning](https://github.com/lance-format/lance/pull/7730) | Closed, unmerged | Historical proposal; do not infer recursive creation-merge support or Python planner APIs from it. |
| [PR #9871 — distributed execution of optimize indices](https://github.com/lance-format/lance/pull/9871) | Merged; in v14.0.2 | Rust serializable plan/build/merge/commit stages, row grouping, shared-model builds, and a separate optimizer merge path. |
| [PR #7761 — window IVF flat merge reads](https://github.com/lance-format/lance/pull/7761) | Closed, unmerged | Proposed creation-merge I/O optimization; benchmark the release path rather than assuming this work landed. |
| [PR #7321 — scheduler-neutral distributed centroid training](https://github.com/lance-format/lance/pull/7321) | Open, non-draft | Adds partial-stat/Lloyd-round primitives and binding support for distributed k-means orchestration. |
| [RFC #7319 — public IVF/PQ model read APIs](https://github.com/lance-format/lance/issues/7319) | Open | Needed to extend a committed global-model index from external engines without retraining. |
| [Issue #6860 — slow ANN with hundreds of distributed deltas](https://github.com/lance-format/lance/issues/6860) | Open | Concrete query-fanout pressure that should guide segment budgets and optimization. |
| [Epic #7301](https://github.com/lance-format/lance/issues/7301), [vector task #7305](https://github.com/lance-format/lance/issues/7305), [draft PR #7312](https://github.com/lance-format/lance/pull/7312) — build memory pools | Issues open; PR closed, unmerged | Memory accounting/spill work remains incomplete; streaming sample bounds are not a global build budget. |
| [Issue #7032](https://github.com/lance-format/lance/issues/7032) / [draft PR #7053 — reject unsafe user UUIDs for segmented builds](https://github.com/lance-format/lance/pull/7053) | Issue open / PR closed, unmerged | Directory-collision safeguards still require caller discipline; watch for replacement implementation work. |

Older question [#5359](https://github.com/lance-format/lance/issues/5359)
(unified versus separate distributed APIs) remains open. Issue
[#4155](https://github.com/lance-format/lance/issues/4155) was closed as not
planned after the canonical segment APIs landed. Two implementation proposals
also remain useful only as history: [#7806](https://github.com/lance-format/lance/pull/7806)
(IVF-HNSW-RQ) and [#6363](https://github.com/lance-format/lance/pull/6363)
(Java residual-aware PQ training) are both closed without merge. Lance v14.0.2 has
no `IVF_HNSW_RQ` variant, and the Java PQ-training gap described above remains.

### 12.2 Landed milestones

| PR | Landed capability |
|---|---|
| [#5117](https://github.com/lance-format/lance/pull/5117) | Initial distributed vector-index creation work. |
| [#5664](https://github.com/lance-format/lance/pull/5664) | Java distributed vector-index build, training helpers, and segment commit surface. |
| [#5834](https://github.com/lance-format/lance/pull/5834), [#6114](https://github.com/lance-format/lance/pull/6114) | PQ transposed-layout metadata fixes and explicit transpose control. |
| [#6209](https://github.com/lance-format/lance/pull/6209) | Segment commit API foundation. |
| [#6220](https://github.com/lance-format/lance/pull/6220) | Distributed vector segment build. |
| [#6269](https://github.com/lance-format/lance/pull/6269), [#6270](https://github.com/lance-format/lance/pull/6270) | Removed staging dependence and clarified logical versus physical segments. |
| [#6296](https://github.com/lance-format/lance/pull/6296) | Independent-centroid segment builds. |
| [#6313](https://github.com/lance-format/lance/pull/6313) | Aligned distributed build around segment-native APIs. |
| [#6358](https://github.com/lance-format/lance/pull/6358) | Applied fragment filters to worker training/build. |
| [#6359](https://github.com/lance-format/lance/pull/6359) | Distributed IVF-RQ support. |
| [#6176](https://github.com/lance-format/lance/pull/6176) | Distributed vector finalization benchmark. |
| [#6376](https://github.com/lance-format/lance/pull/6376) | Query pruning/selection by segment coverage. |
| [#6402](https://github.com/lance-format/lance/pull/6402) | Vector optimization produces bounded segment output per run. |
| [#6704](https://github.com/lance-format/lance/pull/6704) | Threads the selected distance metric through Java IVF/PQ trainers; it does not add residual-aware PQ training. |
| [#6997](https://github.com/lance-format/lance/pull/6997) | Removed the obsolete `IndexSegmentBuilder` API. |
| [#7014](https://github.com/lance-format/lance/pull/7014) | Python shared RaBitQ rotation model. |
| [#7129](https://github.com/lance-format/lance/pull/7129) | Avoided unnecessary object listing after index writes. |
| [#7148](https://github.com/lance-format/lance/pull/7148) | Documented shared versus independent vector model scopes. |
| [#7178](https://github.com/lance-format/lance/pull/7178) | Rebuilds HNSW graphs during physical segment merge. This supersedes the stale #6309 note that graphs are discarded. |
| [#7583](https://github.com/lance-format/lance/pull/7583) | Preserves non-default PQ `num_bits` through shared/distributed paths. |
| [#7768](https://github.com/lance-format/lance/pull/7768) | Trains subset builds only on selected fragments, closing an important model-correctness gap. |
| [#7932](https://github.com/lance-format/lance/pull/7932) | Preserves field and dataset-version provenance in `IndexSegment` and validates it during commit. |
| [#7657](https://github.com/lance-format/lance/pull/7657) | Lists segment artifact files concurrently during commit. |
| [#8011](https://github.com/lance-format/lance/pull/8011) | Prunes stale historical segment coverage when indexed data-file paths no longer match the current dataset. |
| [#8047](https://github.com/lance-format/lance/pull/8047) | Makes vector append segment-set-native, preserving valid heterogeneous segment sets and closing #6398. |
| [#8342](https://github.com/lance-format/lance/pull/8342) | Filters stale source rows during vector segment merge; recomputes partition lengths from surviving rows. |
| [#8351](https://github.com/lance-format/lance/pull/8351) | Applies per-segment ownership before partition top-k, including stable row IDs. |
| [#8936](https://github.com/lance-format/lance/pull/8936) | Changes new IVF-RQ builds to five-bit quantization across bindings. |
| [#8823](https://github.com/lance-format/lance/pull/8823) | Exposes Java `createIndex` stage callbacks for vector builds, including fragment-scoped workers. |
| [#7169](https://github.com/lance-format/lance/pull/7169) | Java `ScanOptions.Builder.indexSegments(...)` selects vector segments for externally routed searches. |
| [#9050](https://github.com/lance-format/lance/pull/9050) | Uses proportional hierarchical k-means for process-local IVF training. |
| [#9051](https://github.com/lance-format/lance/pull/9051) | Splits or joins IVF partitions toward the stored target size during local optimization. |
| [#9134](https://github.com/lance-format/lance/pull/9134) | Allows definition-only vector indices when a whole table cannot yet train the requested quantizer. |
| [#9434](https://github.com/lance-format/lance/pull/9434) | Bounds IVF shuffle-offset preloading through an environment setting. |
| [#9524](https://github.com/lance-format/lance/pull/9524), [#9946](https://github.com/lance-format/lance/pull/9946) | Trace effective segment coverage across compactions while checking indexed-data compatibility. |
| [#9871](https://github.com/lance-format/lance/pull/9871) | Exposes Rust distributed optimization plans, tasks, merges, and commit. |
| [#10076](https://github.com/lance-format/lance/pull/10076) | Allows partition rebalancing within explicit unbounded merge windows. |
| [#10188](https://github.com/lance-format/lance/pull/10188) | Trains an IVF model from caller-supplied samples without dataset sampling. |

### 12.3 Follow-ups to match with current trackers

The release source still leaves these creation-workflow gaps. The status
refresh above checks named items; it is not an exhaustive search for newer
tracking issues:

1. validating equal SQ bounds during creation merge and exposing shared SQ
   bounds to worker APIs;
2. carrying an external build/job identity through the creation segment API;
   the optimizer's `plan_id` and `read_version` records do not extend that API;
3. attaching external build ownership/lease information to uncommitted index
   artifacts;
4. exposing a shared RaBitQ rotation input in Java;
5. replacing #6363 so Java PQ training can consume the IVF model,
   plus adding distributed Java recall coverage rather than commit-only
   coverage.

Before filing, search for newer issues and link them here. If still absent,
these are concrete, independently actionable follow-ups rather than one broad
"distributed indexing" issue.

### 12.4 Refresh procedure

Use the following as a lightweight review checklist:

```bash
repo=lance-format/lance

for n in 6309 6399 7731 7319 6860 7301 7305 7032 5359; do
  gh issue view "$n" --repo "$repo" \
    --json number,title,state,updatedAt,url
done

for n in 7321 9871; do
  gh pr view "$n" --repo "$repo" \
    --json number,title,state,isDraft,mergedAt,updatedAt,url
done

# Closed historical items: re-check only if their design becomes relevant again.
gh issue view 6398 --repo "$repo" --json number,title,state,closedAt,url
gh issue view 4155 --repo "$repo" --json number,title,state,closedAt,url
gh pr view 7806 --repo "$repo" --json number,title,state,isDraft,mergedAt,url
gh pr view 6363 --repo "$repo" --json number,title,state,isDraft,mergedAt,url
gh pr view 7053 --repo "$repo" --json number,title,state,isDraft,mergedAt,url
gh pr view 7730 --repo "$repo" --json number,title,state,isDraft,mergedAt,url
gh pr view 7761 --repo "$repo" --json number,title,state,isDraft,mergedAt,url
gh pr view 7312 --repo "$repo" --json number,title,state,isDraft,mergedAt,url
```

Also review:

- [open distributed vector-index items](https://github.com/lance-format/lance/issues?q=is%3Aopen+distributed+vector+index);
- [open index-segment items](https://github.com/lance-format/lance/issues?q=is%3Aopen+%22index+segment%22);
- Discussion #6189 for design updates that do not appear in issue state.

When an open PR lands, update the source baseline first, verify its tests and
actual APIs in the checked-out code, and only then remove a limitation from
this document.

---

## 13. Source map

| Concern | Authoritative path |
|---|---|
| Worker dispatch and `execute_uncommitted` | `rust/lance/src/index/create.rs` |
| Public segment traits/types | `rust/lance/src/index/api.rs` |
| Merge/commit validation and replacement rules | `rust/lance/src/index.rs` |
| Distributed vector worker build and parameter stages | `rust/lance/src/index/vector.rs` |
| Shared build pipeline and progress stages | `rust/lance/src/index/vector/builder.rs` |
| Physical vector segment merge, root write, HNSW rebuild | `rust/lance/src/index/vector/ivf.rs` |
| Rust optimization plan/task/merge/commit | `rust/lance/src/index/optimize.rs` |
| Optimization options / partition rebalancing | `rust/lance-index/src/optimize.rs`, `rust/lance/src/index/vector/ivf.rs` |
| Historical coverage / FRI remapping | `rust/lance/src/index/frag_reuse_reader.rs` |
| Auxiliary storage merge, PQ transpose, RQ packing, compatibility checks | `rust/lance-index/src/vector/distributed/index_merger.rs` |
| SQ bounds used during decoding | `rust/lance-index/src/vector/sq/storage.rs` |
| Disk shuffler | `rust/lance-index/src/vector/v3/shuffler.rs` |
| Manifest segment metadata | `rust/lance-table/src/format/index.rs` |
| Python coordinator API | `python/python/lance/dataset.py` |
| Python model preparation | `python/python/lance/indices/builder.py` |
| Python native bridge | `python/src/dataset.rs`, `python/src/indices.rs` |
| Java coordinator API | `java/src/main/java/org/lance/Dataset.java` |
| Java parameters/training | `java/src/main/java/org/lance/index/vector/`, especially `VectorTrainer.java` |
| Java progress callbacks / segment selection | `java/src/main/java/org/lance/index/IndexBuildProgress.java`, `java/src/main/java/org/lance/ipc/ScanOptions.java` |
| Java native bridge | `java/lance-jni/src/blocking_dataset.rs`, `java/lance-jni/src/utils.rs`, `java/lance-jni/src/vector_trainer.rs` |
| User-facing guide | `docs/src/guide/distributed_indexing.md` |
| On-disk/query background | `docs/src/format/index/index.md`, `plans/docs/03-index-on-disk-and-search.md` |

---

## 14. Production runbook

Before build:

- [ ] Pin source version and vector-field contract.
- [ ] Decide independent versus shared model scope.
- [ ] Persist model artifacts and hashes.
- [ ] Persist exact, disjoint fragment groups and a segment-count budget.
- [ ] Ensure cleanup retention exceeds build/recovery duration.

For every worker:

- [ ] Open the pinned version.
- [ ] Use identical metric/stage parameters and shared artifacts where required.
- [ ] For Java L2/Cosine IVF-PQ, use an IVF-residual-trained PQ codebook or record an explicit recall acceptance result.
- [ ] Use a unique UUID/attempt identity.
- [ ] Persist the complete returned `Index` / `IndexMetadata`.
- [ ] Record progress, resource peaks, and terminal status.

At the barrier:

- [ ] Select one successful attempt per fragment group.
- [ ] Verify UUID uniqueness, exact coverage, no overlap, field ID, and job/model hashes.
- [ ] Choose direct, one-level merged, or hybrid final topology.
- [ ] Do not physically merge independent SQ segments or recursively merge PQ/RQ outputs through the creation merger.
- [ ] For Rust optimization, persist the plan and validate its ID, read version, task attempts, and intended final coverage before commit.

Before commit:

- [ ] Refresh the coordinator to the latest permitted dataset version.
- [ ] Verify fragments and vector values are still valid relative to the source contract.
- [ ] Persist final UUIDs as commit intent.
- [ ] Commit all final segments in one call.

After commit:

- [ ] Record the committed dataset version and verify manifest segment metadata.
- [ ] Run exact-coverage, correctness, recall, and target-topology query checks.
- [ ] Measure cold/warm query latency at the committed segment count.
- [ ] Mark obsolete worker/merge attempts eligible for explicit unverified-file cleanup.
- [ ] Revisit the open tracker before the next Lance upgrade.
