# Lance IVF partitioning and data reorganization on S3

**Source baseline:** Lance `14.0.2`, code commit `40f15ed1b`, inspected at
`d5312ea00` on 2026-10-29. This document describes this checkout's implementation;
format specifications can describe capabilities that its writers do not yet
support. The [architecture overview](00-overview.md) and
[distributed vector indexing guide](05-distributed-vector-index-creation.md)
cover the surrounding system.

Lance can share existing objects through versioned manifests and shallow clones.
**Changing the physical order of rows requires writing new data objects.**
Existing indices can sometimes survive that rewrite through the Fragment Reuse
Index (FRI); rebuilding every affected index is not always necessary.

This repository supplies storage, indexing, compaction, cloning, and low-level
rewrite APIs. It does not include the external `lance-clustering` package,
`ClusteringConfig`, or a Ray pipeline for sorting base data by IVF partition.
The custom clustering workflows below describe how an external implementation
can use Lance's current mechanisms, not an available one-call clustering API.

## Operations available in this checkout

| Operation | What is written | Effect on base-row order |
| --- | --- | --- |
| Build or optimize an IVF index | Search structures under `_indices/<uuid>/`; index metadata | Groups index entries by partition; does not sort the base table. |
| Shallow clone | A destination manifest and, when necessary, relocated FRI details | Shares existing data, index payloads, and row maps; preserves their order. |
| Deep clone | Copies objects referenced by the selected snapshot and commits destination metadata | Relocates the snapshot without clustering its rows. Managed blob descriptors may also need rebinding. |
| Compact files | Replacement data files and index maintenance metadata/files | Preserves source order while combining eligible fragments and materializing deletions. |
| Custom stable partition | Replacement data files, a destination-label map, and one atomic `Rewrite` with FRI | Groups rows into destination fragments while preserving source order within each destination. Requires a Rust integration. |
| Custom arbitrary sort | Replacement data files plus valid index remapping or rebuilt indices | Can change order within a destination; the current stable-partition FRI map cannot describe this permutation. |

The implementation boundaries are in [dataset APIs](../../rust/lance/src/dataset.rs),
[compaction](../../rust/lance/src/dataset/optimize.rs),
[rewrite operations](../../rust/lance-table/src/transaction/operation.rs), and
[FRI commit handling](../../rust/lance/src/index/frag_reuse.rs).

Binary-copy compaction can avoid decoding and re-encoding compatible pages, but
still writes replacement objects. Index partition rebalancing changes centroids
and index storage; it does not rearrange base data. Neither operation is a
metadata-only physical clustering operation.

## Identifiers, partitions, and models

| Identifier | Meaning | Example |
| --- | --- | --- |
| Application ID | An ordinary user column carried with a row | `id = 12345` |
| Logical index name | Groups compatible physical index segments | `vector_idx` |
| Segment UUID | Identifies one physical index payload and its metadata | Segment A covers source fragments `{0, 1}` |
| IVF partition ID | A centroid's position within one segment's model | Partition `2` of segment A |
| Fragment ID | Identifies a horizontal group of rows; a fragment may have several column files | Fragment `4` |
| Physical row address, `_rowaddr` | Fragment ID and physical row offset | `(4, 17)` |
| System row ID, `_rowid` | A stable logical ID when enabled; otherwise the physical address | Its interpretation depends on the table and index row domain. |
| `base_id` | Looks up a `BasePath` by its numeric `id` | ID `0` is valid; it is not an array position or partition ID. |
| FRI destination label | Position in one transition's ordered destination list | Label `0` selects `destinations[0]`, whatever its fragment ID. |

An IVF partition is a search grouping; a fragment is a storage grouping. A
fragment can contain several IVF partitions, and a partition can span several
fragments. Lance does not automatically create Hive-style `partition_id=.../`
directories when it builds a vector index.

For a segment `s` with centroids `C_s`, coarse assignment chooses a centroid
under that model's metric and preprocessing. L2, cosine, dot, and Hamming do not
have interchangeable assignment semantics. Cosine uses normalization; binary
vectors have their own training and distance paths. Reusing centroids avoids
retraining the coarse model, but recomputation is not proof of identical
membership when normalization, precision, tie handling, or assignment algorithms
differ. Null and invalid vectors must follow the index builder's eligibility
rules while their application rows remain present in the base table.

Partition IDs are local to a segment. Two segments of `vector_idx` can have
different models, so partition `2` need not describe the same region in both.
Independent models can be queried as separate segments. A physical segment
merge has stricter model compatibility requirements, including quantizer
parameters; see [model scope and merge compatibility](05-distributed-vector-index-creation.md).

For a custom multi-segment sort, `(segment_uuid, local_partition_id)` can be an
external routing key. A global ordinal or centroid-proximity rank is scheduling
metadata, not a new Lance partition ID. Record the original segment/model
identity if the rebuild must preserve those assignments.

### Inspect logical indices and physical segments

The Python API exposes segment descriptions without opening index files by
hand. This example reads an existing dataset and returns an inventory:

```python
import lance


def index_inventory(uri, storage_options=None):
    dataset = lance.dataset(uri, storage_options=storage_options)
    return dataset.version, [
        {
            "name": index.name,
            "segments": [
                {"uuid": str(segment.uuid), "fragment_ids": segment.fragment_ids}
                for segment in index.segments
            ],
        }
        for index in dataset.describe_indices()
    ]
```

Pin the returned version for subsequent workers, then inspect index statistics
and the relevant models. In this checkout, `get_ivf_model(index_name)` finds the
**first matching segment** in [the Python binding](../../python/src/dataset.rs).
It is not a per-segment model enumeration API. `get_ivf_partition_info` in
[lance.vector](../../python/python/lance/vector.py) accepts `index_segments` for
IVF_FLAT partition sizes; it does not return all centroid models. A general
reclusterer needs segment-aware model access, such as the Rust UUID-based index
open path, and effective coverage after any FRI translation.

## How metadata connects a query to S3

The manifest is the snapshot boundary. It records schema, fragments, storage
bases, feature flags, and the location of index metadata. Each index metadata
record describes a segment, including its UUID, name, fields, provenance
version, and fragment coverage.

```text
snapshot manifest
|-- schema and feature flags
|-- base_paths: id -> storage location
|-- fragments
|   |-- data files and physical row counts
|   |-- optional deletion files and overlays
|   `-- optional row-ID and version lineage
`-- index metadata
    |-- vector/scalar segments and stored fragment coverage
    `-- optional __lance_frag_reuse history

query -> segment model -> selected IVF partitions -> candidate identifiers
      -> row-domain resolution / FRI translation -> current fragment and row
      -> column pages -> object-store range reads
```

Physical addresses use:

```text
row_address = (uint64(fragment_id) << 32) | uint64(row_offset)
fragment_id = row_address >> 32
row_offset  = row_address & 0xffffffff
```

An address is not an S3 byte offset. The fragment's file metadata and page layout
determine which ranges must be read. With stable row IDs enabled, do not decode
`_rowid` this way: resolve the logical ID to its current address. Preserving
application columns alone does not preserve stable IDs or row-version lineage.
Those sequences must be carried through a value-preserving rewrite, including
sequences stored in hidden columns. See [row IDs and lineage](../../docs/src/format/table/row_id_lineage.md).

A representative layout is:

```text
s3://bucket/dataset.lance/
|-- _versions/                         committed manifests
|-- _transactions/                     transaction metadata
|-- data/*.lance                       base and overlay column files
|-- _deletions/                        deletion vectors
|-- _indices/<segment-uuid>/           index-specific payloads
|   |-- index.idx                      model / sub-index structures
|   `-- auxiliary.idx                  vector storage, where applicable
|-- _indices/<fri-uuid>/details.binpb   FRI history when spilled
|-- _fri/<map-id>/stable_partition.lance
|                                     immutable destination-label map
`-- _refs/                            tags and branches
```

This omits optional blob storage and branch contents. Index filenames and
schemas vary by index format; do not build a generic inspector around the
assumption that every index has an `auxiliary.idx`. Current IVF storage groups
entries by partition and stores row identifiers with original or encoded vector
values, such as PQ codes. The partition directory locates entries in **index
storage**, not rows in base files. See [vector index storage and search](03-index-on-disk-and-search.md).

Physical clustering can improve locality when refinement or payload retrieval
fetches base columns. It does not establish a general query-result ordering
guarantee. Probe policy, selected columns, compression, caching, and column-page
boundaries determine the actual I/O benefit.

## Choosing a rewrite strategy

### Ordered compaction

`dataset.optimize.compact_files(...)` is the supported Python maintenance
entry point. It combines eligible neighboring fragments and can remove deleted
rows. It preserves their source order; it does not assign or sort IVF keys.

For an ordinary table without tagged FRI, compaction defaults to eager index
maintenance. Supported indices are remapped; types that require rebuilding can
lose coverage for rewritten fragments until rebuilt. Address-based tables can
use `defer_index_remap=True` to record FRI movement for dependent indices.
An index-free table does not necessarily receive an FRI just because this option
is set. A table carrying tagged FRI requires the deferred path for compaction.

The Rust task protocol is `plan_compaction` -> `CompactionTask::execute` ->
`commit_compaction`; Python exposes `lance.optimize.Compaction`. Tasks use their
recorded read version and consistent remapping options. Bounded source counts
allow gradual maintenance. This task protocol is not an arbitrary sorting API.
See [Rust compaction](../../rust/lance/src/dataset/optimize.rs) and
[Python compaction](../../python/python/lance/optimize.py).

### Stable partition with FRI

FRI index version 1 supports tagged transitions, including ordered compaction
and stable partition. A stable partition routes source rows into destination
fragments while retaining relative source order **inside each destination**.
It can cluster by an IVF partition or another key if the chosen layout respects
that rule. Existing index payloads can remain in place and be translated at read
time.

Consider source-fragment order `0, 1`, with no deleted rows:

| Source address | Application ID | IVF partition | Destination label |
| --- | --- | --- | --- |
| `(0, 0)` | A | 1 | 1 |
| `(0, 1)` | B | 0 | 0 |
| `(1, 0)` | C | 1 | 1 |
| `(1, 1)` | D | 0 | 0 |

With destinations `[4, 5]`, label `0` writes `[B, D]` into fragment 4 and label
`1` writes `[A, C]` into fragment 5. The stored labels are `[1, 0, 1, 0]`:

```text
existing index: partition 0 -> [(0, 1), (1, 1)]
FRI translation             -> [(4, 0), (4, 1)] -> B, D

existing index: partition 1 -> [(0, 0), (1, 0)]
FRI translation             -> [(5, 0), (5, 1)] -> A, C
```

The destination offset is the number of earlier source rows with the same
label. Deleted physical source positions have null labels. A partition can be
split across several destinations, but each destination must retain its own
source subsequence.

Packing these rows into a single fragment as `[B, D, A, C]` cannot be represented
by the same map: a single label implies `[A, B, C, D]`. Sorting by distance to a
centroid within a destination also violates the contract unless it happens to
preserve source order. The destination label is not an IVF partition ID; using
one destination per partition is one possible policy, not a format requirement.

An external Rust implementation supplies the following work:

1. Choose source fragments and destination layout. Reserve fresh destination
   IDs with `Operation::ReserveFragments`, finalize them before assembling the
   mapping, and use a pinned read snapshot. ID `0` means unassigned on this
   commit path and cannot be used as a mapped destination.
2. Scan the sources in their recorded order, apply source deletions, route live
   rows, and write replacement fragments. Distributed writers must restore that
   order within each destination. Preserve logical values, including supported
   overlays and blob references, rather than copying only stale base values.
3. Feed live-row labels in source order to `RowMapWriter`. It inserts nulls from
   source deletion vectors and writes the label column plus cumulative counts.
   Finish the immutable file under `_fri/<map-id>/` before publication.
4. Construct a transition with ordered source and destination digests, the map
   UUID, exact file size, and optional base reference. Use
   `frag_reuse_entry_appending` against the transaction's read snapshot to
   assemble the complete FRI intent, including existing history.
5. Commit `Operation::Rewrite { groups, rewritten_indices, frag_reuse_index }`
   through `CommitBuilder`. For deferred reuse, existing user-index payloads
   remain unchanged. The commit validates and merges the new transition with
   current history and publishes the fragment replacement and FRI atomically.

The publication is atomic; data writes and fragment-ID reservation precede it.
Concurrent compatible commits can be rebased, while overlapping changes can
conflict. The commit path handles supported deletion folding and validates
digests and history; it cannot prove that an arbitrary external writer emitted
the claimed row values and order. The writer must verify that correspondence.

Use [RowMapWriter](../../rust/lance-index/src/frag_reuse/row_map.rs),
[FRI assembly](../../rust/lance/src/index/frag_reuse.rs), and the existing
`stable_partition_rewrite_commits_atomically` test as implementation references.
Python's `LanceOperation.Rewrite` currently exposes groups and rewritten indices,
but **does not expose `frag_reuse_index` or this row-map writer**. Do not present
the Rust protocol as an executable Python clustering recipe.

Stored segment coverage remains provenance for the original addresses. Readers
derive effective coverage from the history and the usable segments of the same
logical index. They must establish coverage of all contributing sources before
claiming a rewritten destination. Manually replacing an old segment's bitmap
with destination IDs can make its stored addresses appear current and break
translation. Unsupported mappings or segment remappers can force scan fallback.
See [the FRI reader](../../rust/lance/src/index/frag_reuse_reader.rs).

### Arbitrary sorting and rebuilding

If the layout requires an arbitrary permutation within destination fragments,
the stable-partition label format is insufficient. An external writer must
provide index maintenance that actually represents the permutation, or rebuild
affected indices against the output. Reusing IVF centroids does not eliminate
the vector reads and index writes needed for a rebuild.

`Rewrite` expresses unchanged logical rows whose physical addresses move.
`Update` and `Overwrite` have different row-identity, coverage, and conflict
semantics. An append-style `write_fragments` followed by `Update` is not a
general substitute for a value-preserving rewrite, especially with stable IDs.
Tables with tagged FRI also enforce a [write gate](../../rust/lance-table/src/system_index/frag_reuse/gate.rs);
callers cannot bypass history maintenance by attaching hand-built FRI metadata
through `CreateIndex`.

When a supported workflow publishes data replacement and index rebuilding in
separate commits, readers may see an intervening snapshot with uncovered data.
Normal vector search merges indexed candidates with scans of uncovered
fragments. `fast_search=True` intentionally omits that fallback. A two-commit
workflow must account for that visibility and for **every** affected logical
index, including scalar indices and other vector columns. It is not one atomic
clustering transaction.

## Mapping size and S3 read costs

Two different maps must not be confused:

| Map | Schema / role | Current resource behavior |
| --- | --- | --- |
| Stable-partition FRI row map | Nullable `label: uint16`, one per physical source row | Persisted independently; cumulative counts locate offsets and label reads are lazy. |
| Precomputed IVF assignment dataset | `row_id: uint64`, `partition: uint32`, for index construction | Loaded into a `HashMap<u64, u32>` by the current builder; not a streaming lookup. |

For FRI, with `N` physical source rows, `M` destinations, and block size `B`, the
dense counts buffer occupies:

```text
28 + 4 * ceil(N / B) * M bytes
```

The current default `B` is 65,536 rows. Labels add roughly two decoded bytes per
row plus validity; persisted size depends on encoding. The reader keeps the
counts matrix in memory and reads requested label blocks, with shared-cache
chunks of 32 blocks (roughly 4 MiB of decoded label values) when the cache can
retain them. This avoids an obligatory full per-row address map in RAM, but
large destination counts still make the dense matrix expensive.

The [format](../../docs/src/format/index/system/frag_reuse.md) and counts utility allow
65,536 destinations per transition. This checkout's
[`StablePartitionMapping::try_new_with_cache`](../../rust/lance-index/src/frag_reuse/stable_partition.rs)
rejects more than `u16::MAX` destinations, so integrations must currently stay
at or below **65,535**. This implementation discrepancy is separate from the
number of IVF partitions in a logical index.

A precomputed IVF map has 12 raw value bytes per entry, but hash-table memory is
larger. Streaming its creation or fitting its file on scratch disk does not
bound the builder's RAM. The ordinary Python path requires supplied
`ivf_centroids` and a single-fragment, single-file assignment dataset. Its
`row_id` values must match the index input's row-ID domain; synthesizing
`fragment_id << 32 | offset` is only correct for address-based input. The
current builder filters rows missing from a nonempty map, so complete mapping
of eligible input rows matters. See [Python validation](../../python/python/lance/dataset.py),
[map loading](../../rust/lance-index/src/vector/ivf/builder.rs), and
[the current index builder](../../rust/lance/src/index/vector/builder.rs).

Generating an assignment map from known output runs may avoid an extra base-data
scan in an external implementation. Rebuilding the index still reads output
vectors. Repeated partition-range scans, scratch spills, staged compaction, and
index construction all add I/O beyond one read and one write of selected data.

## Reusing untouched objects and relocating a dataset

Suppose a source snapshot has segment A on fragments `{0, 1}` and segment B on
`{2, 3}`. A shallow clone initially references the source's objects. A supported
rewrite of `{0, 1}` can publish new local fragments while `{2, 3}` and B's
payloads remain shared. Existing base IDs are retained across clone chains;
source-local references receive an appropriate source base.

This is an illustrative metadata view after rebuilding A into fragment 4, not
serialized manifest syntax. Assume an address-based table, no prior FRI, and
no other indices:

```yaml
dataset_uri: s3://bucket/clustered.lance
base_paths:
  - {id: 7, path: "s3://bucket/source.lance", is_dataset_root: true}
fragments:
  - id: 2
    files: [{path: "b2.lance", base_id: 7}]
  - id: 3
    files: [{path: "b3.lance", base_id: 7}]
  - id: 4
    files: [{path: "a-clustered.lance", base_id: null}]
index_segments:
  - {name: vector_idx, uuid: rebuilt-A, fragment_ids: [4], base_id: null}
  - {name: vector_idx, uuid: original-B, fragment_ids: [2, 3], base_id: 7}
```

```text
fragment 2 -> s3://bucket/source.lance/data/b2.lance
fragment 3 -> s3://bucket/source.lance/data/b3.lance
fragment 4 -> s3://bucket/clustered.lance/data/a-clustered.lance
segment B  -> s3://bucket/source.lance/_indices/original-B/
segment A  -> s3://bucket/clustered.lance/_indices/rebuilt-A/
```

With FRI reuse, A's payload and original coverage can remain at the source too;
the clone instead publishes mapping history for its replacement fragments.
Shallow cloning relocates tagged FRI details as needed and preserves references
to shared `_fri/` files. New transitions can reference local maps alongside
inherited maps. Copying only `data/` and `_indices/` misses those dependencies.

### Clone APIs

These are actual Python APIs for an existing source. Run one alternative with a
new destination URI and credentials appropriate to the participating stores:

```python
import lance

source = lance.dataset("s3://bucket/source.lance")
source_version = source.version

# Share the selected snapshot's objects.
shared = source.shallow_clone("s3://bucket/shared.lance", source_version)

# Or copy the selected snapshot's Lance-owned objects to a new destination.
copied = source.deep_clone("s3://bucket/copied.lance", source_version)
```

Neither operation clusters the data. A shallow clone does not upload local
source files to S3; remote workers still need access to every referenced base.
For local-to-S3 movement or an independent copy, use `deep_clone` with reachable
source storage and destination `storage_options`. It copies the selected
snapshot, not the source's entire version history. Tagged FRI maps are included
and rebound; managed blob references have their own copy/rebinding path.
User-owned external blob URIs remain external dependencies.

Deep cloning has bounded concurrent object copies and normally streams data
through the calling process. Same-store provider-native copy is opt-in via
`LANCE_IO_SERVER_SIDE_COPY_ENABLED`; copying to S3 is not inherently zero-transfer.
The implementation also documents that distinct per-base source credentials are
not yet supported by this clone path. Check that all inherited bases are
readable through the source configuration. See [clone implementation](../../rust/lance/src/dataset.rs)
and [managed blob cloning](../../rust/lance/src/dataset/blob/clone.rs).

### Retention and cleanup

Removing a fragment from a new manifest does not delete its objects from older
snapshots. Retained versions and tags keep their referenced objects alive.
Cleanup also accounts for registered branches and retained FRI row maps.
Trimming obsolete FRI history is separate from deleting its files: old versions
may still reference them.

A standalone shallow clone creates a dependency on another dataset's objects;
do not assume the source discovers every clone at an unrelated URI. Retain the
source snapshot, for example with a source tag, for as long as such references
are needed, including inherited bases from earlier clones. Registered branch
protection does not establish a global reverse-reference registry. See
[cleanup](../../rust/lance/src/dataset/cleanup.rs) and
[tags and branches](../../docs/src/guide/tags_and_branches.md).

## Current support boundaries

| Capability | v14.0.2 implementation status |
| --- | --- |
| Tagged FRI on address-based datasets | Implemented. First publication sets `FLAG_FRAGMENT_REUSE_INDEX` (`1 << 10`) in both flag words; the capability remains sticky after history trimming. |
| Tagged FRI on stable-row-ID datasets | Rejected, including by `frag_reuse_entry_appending`. |
| FRI version 0 with stable row IDs | Debug-build-only capability (`cfg!(debug_assertions)`), with no environment override. Release builds reject the combination and deferred compaction option. |
| Stable row-ID and version lineage in hidden columns | Supported independently of FRI; bit 11 protects spilled lineage. Preserve it when moving logical rows. |
| Manifest clustering definitions and fragment/segment `clustering_id` | Specified in protobuf and format docs, but writer support is reserved. `FLAG_OBJECT_METADATA` (`1 << 17`) is excluded from supported writer flags. |
| Fragment metadata trees | Specified but reserved behind unsupported `FLAG_FRAGMENT_TREE`; not the current manifest representation to target. |
| Python/Ray IVF base-data clustering | No turnkey implementation in this repository. Scheduling, routing, scratch placement, and ordering remain external responsibilities. |

The [clustering metadata specification](../../docs/src/format/table/clustering.md) must not
be mistaken for an implemented clustering engine or an available writer API.
Legacy column-level clustering hints remain their own frozen contract.

The [feature gates](../../rust/lance-table/src/feature_flags.rs) are authoritative
for this build. Library version, data-file version, index format version, and
manifest feature flags are separate compatibility axes. `stable` resolves to
data format `2.2`; `next` selects unstable `2.3`, and existing datasets retain
their configured write format unless explicitly changed. Preserve stable
contracts and existing legacy reads; new clustering work belongs in current
writers, not extensions to legacy formats. See [version routing](../../rust/lance-file/src/version.rs)
and [release/version distinctions](04-lance-versions.md).

## Implementation and validation references

| Area | Source and existing coverage |
| --- | --- |
| Segment models, building, and remapping | [Vector builder](../../rust/lance/src/index/vector/builder.rs), [FRI reader](../../rust/lance/src/index/frag_reuse_reader.rs), [remapping tests](../../rust/lance/src/dataset/optimize/remapping.rs) |
| Stable-partition arithmetic and file I/O | [Counts and rank logic](../../rust/lance-core/src/utils/stable_partition.rs), [row-map writer/reader](../../rust/lance-index/src/frag_reuse/row_map.rs), [lazy translation tests](../../rust/lance-index/src/frag_reuse/stable_partition.rs) |
| Atomic rewrite, clone chains, and FRI retention | [FRI implementation and tests](../../rust/lance/src/index/frag_reuse.rs), [history trimming](../../rust/lance/src/dataset/index/frag_reuse.rs), [cleanup tests](../../rust/lance/src/dataset/cleanup.rs) |
| Concurrent rewrites and index maintenance | [Tagged rewrite conflict tests](../../rust/lance/src/io/commit/conflict_resolver/tests/tagged_rewrite_conflicts/), [concurrent reindex tests](../../rust/lance/src/dataset/optimize/tests/concurrent_reindex.rs) |
| Python APIs | [Dataset and clone tests](../../python/python/tests/test_dataset.py), [compaction tests](../../python/python/tests/test_optimize.py), [vector index tests](../../python/python/tests/test_vector_index.py) |

An external clustering implementation needs multi-fragment tests for row-value
and row-ID preservation, deleted and null rows, destination ordering, segment
coverage, clone base resolution, and conflicting commits. Compare indexed
results with scans and measure vector recall, including cold-cache behavior.
Measure selected-data reads/writes, index build traffic, mapping reads, scratch
use, and retained storage separately before claiming S3 savings.

This account is based on static inspection of the local source and existing
tests. The S3 examples and performance claims require deployment-specific
execution; no S3 clustering benchmark is implied by this document.
