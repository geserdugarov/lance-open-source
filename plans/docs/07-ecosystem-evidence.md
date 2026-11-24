# Open-source ecosystem evidence

Research date: **2026-11-13**. This is a source review, not a deployment certification or performance benchmark. “Released” below means a published non-prerelease artifact was identified; it does not imply production maturity. “Main” means inspected development source. Recommendations are explicitly marked as proposals.

Local Lance references were refreshed to `v15.0.0-beta.29` on **2026-11-24**.
External artifacts and issue observations remain pinned to the dates below;
this refresh does not claim a new external release or connector qualification.
The upstream crate-architecture proposal and discussion #8973 were read on
**2026-11-24** and are recorded [below](#upstream-crate-architecture-proposal)
as a proposal, not as implemented structure.

The central finding is that distributed Lance is already more than a future idea: public Ray code contains distributed reads, writes, maintenance, index construction, and vector search. The work should strengthen and reuse these implementations. LanceDB also has an open-source embedded library; the commercial distributed service is a separate product boundary.

## Reproducible baselines

| Component | Observed baseline | Interpretation |
| --- | --- | --- |
| This checkout's implementation | `ded121cd9a99dc83040cf41049dc2bd3963241b9`, 2026-11-24 | Requested `main` baseline; later planning and skill commits are outside the implementation baseline. Package version `15.0.0-beta.29` is development provenance. |
| Public Lance stable observed on 2026-11-13 | [v14.0.2](https://github.com/lance-format/lance/releases/tag/v14.0.2), published 2026-10-29 | Historical GitHub release observation; not a claim that this is still the latest stable release. |
| Latest stable tag in the refreshed checkout | `v14.0.3`, release commit `c7dc014f1`, 2026-11-15 | Verified from local tags/history; patch contents are described in [04](04-lance-versions.md). No binaries were qualified by this refresh. |
| Public Lance main observed on 2026-11-13 | [e13a2ee341b0e14d715c8baac15cded1a8034b7f](https://github.com/lance-format/lance/commit/e13a2ee341b0e14d715c8baac15cded1a8034b7f), 2026-11-13 | Historical research pin, included in the refreshed implementation baseline. |
| Lance-Ray release | [v0.5.0](https://github.com/lance-format/lance-ray/releases/tag/v0.5.0), 2026-07-24; commit `93457c7ff3895e9068b6748c03e63bac271d20fe` | [Release package metadata](https://github.com/lance-format/lance-ray/blob/93457c7ff3895e9068b6748c03e63bac271d20fe/pyproject.toml) requires `pylance>=9.0.0`, `ray[data]>=2.41.0`, and Python `>=3.10,<3.14`; classifier remains Alpha. |
| Lance-Ray main | [f155c32425bd3664c88fb4d1c17a3ef7a6314bbe](https://github.com/lance-format/lance-ray/commit/f155c32425bd3664c88fb4d1c17a3ef7a6314bbe), 2026-11-12 | [Main package metadata](https://github.com/lance-format/lance-ray/blob/f155c32425bd3664c88fb4d1c17a3ef7a6314bbe/pyproject.toml) still says `0.5.0` but requires `pylance>=12.0.0`. Pin commit/package artifacts; a version string alone does not distinguish these trees. |
| pglance | [6dece2f725624f3496816135d08b9b4eec63b219](https://github.com/lance-format/pglance/commit/6dece2f725624f3496816135d08b9b4eec63b219), 2025-12-27 | GitHub latest-release endpoint returned 404. [Manifest](https://github.com/lance-format/pglance/blob/6dece2f725624f3496816135d08b9b4eec63b219/Cargo.toml) says package `0.0.0`, Lance `1.0`, Arrow `56.2`, and pgrx `=0.14.3`; default PG feature is `pg16`. |
| Lance-Spark | [75546d97b0c259e88ff3be4d47e9e93970801420](https://github.com/lance-format/lance-spark/commit/75546d97b0c259e88ff3be4d47e9e93970801420), 2026-11-11 | Development reference for an independent distributed engine adapter; no release compatibility is inferred. |
| Lance Namespace | [a13c746bdf369b62d4fc583357564f0cddfad667](https://github.com/lance-format/lance-namespace/commit/a13c746bdf369b62d4fc583357564f0cddfad667), 2026-10-23 | Specification/SDK reference. Individual server implementations can support different subsets. |
| Upstream crate-architecture proposal | [Proposal: Refactoring Lance's crate architecture](https://docs.google.com/document/d/1sqkh0bmDxZRbjGkAsb_ephag9JErtAlpcru7RTGOv_E/edit), dated 2026-11-16, by the maintainer who opened #8973; read 2026-11-24 | Its measurement revision `d8293547c` is an ancestor of `v15.0.0-beta.29`. The proposed crate split is still pending. |
| Lance scope discussion | [#8973, "What should be in scope for Lance?"](https://github.com/lance-format/lance/discussions/8973), opened 2026-09-03 in the Ideas category; read 2026-11-24 | Requirements source for the proposal. The opening post states that nothing is being removed and no decision has been made. |

## What can be reused

| Capability | Evidence and scope | Architecture implication — proposal |
| --- | --- | --- |
| Ray data movement | Main [`datasource.py`](https://github.com/lance-format/lance-ray/blob/f155c32425bd3664c88fb4d1c17a3ef7a6314bbe/lance_ray/datasource.py), [`datasink.py`](https://github.com/lance-format/lance-ray/blob/f155c32425bd3664c88fb4d1c17a3ef7a6314bbe/lance_ray/datasink.py), and [`io.py`](https://github.com/lance-format/lance-ray/blob/f155c32425bd3664c88fb4d1c17a3ef7a6314bbe/lance_ray/io.py) implement partitioned reads, fragment writes, and driver publication; namespace-aware access is present. | Extend `lance-ray`; keep table correctness in Lance and cluster scheduling in the adapter. Do not create a competing connector. |
| Distributed index construction | [v0.5.0 index source](https://github.com/lance-format/lance-ray/blob/93457c7ff3895e9068b6748c03e63bac271d20fe/lance_ray/index.py) contains scalar segment builds and IVF FLAT/PQ/SQ/RQ plus supported HNSW variants. Workers build uncommitted segments; the driver commits them. Shared centroids/quantizer artifacts are present. | Reuse segment lifecycle APIs. Improve model reuse, training scale, retries, and validation before broadening formats. |
| Distributed vector search | [v0.5.0 search source](https://github.com/lance-format/lance-ray/blob/93457c7ff3895e9068b6748c03e63bac271d20fe/lance_ray/search.py) contains segment ownership, uncovered-fragment fallback, worker snapshot serialization, and driver top-k merge, but its fallback L2/Dot formulas disagree with Lance. Both inspected trees also lack stale-row fallback inside covered fragments. | Use `f155c32` or a qualified descendant for quality baselines, and reproduce/fix overlay-row omission before claiming snapshot-equivalent search. |
| Search memory and cohesion | Both [v0.5.0](https://github.com/lance-format/lance-ray/blob/93457c7ff3895e9068b6748c03e63bac271d20fe/lance_ray/search.py) and [pinned main](https://github.com/lance-format/lance-ray/blob/f155c32425bd3664c88fb4d1c17a3ef7a6314bbe/lance_ray/search.py) materialize flat fallback with `scanner.to_table()` and compute distances in Python/NumPy. Main corrects the release's L2/Dot formulas; the trees are not semantically equivalent. | Introduce bounded batch processing and core-owned metric/coverage rules. The release's metric mismatch is concrete evidence of the cost of duplicating those rules in an adapter. |
| Compaction | [`compaction.py`](https://github.com/lance-format/lance-ray/blob/f155c32425bd3664c88fb4d1c17a3ef7a6314bbe/lance_ray/compaction.py) exposes a plan/task/result/commit workflow. | Reuse the existing split between computation and publication as a design example. Distributed data compaction does not prove distributed index optimization. |
| SQL engine connector | pglance's [README](https://github.com/lance-format/pglance/blob/6dece2f725624f3496816135d08b9b4eec63b219/README.md) provides foreign-table import and namespace attach/sync. Its [handler](https://github.com/lance-format/pglance/blob/6dece2f725624f3496816135d08b9b4eec63b219/src/fdw/handler.rs) registers scan/explain callbacks. | Reuse and modernize this early FDW baseline, with an explicit engine/version validation gate. PostgreSQL-compatible SQL does not imply extension ABI compatibility. |
| Second distributed adapter | Lance-Spark's [index documentation](https://github.com/lance-format/lance-spark/blob/75546d97b0c259e88ff3be4d47e9e93970801420/docs/src/operations/ddl/create-index.md) describes parallel scalar builds and contiguous fragment assignment. Its [vector-search documentation](https://github.com/lance-format/lance-spark/blob/75546d97b0c259e88ff3be4d47e9e93970801420/docs/src/operations/dql/vector-search.md) describes optional distributed search and global candidate merge. | Validate shared contracts with both Ray and Spark. Spark is a useful second consumer, rather than a required deployment dependency. |
| Catalog and access boundary | The [Namespace specification](https://github.com/lance-format/lance-namespace/blob/a13c746bdf369b62d4fc583357564f0cddfad667/README.md) separates the client abstraction from directory, REST, and other catalog implementations. The [REST specification](https://github.com/lance-format/lance-namespace/blob/a13c746bdf369b62d4fc583357564f0cddfad667/docs/src/catalog/rest/index.md) includes data operations and table-version APIs for external manifest coordination. | Extend existing namespace contracts where necessary. A generated server interface or REST protocol is not a complete HA coordinator. |

## Lance-Ray search correctness boundaries

Source rechecked **2026-11-16** at the two pins above; the scenarios below were
not executed against a built connector.

- **Released fallback metric mismatch:** v0.5.0 computes L2 as Euclidean norm
  and Dot as `-dot`; Lance uses squared L2 and `1 - dot`. Merging these scores
  with indexed candidates can rank the farther row first. For example, an
  indexed L2 candidate at squared distance 4 loses to a fallback candidate at
  Euclidean distance 3, although the latter's squared distance is 9. Pinned
  main `f155c32` has both formula fixes. Use that exact artifact or a qualified
  descendant for quality numbers, not the released v0.5.0 artifact. Sources:
  [release distance helper](https://github.com/lance-format/lance-ray/blob/93457c7ff3895e9068b6748c03e63bac271d20fe/lance_ray/search.py#L394-L426),
  [main helper](https://github.com/lance-format/lance-ray/blob/f155c32425bd3664c88fb4d1c17a3ef7a6314bbe/lance_ray/search.py#L421-L483),
  [Lance metric guide](../../docs/src/guide/vector_search.md) and
  [Dot kernel](../../rust/lance-linalg/src/distance/dot.rs).
- **Overlay-row omission, inferred from source:** both trees assign fallback
  only to uncovered fragments and force `fast_search=True` for segment tasks.
  Core masks stale overlay rows from ANN; the skipped `knn_combined` supplies
  their row-level fallback. Thus an updated row in a covered fragment can be
  silently absent even with `include_unindexed=True`. Sources:
  [release planner/task](https://github.com/lance-format/lance-ray/blob/93457c7ff3895e9068b6748c03e63bac271d20fe/lance_ray/search.py#L118-L294),
  [main planner/task](https://github.com/lance-format/lance-ray/blob/f155c32425bd3664c88fb4d1c17a3ef7a6314bbe/lance_ray/search.py#L120-L296),
  and [core vector planner](../../rust/lance/src/dataset/scanner/vector.rs).

The formula fixes do not establish equivalence with single-node Lance at the
same snapshot. Core normally re-scores ANN plus fallback candidates exactly;
Lance-Ray sorts the returned approximate/exact scores together. The proposed
contract must preserve core's scoring mode, including its explicit code-only
RQ exception, and account for stale rows. See [09](09-sql-and-ray-integration.md#distributed-search-correctness).

## PostgreSQL integration: useful starting point, explicit gaps

The inspected pglance [`scan.rs`](https://github.com/lance-format/pglance/blob/6dece2f725624f3496816135d08b9b4eec63b219/src/fdw/scan.rs) creates an ordinary Lance scan with a batch-size setting, leaves quals with PostgreSQL, and estimates 1,000 rows. It does not establish projection/filter/top-k pushdown, planner costing, or a transaction-wide Lance snapshot contract. The handler supplies neither foreign-write nor parallel-scan callbacks. These are bounded source observations, not a claim that PostgreSQL cannot support them.

PostgreSQL exposes [FDW callbacks](https://www.postgresql.org/docs/18/fdw-callbacks.html) and [planner integration points](https://www.postgresql.org/docs/18/fdw-planning.html) for richer adapters. **Proposal:** start with read-only, snapshot-pinned SQL access and correct type/NULL semantics; add verified pushdown and distributed execution behind that contract. Treat writes spanning PostgreSQL and Lance as a separate transaction design. An FDW by itself supplies no atomic commit across the two systems. Qualify openGauss or another PostgreSQL-family engine separately; the pglance PostgreSQL feature list is not proof that those engines can load it.

Spark's distributed search documentation explicitly distinguishes its probe-count behavior from namespace execution, requires `prefilter=true` when a filter is supplied, and excludes distance bounds in distributed mode. Those constraints demonstrate why a common search contract must be tested across adapters instead of inferred from matching parameter names. See the pinned [Spark search source documentation](https://github.com/lance-format/lance-spark/blob/75546d97b0c259e88ff3be4d47e9e93970801420/docs/src/operations/dql/vector-search.md).

## NPU: scheduling exists; Lance acceleration needs qualification

Ray documents Huawei Ascend as an **experimental, community-supported** accelerator. It uses the `NPU` resource and `ASCEND_RT_VISIBLE_DEVICES`; the [Ray accelerator guide](https://docs.ray.io/en/latest/ray-core/scheduling/accelerators.html) and [Ray 2.60.0 NPU manager](https://github.com/ray-project/ray/blob/ray-2.60.0/python/ray/_private/accelerators/npu.py) provide primary evidence. This assigns work to devices; it does not convert Lance's CPU/CUDA kernels into NPU kernels.

The checkout's [IVF builder](../../python/python/lance/indices/builder.py) documents CUDA/MPS, while [accelerator training](../../python/python/lance/vector.py) validates string accelerators as CUDA/MPS and contains CUDA-specific settings. [Distance helpers](../../python/python/lance/torch/distance.py) contain CUDA memory assumptions. No validated native Lance NPU backend was established by this review; passing a device object is not evidence of supported end-to-end behavior.

Ascend provides the open-source [TorchNPU adapter](https://github.com/Ascend/pytorch/blob/2abab4d362af564265203cb3170c0b0f1ad47770/README.md) under [BSD-3-Clause](https://github.com/Ascend/pytorch/blob/2abab4d362af564265203cb3170c0b0f1ad47770/LICENSE). Its [compatibility matrix](https://github.com/Ascend/pytorch/blob/2abab4d362af564265203cb3170c0b0f1ad47770/COMPATIBILITY.en.md) treats PyTorch, TorchNPU, the Ascend compute runtime, Python, hardware, firmware, and drivers as a matched environment. **Proposal:** use NPU embedding generation as an initial external pipeline; then qualify sampled IVF training, assignment, and batched scoring through optional compute interfaces. Benchmark transfer and serialization costs as well as kernels. Keep canonical index serialization on the ordinary Lance path. Native AscendCL/CANN kernels should follow profiling evidence, rather than become a mandatory core dependency.

## LanceDB: public library and service boundary

LanceDB's [public repository](https://github.com/lancedb/lancedb/tree/db098ba812ff9cf47e7c8ba5c1a2d58485b6d510) is an embedded retrieval library with an [Apache-2.0 license](https://github.com/lancedb/lancedb/blob/db098ba812ff9cf47e7c8ba5c1a2d58485b6d510/LICENSE). Its commercial Enterprise product supplies a distributed service, described separately in the [product comparison](https://docs.lancedb.com/enterprise/v1-12-0/basics/embedded-vs-enterprise). Therefore “LanceDB is proprietary” is too broad for this plan.

LanceDB publicly describes segment-based construction, coordinated query fan-out, and cached executors in its [distributed-search article](https://www.lancedb.com/blog/how-lancedb-accelerates-vector-search-at-10-billion-scale). That is evidence for a public architectural description, not access to service internals or independent reproduction of its performance numbers. Its [governance announcement](https://www.lancedb.com/blog/lance-community-governance) describes Lance's role as format, SDK, and engine connectors, distinct from LanceDB's lakehouse product.

Verify licensing per reused component; neither LanceDB's Apache-2.0 license nor
TorchNPU's BSD-3-Clause license establishes the terms for accelerator runtimes
or proprietary services. These public sources establish component boundaries,
not organizational commitments.

## Upstream crate-architecture proposal

Read **2026-11-24**. The index API crate, extracted WAL and engine-neutral
task layer are **proposed** at `v15.0.0-beta.29`. The
document describes how the code is organized, not what belongs in Lance's
scope, and says it should fit any outcome of #8973. It leaves the public API of
the `lance` crate and the `pylance` package unchanged: code moves into new
crates and `lance` re-exports it.

The proposal cites build and test cost. At its revision, `lance` held about
615k of the repository's roughly 1.3M Rust lines. Builds waited on that crate,
and testing a behavior exposed only through `Dataset` required opening a
dataset. The proposal reports 2.8-fold growth in Rust code over six months.

Target layering, transcribed from the proposal's figure. Each layer depends
only on layers below it; index implementations depending upward on the index
API is the one inverted dependency:

~~~text
Composition root: Session, pylance, Java bindings
    wires index plugins, kernels and executors into the interfaces below
--------------------------------------------------------------------------
Operations layer            Task layer                  WAL
scan planning, update,      serializable scan, search   own crate, extracted
delete, merge insert,       and write tasks for any     from dataset/mem_wal
SQL; DataFusion             engine
--------------------------------------------------------------------------
Core table                              Index API
fragments, take, deletions, row IDs,    train, update, remap; as_filter,
transactions and commit; no DataFusion  as_ranked; Lance-owned predicates;
                                        no DataFusion
                                            ^ implemented by
--------------------------------------------------------------------------
Vector index                FTS index                   Scalar indexes
IVF, PQ, HNSW;              tokenizer, posting lists,   BTree, bitmap,
uses a KernelSet            BM25                        label list
--------------------------------------------------------------------------
Stable leaves: lance-core, lance-io, lance-file, lance-encoding,
lance-linalg kernels; used directly, no interface
~~~

| Move | Proposed change | State at `v15.0.0-beta.29` |
| --- | --- | --- |
| 1. Common index API crate | Grow `lance-index-core` into the contract between table and indexes: train from a stream of batches, update for new fragments, remap after compaction, report statistics. Query capabilities follow access pattern, not index type: `as_filter()` returns a row set for a predicate, `as_ranked()` a scored top-k stream (ANN distance, BM25). Each takes the prefilter (deletions plus filter results) as a parameter and reports `Option<Cost>`. Index-specific tuning (`nprobes`, `ef`, refine factor) travels as an opaque options bag. Predicates use Lance-owned types, with DataFusion translation in an adapter | `lance-index-core` depends on DataFusion common, expression and execution crates. MemWAL uses `as_filter` and `as_ranked` as result accessors. [Dependencies](../../rust/lance-index-core/Cargo.toml) · [MemWAL query results](../../rust/lance/src/dataset/mem_wal/index/query.rs) |
| 2. Plugin indexes | Vector, FTS and each scalar family become separate crates implementing the API. A registry keyed by index type maps manifest entries to implementations, and only the composition root names concrete types. The vector index takes a `KernelSet` of function pointers chosen once at load time, so downstream projects can supply faster kernels without forking | `lance-index` still owns the vector and FTS implementations and the scalar registry. The vector extension trait lives in a crate-private module. [Registry](../../rust/lance-index/src/registry.rs) · [Vector extension](../../rust/lance/src/session/index_extension.rs) |
| 3. Split `Dataset` | **Core table**, without DataFusion: open a version, list fragments, read and write fragments, take, deletion vectors, row IDs, transactions, commit. **Operations layer**, the only DataFusion user: scan planning, update, delete, merge insert, SQL. Operations become free functions or task objects that take a table; a `DatasetExt` trait may keep the method syntax | `lance-table` owns manifest and transaction actions and has no direct DataFusion dependency. The serializable `CompactionTask` lives in the DataFusion-dependent `lance` crate. [Table dependencies](../../rust/lance-table/Cargo.toml) · [Task implementation](../../rust/lance/src/dataset/optimize/execution.rs) |
| 4. Extract the WAL | `dataset/mem_wal` becomes its own crate over the core table, the index API and, if needed, the operations layer. The proposal says this move needs the most investigation | About 88k lines inside `lance`, with its own memtable, HNSW, index and scanner code |
| 5. Engine-neutral task layer | Scan, search and write operations are described as serializable tasks: fragment or index-segment scans, partial index searches, write and commit steps. They run on one node or under Spark, Ray or another scheduler. DataFusion stays the default executor, but its types do not appear in task definitions. For merge insert, an engine may run the join and index lookups while Lance performs the write | Serializable plans exist for compaction and index optimization only. [`FullSchemaMergeInsertExec`](../../rust/lance/src/dataset/write/merge_insert/exec/write.rs) is a DataFusion execution node |

Five rules place the boundaries: operations take a table as an argument
instead of being `Dataset` methods; volatile code is reached only through an
interface, while stable leaf crates are used directly; the consuming side
defines the interface; one composition root names concrete implementations;
and interfaces work on batches, streams and whole operations, never on single
rows or vectors. A seam is justified when an implementation will be swapped,
a module will be used on its own, or churn should stop forcing downstream
recompilation.

One boundary applies to every move: Lance owns the logic that keeps reads and
writes correct (index coverage, fallback for unindexed fragments, deletion
visibility, conflict resolution), so engines never reimplement it. Decoupling
changes who schedules and runs that logic, not who owns it. The proposal's
list does not name stale overlay rows inside covered fragments; a maintainer
reply in #8973 does.

The work is grouped into three projects that can proceed independently:

| Project | Moves | First milestone | Depends on |
| --- | --- | --- | --- |
| Index API and plugin indexes | 1, 2 | The maintenance contract (train, update, remap) in the index API crate, and one index family moved into its own crate behind it | Nothing to start; the query-side traits share a predicate vocabulary with the core table project |
| Core table, operations and task layer | 3, 5 | A DataFusion-free core table crate with fragment, take and commit APIs, with `CompactionTask` rebuilt on it | The index API, for index metadata in the manifest |
| WAL extraction | 4 | An inventory of what `dataset/mem_wal` uses from `Dataset` internals | Interfaces from the other two; the inventory can start now |

Each project is to have its own tracking issue, design discussion and owners;
none is linked from the document as read. The engine-neutral predicate
vocabulary is a joint decision across the first two projects. Ground rules:
a series of small PRs, public APIs kept working through re-exports from
`lance`, and build, clippy and test timings recorded before and after each
step.

**Runtime plugins are related but out of scope.** Index types, kernels, and
possibly encodings or object stores that are built separately and loaded at
runtime are named as a later project that this refactor must precede. The
seams are to be shaped so they could later be exposed through a C ABI without
committing to one: data crosses as Arrow arrays and streams, options and query
parameters are serializable, signatures carry no generics, borrowed lifetimes
or DataFusion types, and calls are coarse. The Rust traits stay Rust traits; a
plugin would sit behind an adapter crate implementing the trait over the C
interface. The proposal maps this onto existing manifest fields: an index's
type URL identifies the plugin and `index_version` gives the minimum compatible
version, so a reader without the plugin can fail clearly or fall back to an
unindexed scan.

**Deferred until after the reorganization:** whether DataFusion becomes
optional in, or is removed from, the operations layer (#8973 leans toward
default-but-optional); and changes to the public API of `lance` and `pylance`,
including which re-exports stay and whether connectors such as Lance-Spark
should depend on the new crates directly.

**Open questions in the proposal:** what MemWAL needs from `Dataset`; which
predicate vocabulary the index API uses and how much of `ScalarIndexExpr`
survives without DataFusion types; whether scalar indices are plugins or sit
closer to filter evaluation in the core table; and whether a public
row-to-fragment resolver, correct under both address-based and stable row IDs,
belongs in the core table API.

The proposal's targets are contributor build metrics, not runtime performance:
local incremental clippy under 30 seconds, against an unmeasured estimate of
3–4 minutes today, and the CI Rust test job from about 23 minutes median to
under 5, with a stretch goal of 2. It expects many dataset-plus-index
integration tests to become unit tests and per-interface contract tests,
including one suite every index plugin must pass (train, remap, search with
prefilter), run against each implementation and a fake.

**Local check, 2026-11-24.** Counts include every tracked `.rs` line under
each directory, including tests. The two dataset subdirectories are part of
`rust/lance/`.

| Directory | Proposal revision `d8293547c` | Current `main` at `v15.0.0-beta.29` |
| --- | ---: | ---: |
| `rust/lance/` | 614,806 | 633,963 |
| `rust/lance/src/dataset/` | 357,995 | 369,560 |
| `rust/lance/src/dataset/mem_wal/` | 87,885 | 88,226 |
| `rust/lance-index/` | 222,054 | 231,988 |
| `rust/lance-table/` | 60,997 | 80,793 |
| `rust/lance-namespace-impls/` | 42,431 | 42,857 |
| `rust/lance-index-core/` | 2,490 | 2,569 |

The `lance-table` crate now contains the Transaction V2 action implementations
and their tests. `lance-namespace-impls` still depends on `lance`, `lance-index`
and `lance-table`, and `lance-index` uses DataFusion's physical-plan crate.

- The quoted sizes of `scanner.rs` and `merge_insert` (about 16k and 15k lines)
  repeat #8973's September description. Both are already split into
  submodules here: `scanner.rs` is about 1.4k lines beside 12.0k in `scanner/`.
- The row-to-fragment request dates from 2026-09-16. This baseline exposes a
  `Dataset`-level resolver, added by
  [#10720](https://github.com/lance-format/lance/pull/10720) on 2026-11-09:
  Rust [`row_ids_to_row_addrs`](../../rust/lance/src/dataset/rowids.rs), Python
  `LanceDataset.get_row_addresses` and Java `Dataset.getRowAddresses`. The
  fragment ID is the upper 32 bits of the returned address. It is a
  development API, absent from `v14.0.3`. What remains open is its place in a
  core table API.

Repository-wide growth figures and CI timings were not re-measured.

**#8973 as read.** The thread asks what belongs in Lance and what in a query
engine. Positions stated in replies, none recorded as a decision:

- Lance should keep supplying the correctness-sensitive read and write logic
  instead of relying on each engine author to reproduce it.
- One maintainer prefers a Lance-owned native runtime inside an engine-neutral
  planning/task/commit boundary, with host engines owning global relational
  planning and distributed scheduling.
- DataFusion could remain the default executor while becoming optional where
  possible.
- Spark-side contributors ask for a primitive execution unit with explicit,
  limited context, citing distributed cleanup over large manifest histories
  and repeated per-executor dataset opens.
- A Ray user reports using only fragment-level primitives with driver-side
  commits, and asks for the row-to-fragment resolver.

The thread points to [#10429](https://github.com/lance-format/lance/issues/10429)
for drafted write primitives and sketches a shard-level work unit that need
not equal one fragment. Neither was inspected for this review.

How this changes the plan is in
[08](08-extensible-target-architecture.md#alignment-with-the-upstream-crate-refactor).

## Current collaboration watchlist

Statuses were read from the GitHub API on **2026-11-13**, with #7321 and #10797 refreshed on **2026-11-14**. The crate-architecture proposal and #8973 were read from their public pages on **2026-11-24**. An open issue describes a report or proposal; it is not proof that the current checkout still has the reported defect. Check linked fixes and reproduce before implementing.

| Item | Observed status | Planning consequence |
| --- | --- | --- |
| [Lance PR #7321: distributed centroid training](https://github.com/lance-format/lance/pull/7321) | Closed 2026-11-09; `merged_at=null` | Review its Arrow partial-statistics design and discussion; do not count it as merged functionality or an active implementation. |
| [Lance PR #10797: SQ merge model validation](https://github.com/lance-format/lance/pull/10797) | Merged 2026-11-13; commit `793a9e5156556ef315062f051779ee47de32d5ad`, included in `v15.0.0-beta.29` | Rejects mismatched SQ bounds/bit widths in creation merges. Shared-bounds worker input remains separate work; no new validation fix is needed. |
| [Lance #7319: centroid/codebook readers](https://github.com/lance-format/lance/issues/7319) | Open | Coordinate model-export/reuse work with this proposal; check current APIs first. |
| [Lance #7032: segment UUID ownership](https://github.com/lance-format/lance/issues/7032) | Open | Separate logical job identity from fresh physical segment IDs; do not use a shared worker UUID as retry deduplication. |
| [Lance #6860: ANN cost with many deltas](https://github.com/lance-format/lance/issues/6860) | Open | Include segment-count and routing costs in the benchmark matrix; reported timings are workload-specific. |
| [Lance #7731: distributed vector optimization](https://github.com/lance-format/lance/issues/7731) | Open | Verify merge composition and recall before designing tree merges; this is index maintenance, not simply query fan-out. |
| [Lance-Ray #5292: reusable online/batch search](https://github.com/lance-format/lance-ray/issues/5292) | Open | Align actor/session lifecycle and bounded concurrency work; proposed APIs in this issue are not shipped APIs. |
| [Lance-Ray #5317: interleaved segment coverage](https://github.com/lance-format/lance-ray/issues/5317) | Open | Consider compaction eligibility when balancing fragment work; compare Spark's contiguous strategy. |
| [Crate-architecture proposal](https://docs.google.com/document/d/1sqkh0bmDxZRbjGkAsb_ephag9JErtAlpcru7RTGOv_E/edit) | Proposal dated 2026-11-16; per-project tracking issues announced, none linked as read on 2026-11-24 | Take index-extension, kernel and scan/search-task requirements to the matching project instead of proposing a separate seam. Find the tracking issues before filing. |
| [Lance #8973: scope of Lance](https://github.com/lance-format/lance/discussions/8973) | Discussion; no decision recorded as read on 2026-11-24 | Source of the task-layer requirements. Contribute the stale-row fallback and capability-admission cases there or on the project issue. |
| [Lance #10429: write primitives](https://github.com/lance-format/lance/issues/10429) | Referenced from #8973 as a draft; state not inspected | Read before designing fragment-level task ownership or a shard-level work unit. |

The first implementation milestone should lock a tested combination of stable Lance, a specific Lance-Ray artifact, SQL engine, Arrow, and accelerator environment. Keep main-branch trials separate from the stable cross-version matrix. Refresh this watchlist and search for overlapping PRs before filing implementation work.
