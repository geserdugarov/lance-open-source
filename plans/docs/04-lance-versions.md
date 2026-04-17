# Lance v1 through v12 — Major Version Lines and What to Anchor To

**Source baseline:** Lance `12.0.0`, commit `cbeec97cb` (2026-09-17).
Release information below is bounded to that snapshot.

The Lance community deliberately releases **frequently**: any PR labeled
`breaking-change` automatically bumps the major version of the next release
(see `release_process.md`, *Breaking Change Detection*). As a result, stable
majors ship every few weeks — this is normal and expected. For compatibility,
users should **anchor to a `Lance vN` major line**, not to individual
releases: within one major line every release is backwards compatible; across
majors the public API may change.

This document describes each major line, what changed at each boundary, and
how to pin correctly.

---

## 1. Three versioning axes — do not confuse them

| Axis | Example | What it protects | Authoritative source |
|---|---|---|---|
| **Library version** (`Lance vN`) | `v11.0.0`, `v12.0.0` | The public **API** (Rust / Python / Java signatures, defaults, behavior) | `release_process.md`, `docs/src/community/release.md` |
| **File format version** | `2.0`, `2.1`, `2.2`, `2.3` (unstable) | The **data files on disk** | selectors and exact identities: `rust/lance-file/src/version.rs`; `docs/src/format/file/versioning.md` |
| **Table format feature flags** | `FLAG_STABLE_ROW_IDS`, `FLAG_BASE_PATHS` | The **manifest / dataset layout** | `docs/src/format/table/versioning.md` |

Key consequence: **upgrading the library across majors does not rewrite your
datasets.** New libraries retain readers for stable formats written by older
releases (only legacy `0.1` lost *write* support, after library 0.34). What can
break across library majors is the API surface and occasionally a *default*
(see §4). Format versions move independently and much more slowly than library
majors. Files written with an explicitly unstable format are the exception:
intermediate revisions are not compatibility contracts.

Stable file formats are durable backward- and forward-compatibility
contracts. In Lance v12, the public `stable` selector resolves to exact `2.2`,
and `next` resolves to unstable `2.3`. Existing datasets keep their persisted
write format; changing the library default affects newly created datasets.
Manifests, `DataFile` metadata, and file footers persist exact identities through
location-specific codecs; selector aliases such as `stable`, `next`, and
`legacy` are resolved before persistence.

The format guide documents a concrete exception for `FixedSizeList` columns
whose inner values are all null: the corrected full-zip encoding in v12 is
unreadable by readers predating the fix. The guide labels the fix `11.1.0`,
but this repository has no stable `v11.1.0` tag; commit `07eb1b944` (#9130)
lands after `v11.0.0` and is included in `v12.0.0`. Use v12 readers for those
newly written files. The fixed reader can also recover the old zero-width
encoding. See `docs/src/format/file/versioning.md` for the affected pattern.

---

## 2. Release model in one paragraph

All changes merge to `main` first, whose development version normally has an
`X.Y.Z-beta.N` suffix. Beta releases can be published from `main` at any time
and carry **no stability guarantees**.
A stable release is cut onto a `release/vX.Y` branch, goes through an RC +
community vote, and is then published to crates.io / PyPI / Maven Central.
Patch releases (`X.Y.Z+1`) are cherry-picked critical fixes only — safe to
take automatically. If a breaking-change-labeled PR lands on `main`, the next
beta jumps to the next major (`1.4.0-beta.1 → 2.0.0-beta.1`). Full details:
`release_process.md`.

Because major bumps are automated, a major version signals *"at least one
breaking API change since the previous line"* — not a marketing milestone.

---

## 3. The major lines

Dates are release-commit dates in this repository.

| Line | First stable | Latest stable in snapshot | One-line theme |
|---|---|---|---|
| **v0.x** | 2022-08-03 (`v0.0.1`) | `v0.39.0` (2025-11-04) | Pre-semver era; breaking changes possible in any release |
| **Lance v1** | 2025-12-12 | `v1.0.4` (2026-01-26) | First stable major under the semantic-versioning release process |
| **Lance v2** | 2026-02-05 | `v2.0.1` (2026-02-13) | V2 manifest paths by default; blob handling; index-build API rework |
| **Lance v3** | 2026-03-13 | `v3.0.2` (2026-08-06) | DataFusion 52.1; file format 2.2 stabilized; index progress callbacks |
| **Lance v4** | 2026-03-30 | `v4.0.2` (2026-08-06) | Distributed-indexing refactors, FTS build performance, multi-table transactions |
| **Lance v5** | *(never released)* | — | `v5.0.0-rc.1` was cut but never approved; its changes shipped in v6 |
| **Lance v6** | 2026-05-11 | `v6.1.0` (2026-08-06) | **Default storage version 2.0 → 2.1**; Arrow 58 / DataFusion 53; vendored tokenizers |
| **Lance v7** | 2026-05-27 | `v7.1.0` (2026-08-06) | Auto-cleanup off by default; multi-base object store; materialized views |
| **Lance v8** | 2026-07-01 | `v8.0.1` (2026-08-06) | Segmented index framework; RaBitQ approx mode + SIMD reranking; `IndexSegmentBuilder` removed |
| **Lance v9** | 2026-07-24 | `v9.0.1` (2026-08-06) | DataFusion 54, data overlays, index-core split, subset vector builds, FTS v2 default |
| **Lance v10** | 2026-08-08 | `v10.0.0` | Nullable blob selection, overlay-safe index queries, segment-native vector append, exact file-format identity |
| **Lance v11** | 2026-08-30 | `v11.0.0` | Exact-format reader composition, monotonic fragment IDs across overwrite, bounded compaction tasks, external row-address masks |
| **Lance v12** | 2026-09-17 | `v12.0.0` | **Default storage 2.2 and RQ5**, shared batch IVF scans, adaptive probing, partition rebalancing, Java segment selection |

### Lance v1 (2025-12-12)

The first major produced by the new release mechanism
(`ci!: move to semantic versioning release mechanism`, #5089). Everything
before it is the v0.x era, where any release could break.

Breaking / notable:

- Scalar-index `SearchResult` now returns a `NullableRowIdSet` instead of a
  `RowIdTreeMap`, so `NOT` composes correctly with index results (see
  `docs/src/guide/migration.md` §1.0.0).
- Java packages moved to the `org.lance` namespace (#5339).
- macOS x86 support deprecated (#5391); TFRecord support removed (#4593).
- Dynamic pruning for vector search (`perf!`, #4773).
- New features: GEO types, HuggingFace native support, `DatasetDelta` APIs.

### Lance v2 (2026-02-05)

- **New datasets default to V2 manifest path naming** (#5656). Datasets
  created by v2+ are unreadable by libraries older than 0.17.0
  (September 2024). Existing datasets are untouched.
- Index builds return `IndexMetadata` and use a defined default index name
  (#5645).
- Vector search checks metric compatibility before using an index instead of
  silently returning wrong distances (#5609).
- Storage-options accessor rework (#5728); blob-handling APIs for fragments
  exposed to Python.

### Lance v3 (2026-03-13)

- **DataFusion upgraded to 52.1** (#6015) — a pinned public dependency, hence
  breaking for Rust users.
- **File format 2.2 marked stable; 2.3 added as `next`** (#6088). The default
  for new datasets remained 2.0.
- Index progress reporting via callbacks (#5910); shuffle buffer removed from
  the index build path (`perf!`, #5912).
- IVF_RQ index version bumped for compatibility checking (#6097).
- Java: `addFiledStatistics` typo fixed to `addFieldStatistics` (#5763) —
  a rename, so API-breaking.

### Lance v4 (2026-03-30)

This short line followed v3 by about 2.5 weeks:

- Distributed vector indexing no longer uses a staging step (#6269);
  FTS/inverted index build time and memory reduced (`perf!`, #6174).
- Atomic multi-table transactions via the namespace manifest (#6173);
  `abfss://` support for Azure ADLS Gen2.

### Lance v5 — the gap

`v5.0.0-rc.1` was cut but the stable release was never approved; further
breaking changes on `main` moved the target to v6, and v6.0.0 became the next
stable. Two practical consequences:

- **There is no `Lance v5` to anchor to.** A missing major number is normal
  under this process—only an approved stable release defines a line.
- The migration guide (`docs/src/guide/migration.md`) keys its sections by the
  *in-development* version at the time of writing, so its **"5.0.0" section
  describes changes that first shipped in stable v6**, and its "7.2.0" section
  describes changes that shipped in stable v8. Match migration sections to
  stable lines by content, not by header.

### Lance v6 (2026-05-11)

- **Default data storage version changed from 2.0 to 2.1** (#6115). This
  changes `column_indices` in the `DataFile` protobuf: non-leaf fields
  (list/struct containers) get `-1` instead of a sequential index. Only
  advanced users constructing `DataFile` messages by hand are affected; opt
  back with `data_storage_version="2.0"`. Details in
  `docs/src/guide/migration.md` §5.0.0 (see the v5 note above).
- Arrow 58 / DataFusion 53 (#6638).
- The tokenizer stack for FTS is vendored into Lance (#6512).
- Namespace APIs cleaned up (#6186); distributed index builds aligned around
  segments (#6313).
- Scheduler initialization runs eagerly in async `read_tasks` (`perf!`,
  #6710).

### Lance v7 (2026-05-27)

- **Auto-cleanup disabled by default** (#6755) — a behavior change: old
  versions are no longer garbage-collected unless you opt in.
- Dataset object store access is base-aware (#6647), supporting multi-base
  datasets / shallow clones (`FLAG_BASE_PATHS`).
- Materialized view API (#6891); MemWAL sharding work begins; serializable
  caches for BTree / Bitmap / LabelList scalar indices.

### Lance v8 (2026-07-01)

- **Segmented index framework**: Bitmap indices migrated to index segments
  (#6869), distributed BTree builds moved onto the framework (#7013), and the
  parallel `IndexSegmentBuilder` API was **removed** from Rust, Python and
  Java (#6997) — see `docs/src/guide/migration.md` §7.2.0 for the replacement
  (`merge_existing_index_segments` + `commit_existing_index_segments`).
- RaBitQ: approx search mode (#7179) and dedicated SIMD kernels for ex-code
  reranking (#7205); Extended RaBitQ (multi-bit) landed during the v8 RC
  cycle.
- Casting a column that has an attached index now fails fast (#7158).
- File writers return write summaries (#7096); index file listing after
  writes eliminated (`perf!`, #7129).
- Python derives index type from index details instead of opening the index
  (#6903).

### Lance v9 (2026-07-24)

The first stable v9 release was followed by the corruption-safety patch
`v9.0.1`. Notable work in the line:

- DataFusion 54 and a new `lance-index-core` crate that owns shared index
  traits/types while `lance-index` retains implementations.
- Data overlay files: manifest model and commit path, take/scan resolution,
  Python transaction exposure, and overlay-aware compaction thresholds.
- V2 files with unequal column lengths, enabling sparse overlay payloads;
  cached file metadata APIs and continued blob v2 / multi-base work.
- Vector segments trained on explicit fragment subsets, with validation that
  prevents merging independently trained IVF/quantizer models; multi-segment
  hamming clustering and batched streaming IVF partition search.
- Runtime x86_64 SIMD dispatch for pre-Haswell source builds and preservation
  of PQ `num_bits` when Python supplies a pre-trained model.
- Newly created FTS indexes default to format v2. The code analyzer and
  `block_size=256` require format v3; maintenance operations preserve an
  existing index's chosen format.

### Lance v10 (2026-08-08)

- **Blob selections preserve nulls.** Blob `take` APIs now return nullable
  selections instead of collapsing nulls into empty values; this is the
  breaking API change that advanced the major line.
- Scalar and vector queries mask rows changed by newer data overlays. Stale
  index entries are excluded and only affected rows are re-evaluated against
  their current values.
- Vector-index append became segment-set-native: it preserves heterogeneous,
  query-compatible physical segments and builds a new segment only for
  uncovered fragments. Explicit retrain remains the model-unifying rebuild.
- Prefiltered HNSW can use ACORN-1 with `ApproxMode::Fast`, and per-query scan
  statistics expose index-cache hits and misses.
- Bloom filter, RTree, and NGram indices joined the segmented-index framework;
  segment commits list their artifact files concurrently.
- File handling now separates public selectors from exact persisted format
  identities, gives v1 a canonical legacy implementation, and routes current
  formats through exact writers. These are implementation clarifications, not
  changes to stable wire contracts.
- Unstable file format 2.3 gained sparse structural pages and automatic sparse
  layout selection. It remains `next`, not the default stable writer format.

### Lance v11 (2026-08-30)

- **Exact-format readers are composed under `lance-file::versions`** (#8024).
  Public `LanceFileVersion` selectors move from `lance-encoding` to
  `lance-file` (#8026), and index-file creation also uses exact identities
  (#8028). Rust callers must follow the new construction/version APIs.
- **Overwrite does not reuse fragment IDs** (#8206). A concurrent write
  conflicts with tightening a field to NOT NULL (#8347), protecting the
  committed schema's constraints.
- Compaction accepts `max_source_rows` and `max_source_bytes` limits (#8235),
  bounding source work per planned task.
- External row-address allow/block masks can participate in scans and search
  (#7288). Per-segment ownership filtering and merge filtering prevent stale
  vector entries from surviving in-place column updates (#7371, #8342).
- `IndexMetadata.covering_fields` records carried payload fields separately
  from search keys (#8535). Transaction implementations move into `lance-table`
  (#8054); `lance` keeps its public re-export.

### Lance v12 (2026-09-17)

- **IVF_RQ defaults to five bits per dimension** across Rust/Python/Java
  (#8936). Set `num_bits=1` explicitly to build binary RaBitQ. Existing model
  metadata retains its chosen bit width.
- **`stable` resolves to file format 2.2** for new datasets (#8657).
  `CompactionOptions.data_storage_version` can target an exact V2 version
  without changing the dataset's default write format (#8584).
- IVF training allocates hierarchical centroid quotas proportionally (#9050).
  Default optimization splits oversized partitions toward the persisted target
  size, or joins undersized partitions when no split runs (#9051).
- Compatible fixed-probe batch queries share IVF partition scans (#7640).
  Ordinary Float32 IVF_FLAT L2/cosine queries gain metric-aware initial probe
  budgets (#9195); other query shapes retain their existing paths.
- Stale segment rows are excluded before top-k (#8351), HNSW build connectivity
  is repaired (#9053), and IVF prewarming reads parallel byte windows (#9049).
- Java adds segment selection through `ScanOptions.Builder.indexSegments(...)`
  (#7169) and extends `createIndex` progress callbacks (#8823).
- `json_extract` predicates fall back to scan evaluation instead of querying
  JSON-path indices with incompatible serialized-text semantics (#9101).
  Namespace merge-insert keys can contain multiple columns (#8915).
- Nullable fixed-size-list encoding includes the all-null-inner-values fix
  (#9130); observe the reader compatibility caveat in §1.

---

## 4. How to anchor (user guidance)

**Pin the major, float the rest.** Within a `Lance vN` line, minor releases
add backwards-compatible features and patch releases contain only critical
fixes — both are safe to absorb automatically.

| Ecosystem | Pin |
|---|---|
| Rust | `lance = "12"` (crates.io carries stable releases only) |
| Python | `pylance>=12,<13` (betas go to fury.io, not PyPI) |
| Java | `[12.0,13.0)` on Maven Central (betas and RCs are also published there, so exclude prereleases) |

Rules of thumb:

1. **Never pin to `-beta.N` in production.** Betas have no stability
   guarantees and may even reference unstable file format encodings.
2. **Your stable-format data outlives the pin.** Datasets written by an older
   major remain readable after upgrading; you do not need to rewrite data to
   cross a major boundary. The reverse (old library reading new data) is
   limited by *format* versions and feature flags—e.g. a pre-0.17.0 library
   cannot read datasets created with V2 manifest paths (default since Lance
   v2), and a pre-v6-default `2.1` file needs library ≥ 0.38.1. For the
   all-null-inner-values pattern fixed in v12, also observe the specific
   forward-compatibility caveat in §1.
3. **Watch default changes, not just API changes, when crossing majors.**
   Examples include V2 manifest paths (v2), storage version 2.1 (v6),
   auto-cleanup off (v7), nullable blob selections (v10), and both stable
   storage 2.2 and five-bit IVF_RQ (v12).
4. **Pin the file format explicitly** (`data_storage_version="2.2"`, or
   `"2.1"` when older readers require it) if you
   need a fixed on-disk identity and capability set during a rollout—the
   `stable` alias may resolve differently in a future library release. A fixed
   format identity does not promise byte-for-byte identical encoder output
   across library builds (`docs/src/format/file/versioning.md`), and pinning
   `2.1` does not avoid the all-null-inner-values reader caveat.
5. **Before crossing a major, read** `docs/src/guide/migration.md` — and
   remember its section headers can name unreleased versions (see the v5
   note in §3).

---

## 5. Related documents

- `release_process.md` — branching, RC/vote flow, breaking-change detection
  mechanics.
- `docs/src/community/release.md` — release types and semver policy.
- `docs/src/guide/migration.md` — per-boundary migration guides.
- `docs/src/format/file/versioning.md` — file format versions (0.1 → 2.3).
- `docs/src/format/table/versioning.md` — table format feature flags.
