# Lance v1, v2, v3, … — Major Version Lines and What to Anchor To

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

## 1. Two versioning axes — do not confuse them

| Axis | Example | What it protects | Authoritative source |
|---|---|---|---|
| **Library version** (`Lance vN`) | `v8.0.0`, `9.1.0-beta.3` | The public **API** (Rust / Python / Java signatures, defaults, behavior) | `release_process.md`, `docs/src/community/release.md` |
| **File format version** | `2.0`, `2.1`, `2.2`, `2.3` (unstable) | The **data files on disk** | `rust/lance-encoding/src/version.rs` (`LanceFileVersion`), `docs/src/format/file/versioning.md` |
| **Table format feature flags** | `FLAG_STABLE_ROW_IDS`, `FLAG_BASE_PATHS` | The **manifest / dataset layout** | `docs/src/format/table/versioning.md` |

Key consequence: **upgrading the library across majors does not rewrite or
break your datasets.** A new library major reads every format version an older
one wrote (only the legacy `0.1` format lost *write* support, after
library 0.34). What breaks across library majors is the API surface and
occasionally a *default* (see §4). Format versions move independently and much
more slowly than library majors.

Stable file formats are durable backward- and forward-compatibility
contracts. An explicitly unstable format (currently `2.3` / `next`) is
disposable: unreleased intermediate revisions do not receive migrations or
compatibility fallbacks.

```
 Library majors  v1 ─ v2 ─ v3 ─ v4 ─ (v5) ─ v6 ─ v7 ─ v8 ─ v9β   ← weeks apart
                  │         │              │
 File format      2.0 default    2.2 stable    2.1 default        ← years apart
                 (since 0.16)     (in v3)       (in v6)
```

---

## 2. Release model in one paragraph

All changes merge to `main` (version `X.Y.Z-beta.N`). Beta releases can be
published from `main` at any time and carry **no stability guarantees**.
A stable release is cut onto a `release/vX.Y` branch, goes through an RC +
community vote, and is then published to crates.io / PyPI / Maven Central.
Patch releases (`X.Y.Z+1`) are cherry-picked critical fixes only — safe to
take automatically. If a breaking-change-labeled PR lands on `main`, the next
beta jumps to the next major (`1.4.0-beta.1 → 2.0.0-beta.1`). Full details:
`release_process.md`.

Because major bumps are automated, a major version signals *"at least one
breaking API change since the previous line"* — not a marketing milestone.
Some majors are large (v8: 243 commits), some small (v4: 90 commits).

---

## 3. The major lines

Dates are the stable-tag commit dates in this repository.

| Line | First stable | Latest patch | Size¹ | One-line theme |
|---|---|---|---|---|
| **v0.x** | 2022-08-03 (`v0.0.1`) | `v0.39.0` (2025-11-04) | ~3 years | Pre-semver era; breaking changes possible in any release |
| **Lance v1** | 2025-12-12 | `v1.0.4` (2026-01-26) | 162 | First stable major under the semantic-versioning release process |
| **Lance v2** | 2026-02-05 | `v2.0.1` (2026-02-13) | 223 | V2 manifest paths by default; blob handling; index-build API rework |
| **Lance v3** | 2026-03-13 | `v3.0.1` (2026-03-19) | 218 | DataFusion 52.1; file format 2.2 stabilized; index progress callbacks |
| **Lance v4** | 2026-03-30 | `v4.0.1` (2026-04-24) | 90 | Small line: distributed-indexing refactors, FTS build perf, multi-table transactions |
| **Lance v5** | *(never released)* | — | — | `v5.0.0-rc.1` was cut but never approved; its changes shipped in v6 |
| **Lance v6** | 2026-05-11 | `v6.0.1` (2026-05-20) | 198² | **Default storage version 2.0 → 2.1**; Arrow 58 / DataFusion 53; vendored tokenizers |
| **Lance v7** | 2026-05-27 | `v7.0.0` | 141 | Auto-cleanup off by default; multi-base (base-aware) object store; materialized views |
| **Lance v8** | 2026-07-01 | `v8.0.0` | 243 | Segmented index framework; RaBitQ approx mode + SIMD reranking; `IndexSegmentBuilder` removed |
| **Lance v9** | *(in beta)* | `9.1.0-beta.3` | — | Current `main`; DataFusion 54, data overlays, index-core split, subset vector builds, and continuing storage/index work |

¹ non-merge commits since the previous stable major. ² v4 → v6 combined,
since v5 never shipped.

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

The smallest line to date (90 commits, ~2.5 weeks after v3):

- Distributed vector indexing no longer uses a staging step (#6269);
  FTS/inverted index build time and memory reduced (`perf!`, #6174).
- Atomic multi-table transactions via the namespace manifest (#6173);
  `abfss://` support for Azure ADLS Gen2.

### Lance v5 — the gap

`v5.0.0-rc.1` was cut but the stable release was never approved; further
breaking changes on `main` moved the target to v6, and v6.0.0 became the next
stable. Two practical consequences:

- **There is no `Lance v5` to anchor to.** A missing major number is normal
  under this process — only stable tags (`vN.0.0`) define a line.
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

The largest line so far (243 commits):

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

### Lance v9 (in beta)

At base commit `b1570222c`, current `main` is `9.1.0-beta.3`. The v9 line
began with the `FMIndexIndexDetails` → `FMIndexDetails` rename (#7397) and
remains preview only until a stable v9 release passes a vote. Notable work
across the betas:

- DataFusion 54 and a new `lance-index-core` crate that owns shared index
  traits/types while `lance-index` retains implementations.
- Experimental data overlay files: manifest model and commit path, take/scan
  resolution, Python transaction exposure, and overlay-aware compaction
  thresholds.
- V2 files with unequal column lengths, enabling sparse overlay payloads;
  cached file metadata APIs and continued blob v2 / multi-base work.
- Vector segments trained on explicit fragment subsets, with validation that
  prevents merging independently trained IVF/quantizer models; multi-segment
  hamming clustering and batched streaming IVF partition search.
- Runtime x86_64 SIMD dispatch for pre-Haswell source builds and preservation
  of PQ `num_bits` when Python supplies a pre-trained model.

---

## 4. How to anchor (user guidance)

**Pin the major, float the rest.** Within a `Lance vN` line, minor releases
add backwards-compatible features and patch releases contain only critical
fixes — both are safe to absorb automatically.

| Ecosystem | Pin |
|---|---|
| Rust | `lance = "8"` (crates.io carries stable releases only) |
| Python | `pylance>=8,<9` (betas go to fury.io, not PyPI) |
| Java | `[8.0,9.0)` on Maven Central (note: betas and RCs *are* published to Maven Central — exclude prereleases) |

Rules of thumb:

1. **Never pin to `-beta.N` in production.** Betas have no stability
   guarantees and may even reference unstable file format encodings.
2. **Your data outlives the pin.** Datasets written by an older major remain
   readable after upgrading; you do not need to rewrite data to cross a major
   boundary. The reverse (old library reading new data) is only limited by
   *format* versions and feature flags — e.g. a pre-0.17.0 library cannot read
   datasets created with V2 manifest paths (default since Lance v2), and a
   pre-v6-default `2.1` file needs library ≥ 0.38.1.
3. **Watch default changes, not just API changes, when crossing majors.**
   The three that have mattered so far: V2 manifest paths (v2), storage
   version 2.1 (v6), auto-cleanup off (v7).
4. **Pin the file format explicitly** (`data_storage_version="2.1"`) if you
   need bit-identical behavior across environments during a format rollout —
   the `stable` alias resolves differently per library release
   (`docs/src/format/file/versioning.md`).
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
