---
name: review
description: Review checklist for reviewer agents on Lance PRs (a Rust workspace with Python/Java bindings), plus Rust-specific LLM defect patterns. Use when evaluating a developer-produced branch before approval or change-requests.
---

# Reviewer skill — Lance

Lance is a Rust workspace with thin Python (PyO3) and Java (JNI) bindings; the Rust core is the source of truth. Contributor and maintainer attention is the scarce resource — be concise and focus on P0/P1 issues (severe bugs, perf regressions, security, format/compatibility breaks). Don't reiterate detailed changes or repeat what's already well done.

## CI / lint gates

Request fixes if a gate for a touched surface is red:

- Rust: `cargo fmt --all -- --check`, `cargo clippy --all --tests --benches -- -D warnings`, `cargo check --workspace --tests --benches`, `cargo test --workspace`. For any PR touching `unsafe`, `cargo miri test` must be referenced in the description.
- Python: `uv run make lint`, `uv run make format`, `uv run make test`, `uv run make doctest` — all via `uv run`. A bare-`python`/`pytest` "failure" is an environment-usage mistake, not a baseline failure; don't accept it as one.
- Java: `./mvnw spotless:check`, `cargo clippy --tests --manifest-path ./lance-jni/Cargo.toml`, `./mvnw test`.
- PR title follows Conventional Commits — commitlint (`.github/workflows/pr-title.yml`) validates the title and body.

## Public API & format compatibility

- No public API signature changes in place — old methods must be `#[deprecated]` / `@deprecated` with a new method added, not mutated or removed.
- On-disk and serialization format changes are versioned (enums for format versions, stable serialization for index/manifest files). A "harmless rename" of a persisted key, proto field, or manifest field is a migration, not a refactor — reject it when framed as a refactor.
- Spot-check that moved code still routes through the same I/O, object-store, and error paths. A refactor is not allowed to silently change side effects.

## Cross-language bindings

- Validation and logic live in the Rust core, not duplicated into the bindings; Python/Java stay thin wrappers.
- Parameter names are identical across Rust, Python, and Java — flag any rename that lands in only one.
- Mutually exclusive options are a single enum/mode, validated with a clear error; reject silent clamping of out-of-range inputs.
- New API reads idiomatically per language (Pythonic named args; Java `XxxOptions`/builders and JavaBean `getXxx()` accessors).

## Error handling & code style (Rust core)

Call these out explicitly:

- `.unwrap()` / `.expect()` / `panic!()` / `assert!()` / `todo!()` / `unimplemented!()` in library (non-test) code — require `?` with a root-cause-matched `Error` variant (`invalid_input` / `corrupt_file` / `not_found` / `io`) and context-rich messages.
- `#[allow(dead_code)]` instead of deleting dead code; obsolete `pub(crate)`/private methods left behind once their replacement landed.
- `wrapping_add` / `wrapping_mul` on counters/IDs (want `checked_*`); `.is_none()`-then-`.unwrap()`; `unwrap_or(default)` on a required config lookup.
- Memory: collecting a `RecordBatch` stream into memory; `HashSet<u32>` / `Vec<Range<u64>>` where `RoaringBitmap` / `RowAddrTreeMap` belongs; deep clones of schemas/metadata that should be `Arc`-wrapped.

## Tests required

No bugfix or feature merges without tests. Verify:

- Cases that differ only in inputs use `rstest` / `@pytest.mark.parametrize`, added to existing test modules rather than overlapping new files.
- Multi-fragment dataset scenarios and NULL edge cases (null items, all-null/empty collections, null columns) for index changes.
- Vector index tests assert recall (`>= 0.5`), not just successful creation.
- Backwards-compat changes read checked-in `test_data/` via `copy_test_data_to_tmp` with a version-asserting `datagen.py`.
- A skipped test links a tracking issue — reject a bare `#[ignore]` / `@pytest.mark.skip` / `@Ignore`.
- Also review test economy and assertion quality:
  - Identify newly added tests that duplicate existing tests or each other; request merging into cases/loops when the only difference is fixture values, null density, or branch selection.
  - Verify each added test fails against the old behavior or directly protects a changed contract.
  - For resource-usage fixes, reject tests that only assert final output size if the bug was over-read, over-retention, or temporary buffer growth; require at least one assertion at the producer/helper level.
  - Prefer fewer tests with clear distinct coverage over many narrowly overlapping regression tests.

## Documentation drift

After a symbol or behavior move, grep the PR for stale pointers and require updates to the relevant `AGENTS.md` (root + touched directory; `CLAUDE.md` is a symlink), the affected public doc comments/examples, and `docs/src/format/` + `protos/` whenever the format or messages change. Treat blanket claims ("every helper is re-exported") with suspicion — verify them literally against the code.

## Comment hygiene

- Flag diff-relative comments — "previously", "the old `2 * hint` over-fetch", "instead of a hash set", "now uses" — in code and test docs alike. A comment must read correctly to someone who never saw the change; the before/after story belongs in the commit message or PR description.
- Flag comments that paraphrase an already-readable line or the assert below them instead of stating a why (invariant, non-local consumer, prevented failure). Ask for the reason or for deletion. Do not flag plain-language summaries above genuinely dense code (bit arithmetic, unsafe byte-offset math, multi-step iterator chains) — a comment that is faster to understand than the code it heads earns its place.

## Commit hygiene

- Conventional Commits: `<type>(<optional scope>): <subject>`, type one of `feat`, `fix`, `docs`, `perf`, `ci`, `test`, `build`, `style`, `chore`. Reject a non-imperative or mistyped subject.

## Rust gotchas

For Rust changes, the patterns below are LLM-prone defects to call out explicitly:

- **Lifetime laundering.** Reject functions whose returned reference is implicitly tied to a temporary or to an unrelated input — the borrow checker collapses both lifetimes to their intersection and the caller hits a `does not live long enough` error far from the signature. Ask for split lifetimes (`<'a, 'b>`) or owned data (`String`, `Vec<T>`).
- **Sync mutex in async paths.** Flag any `std::sync::Mutex` / `std::sync::RwLock` / `std::mpsc` whose guard or receiver can span an `.await`. Require `tokio::sync::*` (or `parking_lot` only where the critical section is provably sync).
- **Drop in async paths.** A `Drop` impl that performs blocking or async-unsafe work inside an async path is a defect — silent rollback on a failed `commit().await?`, blocking I/O on an executor thread, panics from re-entry. Require an explicit `commit().await?` / `close().await?` / `shutdown().await?`, not implicit drop.
- **Unsafe without `// SAFETY:`.** Every `unsafe` block must carry a `// SAFETY:` comment naming the invariants the caller upholds (alignment, aliasing, lifetime, init state). PRs touching `unsafe` must run `cargo miri test` and reference the result. `ptr::read` / `from_raw_parts` / `transmute` over external or Arrow bytes are the usual suspects.
- **Cancel-safety unanalyzed.** Reject newly-introduced or modified async fns that don't declare `// cancel-safe` or `// NOT cancel-safe` with justification. Futures used inside `tokio::select!`, `timeout`, or `JoinSet` whose cancellation can leave state half-written need an isolating `tokio::spawn(...).await`.
- **Blanket impls in public APIs.** `impl<T: Trait> MyTrait for T` in a non-sealed public trait is a semver landmine — a downstream `impl MyTrait for Foo` conflicts the moment the upstream adds its own impl. Ask for concrete impls per type or a sealed trait.
- **Large stack values.** Returning `[T; N]` or binding `let x = [T; N];` with large `N` (rule of thumb: > ~16 KiB) belongs on the heap. NRVO is not guaranteed and debug builds will overflow. Expect `Vec<T>`, `vec![..; N].into_boxed_slice()`, or `Box::<[T]>::new_uninit_slice(N)` with explicit initialization and a `// SAFETY:` note around `assume_init()`. Reject `Box::<[T; N]>::new_zeroed()` patterns — they return `Box<MaybeUninit<_>>`, require unsafe `assume_init`, and are only sound when zero is a valid bit pattern for `T`.

## Out of scope — push back

- New dependencies where std or existing workspace crates suffice; unintentional `Cargo.lock` churn; optional/domain-specific deps not gated behind a Cargo feature.
- Reformatting of files outside the change's blast radius.
- Abstractions or generality added for hypothetical future features. The issue's stated scope is the source of truth.
