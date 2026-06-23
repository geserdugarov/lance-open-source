---
name: develop
description: Project conventions and recurring gotchas for implementer agents working on Lance (a Rust workspace with Python/Java bindings), plus Rust-specific LLM gotchas. Use before opening a PR for any change under rust/, python/, java/, protos/, or docs/.
---

# Developer skill — Lance

Lance is a Rust workspace (`rust/lance*`) with Python (PyO3/maturin) and Java (JNI) bindings. The Rust core is the source of truth; the bindings are thin wrappers. Before you start, read the root `AGENTS.md` plus the directory guide for whatever you touch (`rust/AGENTS.md`, `python/AGENTS.md`, `java/AGENTS.md`, `protos/AGENTS.md`, `docs/src/format/AGENTS.md`).

## Commits and PR titles

- Conventional Commits: `<type>(<optional scope>): <subject>`. Type is one of `feat`, `fix`, `docs`, `perf`, `ci`, `test`, `build`, `style`, `chore`.
- The PR title **and** body are validated by commitlint (`.github/workflows/pr-title.yml`) — a title that doesn't follow the spec fails CI.
- Imperative mood, short and specific. Match the style in `git log --oneline -20`.
- Keep PRs focused — no drive-by refactors, reformatting, or cosmetic churn outside the change's blast radius.

## Pre-merge checklist

Run the lint/test gates for **every** language surface you touched, even when they are slow. Follow the environment rules in each directory guide — don't substitute a different toolchain because a command looks missing, unavailable, or slow.

Rust (`rust/`):

- `cargo fmt --all`
- `cargo clippy --all --tests --benches -- -D warnings`
- `cargo check --workspace --tests --benches`
- `cargo test --workspace` (use `cargo test -p <package> <test_name>` while iterating)
- `cargo miri test` if the diff touches any `unsafe` block

Python (`python/`): every command goes through `uv run` — never bare `python`/`pytest`/`maturin`. Run `make install` once per fresh worktree first, and `make build` after Rust changes.

- `uv run make lint`
- `uv run make format`
- `uv run make test`
- `uv run make doctest`

Java (`java/`): use `./mvnw`, not `mvn`.

- `./mvnw spotless:apply && cargo fmt --manifest-path ./lance-jni/Cargo.toml --all`
- `cargo clippy --tests --manifest-path ./lance-jni/Cargo.toml`
- `./mvnw test`

If a required gate genuinely cannot run, state the blocker explicitly in the PR summary instead of skipping it silently. A "missing module/command" error from a bare `python`/`pytest`/`maturin` invocation is an environment-usage mistake, not a real failure — fix the `uv run` usage and rerun before reporting anything as broken.

## Cross-language bindings

The recurring failure mode here is logic drifting out of the Rust core into the bindings. Keep the discipline:

- Centralize validation and logic in the Rust core; Python and Java stay thin wrappers.
- Keep parameter names identical across Rust, Python, and Java — rename in all three or none.
- Never break a public API signature. Deprecate with `#[deprecated]` / `@deprecated` and add a new method instead of mutating the old one.
- Extend existing methods with named/optional arguments rather than adding parallel methods that take policy/config objects. Python should read Pythonic (`cleanup_old_versions(..., retain_versions=N)`); Java should encapsulate optional params in `XxxOptions`/builders and expose JavaBean `getXxx()` accessors.
- Replace mutually exclusive boolean flags with a single enum/mode parameter, and reject conflicting or out-of-range options at the API boundary with a descriptive error — never silently clamp or adjust.

## Error handling (Rust core)

- No `.unwrap()` / `.expect()` / `panic!()` / `assert!()` in library code for fallible paths — use `?` with the right `Error` variant. Reserve `.unwrap()` for tests.
- Match the `Error` variant to the root cause: `invalid_input` for caller data, `corrupt_file` for format/integrity, `not_found` for missing resources, `io` for I/O.
- Return `Error::NotSupported` instead of `todo!()` / `unimplemented!()`; test with `Result::Err` assertions, not `#[should_panic]`.
- Include full context in messages — variable names, values, sizes, types, indices.
- Use `checked_add` / `checked_mul` for counters and IDs; return an error on overflow.

## Tests

We do not merge code without tests. For every bugfix and feature:

- Use `rstest` (Rust) or `@pytest.mark.parametrize` (Python) for cases that differ only in inputs; name Rust cases `#[case::<name>(...)]`. Extend the existing `test_{module}.py` or the bottom-of-file `#[cfg(test)] mod tests` instead of adding overlapping new files.
- Replace `print()` / `println!` in tests with `assert`.
- Cover multi-fragment scenarios for dataset operations, and NULL edge cases for index tests (null items, all-null/empty collections, null columns).
- Vector index tests must assert recall (`>= 0.5`), not just that creation succeeded.
- For backwards compatibility, read checked-in datasets from `test_data/` via `copy_test_data_to_tmp`, with a `datagen.py` that asserts the Lance version used.
- In Rust tests prefer the ergonomic helpers: `record_batch!()`, `gen_batch()` (`.col()`, `.into_reader_rows()`), `.try_into_batch()`, plain `"memory://"` URIs, and `batch["col"]` access. Assert on both the error variant and the message content.
- Before finalizing tests, do a redundancy pass:
  - List each added/modified test and the distinct behavior it protects.
  - Merge tests that differ only by input shape, null density, or branch case into `rstest` cases or a small named loop, unless separate setup materially improves clarity.
  - Prefer one focused helper/unit test that covers sibling branches over multiple tests with repeated setup.
  - Keep end-to-end tests only when they exercise an integration boundary that helper tests cannot cover.
  - Ensure assertions observe the behavior being fixed. For memory, prefetch, streaming, or buffering bugs, add a direct producer/helper assertion when output-length checks could pass after post-processing/truncation.
  - Remove incidental unsafe/manual byte assertions when existing tests already cover byte correctness.

## Comments

Write every comment against the current state of the code, as if it had always been this way:

- Prefer stating why the code below exists — the invariant it protects, the non-local consumer it serves, the failure it prevents — over describing what it does. If a comment paraphrases an already-readable line (`// cap the copy at what we still need` above `.min(remaining_rows)`) or the assert below it, delete it or replace it with the reason.
- Exception: a plain-language summary of genuinely dense code (bit arithmetic, unsafe byte-offset math, a multi-step iterator chain) is fine even though it "restates" the code. The test is whether the comment is faster to understand than the code below it, not whether it repeats it.
- No diff-relative wording: "previously", "the old X", "instead of a `HashSet`", "no longer", "now sized to". Those sentences address the reviewer and go stale the moment the PR merges — put the before/after story in the commit message or PR description instead.
- Same rule for test doc comments: describe the behavior the test pins down, not the bug or implementation it replaced.

## Documentation drift

When you move or rename a public symbol, constant, or behavior, update the docs in the same PR:

- the relevant `AGENTS.md` (root, `rust/`, `python/`, `java/`, `protos/`, `docs/src/format/`) — note `CLAUDE.md` is a symlink to `AGENTS.md`.
- the doc comments on the affected public API; keep examples compiling — write Rust doctests as a compiled `# async fn` rather than marking them `ignore`.
- the format/spec docs under `docs/src/format/` and the `.proto` comments under `protos/` whenever the on-disk format or messages change.

## Rust gotchas

These are patterns LLMs reliably get wrong in async, Arrow-native, `unsafe`-touching code like Lance:

- **Lifetime laundering.** Don't return references whose lifetime is implicitly bound to a temporary or to an unrelated input — a cache keyed by `&'a str` collapses to an empty lifetime the moment any of its inputs goes out of scope. If two references have independent lifetimes, give them independent parameters (`<'a, 'b>`); if the borrow checker still won't let it pass, store owned data (`String`, `Vec<T>`) instead of `&str` / `&[T]`. When asking for help, show the calling code — lifetime errors usually live at the call site, not the signature.
- **Sync mutex inside async.** Use `tokio::sync::Mutex` whenever a guard may be held across an `.await`. `std::sync::Mutex` blocks the worker thread and deadlocks under tokio. Same rule for `RwLock`, channels, and `Notify` — pick the async-aware variant.
- **Drop order / RAII in async.** `Drop` runs in reverse declaration order and cannot be `async`. Transactions, file handles, and pooled connections must be closed explicitly (`tx.commit().await?`, `handle.shutdown().await?`) — never lean on implicit drop in async paths, which silently rolls back, blocks the executor, or panics.
- **Unsafe without invariants.** Every `unsafe { ... }` block needs a `// SAFETY:` comment naming the invariants the caller is upholding (alignment, aliasing, lifetime, init state). `ptr::read` on a network/Arrow buffer without an alignment guarantee is UB even when the test passes on x86. Run `cargo miri test` against any change touching `unsafe`; bare `cargo test` won't catch UB that happens to compile.
- **Cancel-safety in async.** Any future passed to `tokio::select!`, `timeout`, or `JoinSet` can be dropped mid-`.await` — a cancellation between "write" and "ack" duplicates work on retry. Annotate every async fn `// cancel-safe` or `// NOT cancel-safe` with a one-line justification, and isolate non-cancel-safe critical sections behind `tokio::spawn(...).await` so the join, not the inner future, is what gets cancelled.
- **Blanket impls in public APIs.** `impl<T: Trait> Mine for T` is a semver landmine: a downstream `impl Mine for Foo` conflicts the moment your crate adds its own impl, and the breakage doesn't show up until someone else upgrades. Reserve blanket impls for sealed traits; otherwise write concrete impls per type.
- **Large values on the stack.** `fn f() -> [u8; 1 << 20]` and `let buf = [0u8; 1 << 20];` overflow in debug builds and aren't reliably elided by NRVO. Allocate on the heap instead: `vec![0u8; N].into_boxed_slice()` for a zeroed `Box<[u8]>`, or `Box::<[T]>::new_uninit_slice(N)` followed by per-element initialization and an `unsafe { slice.assume_init() }` (with a `// SAFETY:` note) when the element type doesn't have a meaningful zero. Don't reach for `Box::<[T; N]>::new_zeroed()` — it yields `Box<MaybeUninit<[T; N]>>`, needs unsafe `assume_init`, and is UB unless all-zero is a valid bit pattern for `T`.

Before committing Rust changes, run the Rust gates in the Pre-merge checklist above (and `cargo miri test` for any `unsafe` change).

## Out of scope without explicit ask

- Adding dependencies — prefer std or existing workspace crates; gate optional/domain-specific deps behind a Cargo feature, and keep `Cargo.lock` changes intentional (revert unrelated bumps).
- Reformatting unrelated files or churning whitespace.
- "Future-proofing" abstractions for hypothetical features. Implement what the issue asks for and stop.
