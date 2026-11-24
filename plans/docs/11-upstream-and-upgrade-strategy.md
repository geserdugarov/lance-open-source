# Upstream cooperation and sustainable upgrades

Status: **proposed technical contribution workflow**, 2026-11-13. It does not represent a commitment from project maintainers.

The implementation audit is refreshed to `67faafe65` (2026-11-16); the latest
stable tag in this checkout is `v14.0.3`. Connector observations remain pinned
as recorded in [07](07-ecosystem-evidence.md). Reconciled on 2026-11-24 with
the upstream crate-architecture proposal
([07](07-ecosystem-evidence.md#upstream-crate-architecture-proposal)).

## Shared value

Lance is an open format and SDK foundation. Public component and governance
boundaries are documented in [07](07-ecosystem-evidence.md#lancedb-public-library-and-service-boundary).

Shared contributions should improve storage correctness and interoperable
execution contracts across consumers.

| Contribution | Shared benefit | Likely owner |
| --- | --- | --- |
| Provenance and publication validation | Reliable builders across schedulers | Lance lifecycle / table owners |
| Bounded scan/search and conformance | Predictable memory and consistent adapter behavior | Scanner and adapter maintainers |
| Current-format extension boundary | Index/backend experiments without format forks | Core API and external implementations; the upstream index API and plugin project is the venue |
| Requirements and tests for the crate refactor | Task, index API and kernel seams that fit more than one consumer | The three upstream refactor projects |
| Optional compute operations | Hardware choice without mandatory vendor dependencies | Numerical owner and optional backend |
| Version/capability admission | Clear upgrade failures and testable adaptation | Common contract and each adapter |
| Benchmarks and failure fixtures | Reproducible correctness and performance evidence | Public test suites |
| Namespace and storage conformance | Reusable catalog/storage integrations | Namespace and I/O projects |

## Placement rules

**Upstream Lance:** correctness invariants, common operation metadata, current-format extension mechanisms, bounded execution and tests that more than one consumer can use.

**Public connector repositories:** engine-specific FDW code, Ray scheduling policy, namespace implementations and reusable accelerator adapters. Reuse `pglance`, `lance-ray`, `lance-spark` and `lance-namespace` where they fit. An existing REST operation should be extended or implemented before proposing a competing protocol.

**External implementations:** depend on the documented contract through a small
compatibility adapter. Keep a public mock or reference implementation so CI can
exercise the contract without a particular deployment or device. Maintainers
may prefer a narrower interface change or an existing API.

## Upstream refactor in flight

The crate-architecture proposal would move code out of `lance` and
`lance-index` in a series of small PRs while keeping the public `lance` and
`pylance` APIs through re-exports. It is a proposal; the points below apply
from the first PR that implements it.

- **Public API users are insulated; path-level consumers are not.** Adapters
  built on `lance` / `pylance` should need no source change. Anything that
  depends on internal module paths, private items or file locations will churn
  with each move: local patches, long-lived branches, and the source maps in
  chapters 02, 03 and 05.
- **Carry as little as possible in the modules being moved:**
  `rust/lance/src/dataset` (scanner, write paths, MemWAL),
  `rust/lance/src/index` and `rust/lance-index`. Upstream a needed change
  before the corresponding move lands, or design it against the new boundary.
  A patch that must be re-applied across a file move is a recurring cost.
- **Do not depend on new sub-crates yet.** Whether connectors should use the
  core table and index API crates directly, which re-exports stay long term,
  and whether DataFusion becomes optional are all deferred upstream decisions.
  Treat a new crate as internal until a stable release documents it.
- **Contribute requirements where the seams are being drawn:** the index API
  project for vector-index and kernel extension; the core table and task
  project for scan/search tasks, the predicate vocabulary and fallback
  ownership; WAL extraction for MemWAL users. Each is to have a tracking
  issue; none was linked when the proposal was read.
- **Expect no format change from it.** The proposal reorganizes crates and
  describes no file or table format change. Verify that per PR instead of
  assuming it.

## Upgrade contracts are different

| Contract | Promise / policy |
| --- | --- |
| Stable file formats | Preserve backward and forward compatibility under repository policy; honor all stable contracts, using the latest stable release as the active reference |
| Legacy formats / writers | Preserve existing read behavior; exclude them from new feature design; isolate unavoidable shared-boundary changes and test with released historical fixtures |
| Unstable file formats | May change freely; do not add compatibility fallbacks/migrations/tests for previous unstable revisions |
| Public APIs | Preserve signatures; deprecate and add replacements when needed; parameter names stay consistent across bindings |
| Rust plugin implementation surface | Current scalar interface is explicitly unstable and same-version; plan source adaptation and rebuilds per supported release. The proposed index API traits would replace it and stay same-version Rust interfaces; a C-ABI runtime plugin interface is separate, later work |
| Crate layout and internal module paths | No stability promise. The proposed refactor moves implementations between crates while preserving `lance` / `pylance` public APIs through re-exports; direct sub-crate dependencies are unsupported until upstream decides |
| New external execution protocol | Initially experimental; stabilize only after multiple consumers and tests; required capabilities must fail clearly on unsupported workers |
| Native ABI / RPC | A separate commitment if introduced; document ownership, versions, errors and supported types explicitly |

A stable file format does not make the Rust ABI stable. Conversely, a library API change need not change the file format. Tracking these contracts separately is essential to reducing upgrade work. Repository policy is defined in [AGENTS.md](../../AGENTS.md), with release context in [04](04-lance-versions.md).

Track specification, runtime and binding availability independently. Transaction
V2's schema is present while runtime execution is unsupported; bounded
consolidation is available across bindings while UUID-selected consolidation
is Rust-only. Independent covering fields and scalar row-address layouts also
require capability-aware metadata handling. A successful recompile or generated
protobuf type is insufficient evidence for an admitted operation.

## Release and CI lanes

Maintain two lanes:

1. **Qualified stable:** latest approved stable Lance release and explicitly selected stable historical fixtures, with pinned connector/device artifacts. Promote a new stable release only after the matrix passes.
2. **Development integration:** current main or a pinned prerelease for upcoming APIs. Exercise new capabilities in ordinary unit/integration tests. Do not add alpha/beta/RC entries to `python/python/tests/compat` or encode prerelease boundaries there.

For a transition, build old and new adapter packages independently. Run stable-reader/writer fixture checks, compare query results, verify the optional backend can reopen canonical data on CPU, then run failure and deployment tests. Keep writers on approved stable capabilities until all participating readers support the intended stable contract. Do not accidentally make the current development branch a permanent compatibility surface.

Rolling operation protocol upgrades require explicit capability negotiation; initially drain a job on its original tested worker set and start new jobs on the new set. Reader retention and rollback policy must account for any newly emitted stable feature. Rollback is not simply replacing a binary if the old binary cannot understand newly enabled functionality.

## Proposed maintainership workflow

Before implementation, refresh the [watchlist](07-ecosystem-evidence.md#current-collaboration-watchlist), search overlapping PRs, reproduce the actual gap, and discuss a bounded design with the relevant maintainers. If the change touches a seam owned by one of the three refactor projects, take it to that project's tracking issue first.

For each feature:

1. Write a one-page RFC naming the use case, owner, existing API, proposed change, alternative and compatibility effect.
2. Demonstrate it with an existing connector plus a small second consumer/test adapter; Spark can provide useful independent feedback without becoming a deployment requirement.
3. Submit a minimal change and regression tests. Keep hardware/provider implementation separate from general contracts when possible.
4. Add conformance and observability, publish capability documentation, and only then claim a supported combination.
5. Revisit the adapter on each stable release and record compatibility changes and measured upgrade effort.

Changes to Rust/Python/Java logic follow their language guides and all required lint/test gates. A workspace dependency update must refresh all three lockfiles as required by the root guide. API extensions should not duplicate validation in bindings or add vendor flags throughout unrelated modules.

## Measures of success

Record adapter rebuild/adaptation time, manual upgrade steps and compatibility
failures before implementation. A new optional backend or SQL-engine variant
should change its adapter and conformance cases; shared core changes need a
demonstrated capability gap. Use a trial upgrade and the conformance matrix to
assess whether the boundary reduces recurring semantic rewrites. While the
refactor is in flight, also record the conflicts met when rebasing local
changes across moved modules, and whether the adapter's dependency closure
still includes DataFusion.
