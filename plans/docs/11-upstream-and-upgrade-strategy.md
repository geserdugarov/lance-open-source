# Upstream cooperation and sustainable upgrades

Status: **proposed collaboration model**, 2026-11-13. It does not represent an agreement with LanceDB, Huawei, or project maintainers.

## Shared value

Lance is an open format and SDK foundation. LanceDB has an open-source embedded library as well as a commercial distributed service; these are distinct from the format project. Public product and governance sources are in [07](07-ecosystem-evidence.md#lancedb-and-huawei-a-shared-foundation).

The proposed shared investment is reliable, extensible storage behavior and interoperable execution contracts. These make managed services cheaper to maintain while making additional engines and hardware feasible.

| Contribution | Value for the Lance community and LanceDB | Value for Huawei | Likely owner |
| --- | --- | --- | --- |
| Provenance and publication validation | More reliable distributed builders regardless of service scheduler | Fewer bespoke correctness patches | Lance lifecycle / table owners |
| Bounded scan/search APIs and conformance cases | Predictable service memory and consistent behavior across adapters | Ray and SQL integration reuse | Lance scanner plus adapter maintainers |
| Current-format extension boundary | Additional index/backend experimentation without format forks | Internal implementations can live outside core | Small upstream API with external implementations |
| Optional compute operations | Hardware competition without mandatory vendor dependencies | Ascend qualification and optimization | Numerical owner plus optional backend package |
| Version/capability admission | Clear failure modes during upgrades | Smaller, testable adaptation work | Common contract and each adapter |
| Benchmarks and failure fixtures | Reproducible engineering decisions | Verifiable deployment readiness | Shared public suites; private workload data stays private |
| Namespace and storage conformance | Reusable catalog/storage integrations | Cloud/OBS/catalog integration behind providers | Existing namespace and I/O projects |

Managed routing policy, tenancy, fleet automation, billing, SLO management and proprietary operational tooling can remain product-owned. This boundary is a proposal for cooperation, not a limit on what contributors may build in open source. Do not claim private service behavior or duplicate private code based on public marketing diagrams.

## Placement rules

**Upstream Lance:** correctness invariants, common operation metadata, current-format extension mechanisms, bounded execution and tests that more than one consumer can use.

**Public connector repositories:** engine-specific FDW code, Ray scheduling policy, namespace implementations and reusable accelerator adapters. Reuse `pglance`, `lance-ray`, `lance-spark` and `lance-namespace` where they fit. An existing REST operation should be extended or implemented before proposing a competing protocol.

**Product or internal packages:** deployment choices, private algorithms, workload policy, infrastructure credentials and vendor operational integrations. An internal feature should depend on a public contract with a small compatibility adapter. Keep a public mock or reference implementation so CI can exercise the contract without private source or hardware.

**Temporary core patches:** record an owner, upstream issue/proposal, invariant, affected releases, tests and removal condition. Track the patch count and rebase cost. An interface proposal is not automatically accepted upstream; maintainers may prefer a narrower change or an existing API.

## Upgrade contracts are different

| Contract | Promise / policy |
| --- | --- |
| Stable file formats | Preserve backward and forward compatibility under repository policy; honor all stable contracts, using the latest stable release as the active reference |
| Legacy formats / writers | Preserve existing read behavior; exclude them from new feature design; isolate unavoidable shared-boundary changes and test with released historical fixtures |
| Unstable file formats | May change freely; do not add compatibility fallbacks/migrations/tests for previous unstable revisions |
| Public APIs | Preserve signatures; deprecate and add replacements when needed; parameter names stay consistent across bindings |
| Rust plugin implementation surface | Current scalar interface is explicitly unstable and same-version; plan source adaptation and rebuilds per supported release |
| New external execution protocol | Initially experimental; stabilize only after multiple consumers and tests; required capabilities must fail clearly on unsupported workers |
| Native ABI / RPC | A separate commitment if introduced; document ownership, versions, errors and supported types explicitly |

A stable file format does not make the Rust ABI stable. Conversely, a library API change need not change the file format. Tracking these contracts separately is essential to reducing upgrade work. Repository policy is defined in [AGENTS.md](../../AGENTS.md), with release context in [04](04-lance-versions.md).

## Release and CI lanes

Maintain two lanes:

1. **Qualified stable:** latest approved stable Lance release and explicitly selected stable historical fixtures, with pinned connector/device artifacts. Promote a new stable release only after the matrix passes.
2. **Development integration:** current main or a pinned prerelease for upcoming APIs. Exercise new capabilities in ordinary unit/integration tests. Do not add alpha/beta/RC entries to `python/python/tests/compat` or encode prerelease boundaries there.

For a transition, build old and new adapter packages independently. Run stable-reader/writer fixture checks, compare query results, verify the optional backend can reopen canonical data on CPU, then run failure and deployment tests. Keep writers on approved stable capabilities until all participating readers support the intended stable contract. Do not accidentally make the current development branch a permanent compatibility surface.

Rolling operation protocol upgrades require explicit capability negotiation; initially drain a job on its original tested worker set and start new jobs on the new set. Reader retention and rollback policy must account for any newly emitted stable feature. Rollback is not simply replacing a binary if the old binary cannot understand newly enabled functionality.

## Proposed maintainership workflow

Before implementation, refresh the [watchlist](07-ecosystem-evidence.md#current-collaboration-watchlist), search overlapping PRs, reproduce the actual gap, and discuss a bounded design with the relevant maintainers.

For each feature:

1. Write a one-page RFC naming the use case, owner, existing API, proposed change, alternative and compatibility effect.
2. Demonstrate it with an existing connector plus a small second consumer/test adapter; Spark can provide useful independent feedback without becoming a deployment requirement.
3. Submit a minimal change and regression tests. Keep hardware/provider implementation separate from general contracts when possible.
4. Add conformance and observability, publish capability documentation, and only then claim a supported combination.
5. Revisit the adapter on each stable release; remove obsolete patches and record measured upgrade effort.

Changes to Rust/Python/Java logic follow their language guides and all required lint/test gates. A workspace dependency update must refresh all three lockfiles as required by the root guide. API extensions should not duplicate validation in bindings or add vendor flags throughout unrelated modules.

## Measures of success

Record a baseline in phase 0: core patch count, core files touched per internal feature, adapter rebuild/adaptation time, manual upgrade steps, and compatibility failures. Proposed direction: a new optional backend or SQL-engine variant changes its adapter and conformance cases; unrelated core files remain stable unless a real shared capability is added.

Use a trial upgrade as the proof. A design with many traits but recurring semantic rewrites has not achieved the objective. The plan explicitly budgets an upgrade exercise and acceptance review rather than promising maintenance-free private features.
