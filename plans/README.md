# Lance architecture and distributed execution plans

Start with the [proposed distributed/extensibility plan](distributed-extensibility-plan.md) or open the [illustrated HTML onboarding guide](distributed-onboarding.html) in a browser. The HTML is a self-contained local file: diagrams and interactions work without a server or network. Links to external sources need network access; linked Markdown/source files remain repository files.

This set is planning material for this fork, including internal staffing and
positioning. Keep these documentation commits out of upstream PRs. Prepare
public implementation proposals separately under the placement rules in
[11](docs/11-upstream-and-upgrade-strategy.md#placement-rules).

## New research and design set

Prepared 2026-11-13. This is documentation and proposed work, not a certification of a deployed system. It separates the inspected upstream implementation snapshot, published connector versions, proposals and unverified integrations.

| Document | Read it for |
| --- | --- |
| [06 · Distributed current state](docs/06-distributed-current-state.md) | Code-backed capability inventory, coupling/cohesion assessment, source baseline and research limits |
| [07 · Ecosystem evidence](docs/07-ecosystem-evidence.md) | Public releases/commits, Ray/Spark/PostgreSQL/Namespace/Ascend, LanceDB boundary and dated issue watchlist |
| [08 · Target architecture](docs/08-extensible-target-architecture.md) | Ownership, extension points, execution contracts, version admission and design decisions |
| [09 · SQL and Ray](docs/09-sql-and-ray-integration.md) | Snapshot semantics, engine boundaries, recovery, global top-k, memory and integration stages |
| [10 · NPU acceleration](docs/10-npu-acceleration.md) | Kernel priorities, optional backend design, numerical invariants and qualification |
| [11 · Upstream and upgrades](docs/11-upstream-and-upgrade-strategy.md) | Proposed shared value, public/private placement, compatibility and release workflow |
| [12 · Validation](docs/12-validation-and-experiments.md) | Experiments, failure matrix, test ownership and measurable promotion gates |
| [13 · Examples and records](docs/13-examples-and-contracts.md) | Existing API examples, explicit prerequisites, proposed execution envelope and recovery algorithm |

## Existing vector-focused guides

These retain their original 2026-11-05 implementation baseline. Issue-status refreshes are dated separately in documents 05 and 07.

| Document | Subject |
| --- | --- |
| [00 · Overview](docs/00-overview.md) | Repository layers and vector lifecycle |
| [01 · Storage](docs/01-vector-storage.md) | Encodings and physical vector storage |
| [02 · Indices](docs/02-vector-indexes.md) | Algorithms and index construction |
| [03 · Persistence and search](docs/03-index-on-disk-and-search.md) | Index layout, visibility and query execution |
| [04 · Versions](docs/04-lance-versions.md) | Stable release and format boundaries |
| [05 · Distributed index creation](docs/05-distributed-vector-index-creation.md) | Detailed build/merge/commit protocols and source map |

The new roadmap assumes PostgreSQL as the initial engine reference, Ray as the primary scheduler and Ascend as the first optional NPU backend. Exact product/version selection, hardware qualification and performance measurements are phase-0 work. No private service implementation was inspected.
