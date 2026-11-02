# Lance clustering and NPU acceleration: an Ascend feasibility study

**Research date:** 2026-10-30. **Local baseline:** Lance 14.0.2, code release commit `40f15ed1b`; inspected checkout `72c65d07f2687d80b8a99a74beb1fe8976612aa8`. **Upstream status:** GitHub PR/discussion APIs checked on the research date; observed `main` was `fb465bb55c0dbb851fe8995aabd0e6ed2ecfdb70`. **Companion:** [interactive illustrated guide](lance-clustering-npu-ascend-guide.html).

This report combines local source inspection, the latest documents in `plans/docs/`, current upstream development, primary research papers, and vendor documentation. It contains no new Lance, Ascend, GPU, or S3 benchmark measurements. Published results, implementation facts, analytical examples, and proposed engineering work are identified separately. “Current checkout” and “upstream main” are deliberately separate baselines.

## 1. Recommendation

**Prototype Ascend acceleration for batched vector-to-centroid assignment first, then for IVF/PQ training. Keep Lance responsible for storage, row identity, index serialization, and commits.** This addresses a computation already present in Lance and potentially reusable by a physical clustering provider. It can produce useful results before a full clustering engine exists.

The important distinction is between **computing where a row belongs** and **moving that row into a new physical layout**. NPUs can substantially accelerate the first when it involves dense vector arithmetic. The second involves scanning, gathering, shuffling, encoding, uploading, and maintaining row/index references. A faster matrix multiplication does not remove those costs.

The strongest initial targets are:

1. Assignment of large batches to fixed IVF centroids, including a physical clustering provider's routing calculation.
2. Repeated assignment during k-means, followed by measured optimization of centroid reductions.
3. PQ training and encoding, using batches across subspaces to avoid many small launches.
4. Later, batched IVF search and compatible RaBitQ computations, after build integration and correctness are established.

Scalar liquid clustering by Hilbert/Z-order is a different workload. Its low-dimensional key transformations, sorting, and data shuffle have less obvious benefit from matrix hardware. Start that work with a CPU/distributed sorting baseline and measured profiles.

**Do not assume that `accelerator="npu"` enables Ascend in Lance.** The local Python accelerated index path supports CUDA/MPS for IVF_PQ, explicitly validates those strings, and includes CUDA-specific memory/precision handling. No Ascend/CANN/`torch_npu` backend was found in the inspected Rust/Python implementation. Apple's MPS path uses the GPU, not the Neural Engine. [Local accelerator implementation][L4] [Torch device selection][L5]

## 2. Latest clustering plans and implementation status

### 2.1 What the local plans actually describe

The newest relevant local document is [Lance IVF partitioning and data reorganization on S3][L1], added by `72c65d07f` on October 30. Its source baseline is the v14.0.2 implementation. Read it together with the [distributed vector index creation plan][L2] and [vector index architecture][L3].

Its central conclusions remain applicable to this checkout:

- Building an IVF index groups **index entries** by centroid; it does not reorder base-table rows.
- Shallow clones reuse objects. Deep clones copy referenced objects. Neither operation clusters rows by itself.
- Physical reorganization writes replacement data objects. An external integration supplies routing, scheduling, bounded buffering, and output order.
- Tagged Fragment Reuse Index (FRI) transitions can preserve existing index payloads across supported stable partitions of address-based tables.
- The current stable-partition map describes a destination label per source position, with source-relative order preserved within each destination. It cannot encode an arbitrary sort within a destination.
- Python's rewrite binding does not expose the complete Rust FRI publication protocol.
- The precomputed IVF assignment dataset is a different artifact from an FRI row map; its current loader builds an in-memory hash table.

These are implementation constraints, not objections to acceleration. They identify the boundary at which an NPU should return labels or codes to the existing storage machinery.

### 2.2 Three meanings of clustering

| Meaning | Objective | Typical calculation | Physical effect |
| --- | --- | --- | --- |
| IVF clustering | Route vectors into an approximate search index | k-means, distance, argmin | Groups entries inside index storage |
| Vector-based table clustering | Place related base rows near one another | Assignment using a chosen vector model, then stable routing or sorting | Rewrites base fragments to improve refinement/payload locality |
| Scalar liquid clustering | Improve skipping for predicates on several columns | Rank/coordinate mapping, Hilbert or Z-order keys, sorting | Rewrites base fragments to improve zone-map selectivity |

A fourth operation, distributing work across a **compute cluster**, is scheduling. Adding Ray, Spark, or multiple NPUs does not by itself implement any of the three layouts.

### 2.3 Upstream developments beyond this checkout

Dates and merge states below were checked through GitHub's live API. Search-engine snapshots were stale in several cases, including #9878. The local checkout was not upgraded.

| Work | Observed status on 2026-10-30 | Meaning for this study |
| --- | --- | --- |
| [Discussion #4125: partitioning versus clustering][S1] | Design discussion, started 2025-07-02 | Explores clustering and zone-map-driven locality as alternatives to fixed physical partitioning |
| [Discussion #8921: liquid clustering][S2] | Proposal; API reports last update 2026-09-13 | Proposes scalar multi-column Hilbert/Z-order, task-wide quantile-rank normalization, incremental maintenance, and zone-map use; not an IVF/NPU implementation |
| [PR #9878: provider-managed clustering metadata][S3] | Merged 2026-10-19; format present locally | Defines immutable clustering definitions, object references, and writer protection; the local runtime still reserves that capability |
| [PR #10383: runtime metadata support][S4] | Merged into main 2026-10-28 | Preserves and validates provider metadata through runtime operations; providers still own layout algorithms |
| [PR #10409: compaction output properties][S5] | Merged into main 2026-10-29 | Lets a task request output clustering ID and opaque metadata; does not choose grouping or sort rows |
| [PR #10445: explicit compaction groups][S6] | Merged into main 2026-10-29 | Accepts application-defined candidate fragment groups, including nonadjacent inputs, and plans useful compaction within them |
| [PR #10415][S7] | Closed without merge | Earlier explicit-fragment planning work; do not count it as a separately delivered feature |
| [PR #10440: revert compaction suffix relabeling][S8] | Merged 2026-10-28 | Main does not promise table-wide scan order through compaction; check the selected version's ordering contract |
| [PR #10357: definition allocation in UpdateConfig][S9] | Open discussion draft | Persisted transaction extension is still under discussion; its old “gap” description must be read alongside merged #10383 |
| [Issue #10472: Transaction V2 metadata propagation][S10] | Open | A remaining integration item, not an implemented clustering algorithm |

The September proposal's config/API names are proposed designs, not a supported v14 API. The October work establishes a provider-oriented metadata and maintenance foundation. A new accelerator should fit that foundation rather than hard-code the proposal's older declaration layout.

The local plan mentions an external `lance-clustering` package and `ClusteringConfig`, but neither exists in this repository. A public `lance-format/lance-clustering` repository lookup returned 404 during research. That does not establish whether a private or differently named implementation exists. No capability claim in this report depends on it.

### 2.4 Current checkout capability matrix

| Capability | Local v14.0.2 status | Acceleration consequence |
| --- | --- | --- |
| CPU IVF training and assignment | Implemented, with parallel distance kernels, hierarchical training, and optional streaming coreset work | Use this optimized implementation as the baseline |
| Python accelerated index creation | IVF_PQ; CUDA/MPS; other index types fall back to CPU | Existing precedent, but not a generic NPU backend |
| External centroids/PQ codebooks | Accepted through current index creation inputs | A practical first handoff from an external NPU trainer |
| External IVF assignment dataset | Accepted with validation constraints; loaded into a hash map | Convenient small/medium prototype; unsuitable as an assumed bounded-memory billion-row solution |
| Distributed index build protocol | External orchestration with fragment-scoped builds, merge, commit | The existing Python Torch acceleration path enforces single-node behavior |
| Tagged FRI stable partition | Implemented for address-based datasets | Useful for vector-based physical clustering without rebuilding every index |
| Tagged FRI with stable row IDs | Rejected in this checkout | Must not be promised by a prototype |
| Older FRI v0 with stable row IDs | Debug-build-only support | Not a production alternative for tagged clustering rewrites |
| New provider clustering metadata | Specified; `FLAG_OBJECT_METADATA` is unsupported locally | Upstream runtime support requires a different baseline |
| Native NPU search or build | No implementation found | Engineering work is required |

Sources: [feature gates][L6], [Rust index builder][L7], [Python index creation][L4], [k-means][L8], [FRI integration][L9]. Format documentation alone is insufficient evidence of writer support.

## 3. Calculations in Lance that can benefit

### 3.1 Existing CPU baseline is already substantial

Lance's Rust k-means supports L2, cosine, and dot paths; binary/Hamming data has a separate path. Cosine training normalizes vectors. For more than 256 centroids, the default branching configuration enables hierarchical training. Optional streaming training reduces raw sample residency through weighted coresets. The usual sample target is `sample_rate × K`, with a default rate of 256. These mechanisms reduce or reshape the work compared with naive full-data Lloyd iterations. [k-means source][L8] [IVF configuration][L10]

The linalg layer includes architecture-specific SIMD and eligible FP16 AMX kernels. A comparison against a scalar Python loop would misrepresent the opportunity. Keep CPU core count, ISA, sample, iteration/convergence policy, and recall in the benchmark record. [FP16 dot kernels][L11]

### 3.2 Workload ranking

Ratings below are engineering assessments, not measured speedups.

| Stage | Calculation and scaling | NPU suitability | Recommended treatment |
| --- | --- | --- | --- |
| Full-data IVF assignment | `X Cᵀ`, norms, row-wise argmin; approximately `2NDK` multiply/add FLOPs | **High** for large dense batches | First prototype; return compact labels |
| IVF k-means assignment | Same operation repeated on training data | **High**, subject to hierarchy/batch size | Keep sample and centroids resident where practical |
| Centroid update | Per-cluster sums and counts, `O(ND)` per pass | **Medium** | Use vector reductions/segmented aggregation; measure scatter contention |
| Cosine normalization | Norms and elementwise division, `O(ND)` | **Medium** alone; high as part of a fused pipeline | Normalize once according to the metric contract |
| Residual calculation | `rᵢ = xᵢ − c[aᵢ]` | **Medium** | Fuse with assignment/PQ encoding while vectors are resident |
| PQ codebook training | Many smaller k-means problems | **High** in sufficiently large grouped batches | Batch subspaces; avoid one tiny device launch per centroid/subspace |
| PQ encoding | Nearest sub-centroid for each subvector | **High/medium** | Fuse distance and selection; produce compact codes |
| RaBitQ dense rotation | Matrix transform, normalization, sign/code generation | **High** for a dense rotation | Preserve Lance's exact representation and metadata |
| RaBitQ fast rotation | Sign flips and butterfly mixing | **Medium/uncertain** | Vector-oriented kernel, not automatically a large GEMM |
| Batched exact/IVF_FLAT search | Many query/vector distances plus top-k | **High** when resident and batched | Later serving experiment with a latency budget |
| Single-query centroid routing | Matrix-vector calculation over a relatively small model | **Low/medium** | CPU often avoids launch and transfer overhead |
| PQ/RQ compressed-code scan | LUT construction, gathers, integer/bit operations, selection | **Medium**, architecture-sensitive | Requires custom layout/kernels; not a direct CPU FastScan transplant |
| HNSW graph traversal | Dependent random gathers, branchy frontier expansion | **Low** for naive offload | Keep control on CPU; investigate only batched distance work |
| Graph construction/pruning | Candidate generation plus batches of pairwise distances | **Medium** | Accumulate independent distance tasks; keep graph mutation on CPU |
| Scalar curve-key generation | Quantile lookup, integer bit operations, coordinate transform | **Low/medium** | Profile first; small key dimension limits matrix utilization |
| Sort, shuffle, fragment write | Comparisons/radix work, memory movement, network and storage I/O | **Low** as a first Ascend target | Optimize streaming and storage independently |
| FRI rank/count lookup, commits | Small metadata operations, mapping reads, validation | **Low** | Keep in Lance's host-side implementation |

The RQ distinction matters: [Lance's fast rotation][L12] uses random sign flips, Walsh–Hadamard-style mixing, and additional mixing for non-power-of-two dimensions. Replacing it with an unrelated dense rotation would change the model; it is not a transparent implementation optimization.

### 3.3 Why distance calculations map to matrix hardware

For `B` vectors of dimension `D` and `K` centroids:

```text
X: B × D       C: K × D
G = X Cᵀ: B × K

L2_squared(i,j) = sum_d X[i,d]^2 + sum_d C[j,d]^2 - 2 G[i,j]
assignment(i)  = argmin_j L2_squared(i,j)

dot ranking:     maximize G[i,j]
cosine ranking:  normalize according to Lance's metric, then compare
```

The matrix multiplication performs most arithmetic; vector units can handle norms, corrections, masking, and reductions. Tile both the vector and centroid axes, and retain only the current best distance/ID for each row. There is no need to materialize the dataset-wide distance matrix.

Selection must remain explicit. Fast matrix multiplication followed by an expensive generic sort can lose much of the gain. For assignment, use argmin; for search, use a bounded top-k scheme whose approximation, if any, is part of the evaluated recall contract.

### 3.4 Worked size example

**Analytical example, not a benchmark:** `N = 100,000,000`, `D = 768`, `K = 4096`, FP32 input, `B = 8192`. Decimal GB/TB are used for dataset sizes; MiB is binary.

| Quantity | Calculation | Size/work |
| --- | --- | --- |
| Raw vectors | `N × D × 4` | 307.2 GB |
| Resident centroids | `K × D × 4` | 12 MiB |
| One input batch | `B × D × 4` | 24 MiB |
| One full batch distance matrix | `B × K × 4` | 128 MiB |
| Centroid tile of 1024, same batch | `B × 1024 × 4` | 32 MiB of scores |
| Dataset-wide distance matrix | `N × K × 4` | 1.6384 TB: avoid materializing it |
| One exhaustive assignment pass | `2 × N × D × K` | 629.15 trillion FLOPs, plus norms/reductions |
| Partition labels | `N × 4` | 0.4 GB |
| Assignment labels plus uint64 row IDs | `N × (4 + 8)` | 1.2 GB raw values, before containers/encoding |

For an illustrative flat Lloyd trainer, `N_sample = 256 × 4096 = 1,048,576` and 20 assignment iterations imply about 131.94 trillion multiply/add FLOPs. Lance's hierarchical/coreset execution does not necessarily perform that flat workload. This number is a sizing reference, not an estimate of current Lance runtime.

The batch figures also exclude buffers for reductions, model layouts, padding, double buffering, allocator reservations, and runtime workspace. A batch that “fits” by raw tensor sizes may still exhaust device memory.

## 4. What kind of NPU is relevant?

“NPU” describes several distinct execution environments. Neither TOPS nor the presence of an AI accelerator proves that a database calculation can run efficiently on it.

| Hardware/software class | Relevant programming model | Assessment for Lance |
| --- | --- | --- |
| Server Ascend | CAN, TorchNPU, Ascend C, Index SDK; capabilities depend on exact device/runtime | Strong candidate for training and batched dense work; custom kernels and model artifacts are practical integration boundaries |
| Google TPU | XLA-compiled tensor programs, e.g. JAX | Useful evidence for dense distance and selection design; requires a separate backend and deployment environment |
| Intel client NPU | OpenVINO compiled inference graphs | More plausible for bounded fixed-shape assignment/embedding inference than a direct transplant of Rust's dynamic indexing pipeline |
| Apple Neural Engine | Core ML model execution with runtime placement | Separate from Lance's MPS GPU path; graph/operator placement must be verified |
| CUDA GPU | Mature general compute and cuVS/Faiss ecosystem | Useful comparison baseline and architectural precedent, although not an NPU |
| CPU SIMD/AMX | In-process vector/matrix kernels | Essential baseline; avoids transfers and often wins on small or irregular jobs |

Google documents matrix-dominated workloads as the natural TPU fit. OpenVINO's current NPU documentation describes bounded dynamic-shape support as preview with restricted model coverage. Apple exposes per-operation compute placement and can choose a CPU even when an operation is eligible for the Neural Engine. These are reasons to test the complete operator graph rather than assume uniform portability. [Cloud TPU][S11] [OpenVINO NPU][S12] [Core ML placement][S13]

For Ascend procurement or allocation, record the exact board/SoC, HBM capacity, host RAM/NUMA placement, host-device link, supported precision, driver/firmware, and software tuple. Do not extrapolate a result from a server NPU to an edge device or to all Ascend generations.

## 5. Ascend: execution model and integration choices

### 5.1 Map work to the appropriate engines

Current Ascend C documentation distinguishes Cube and Vector compute and describes separate-core execution on relevant architectures. Matrix multiplication belongs on Cube/AIC; elementwise work and reductions belong on Vector/AIV. Host CPU, device-side AI CPU where supported, and transfer engines have different roles. These details vary by product. [Ascend C decoupled matrix example][S14]

An appropriate proposed pipeline is:

```text
Lance host process
|-- pin snapshot, select source fragments, establish row-ID/model contract
|-- scan/decode bounded Arrow batches and filter vector eligibility
|-- prepare contiguous vector buffers; retain row identity on the host
|-- submit batches to a device worker
|   `-- Ascend
|       |-- AIC: tiled distances / supported matrix transforms
|       |-- AIV: norms, residuals, masks, argmin, code generation
|       `-- compact output: labels, distances when needed, codes, statistics
|-- restore source sequence and write standard Lance artifacts
`-- validate and publish through existing Lance commit machinery
```

Double buffering can overlap host preparation, transfer, compute, and output writing, but only with bounded queues and backpressure. Keep device waits outside Lance's pure-CPU worker pool; [Rust concurrency guidelines][L13] prohibit blocking waits in `spawn_cpu()`.

### 5.2 Route A: external TorchNPU prototype

TorchNPU exposes Ascend through PyTorch, including `npu` tensors and matrix operations. Released TorchNPU/PyTorch/CANN/Python combinations must follow its compatibility matrix. Graph compilation is a separate concern: TorchAir provides an Ascend backend for `torch.compile`; the local Lance decorators cannot simply be assumed to select it. [TorchNPU][S15] [TorchAir][S16]

Recommended progression:

1. Implement a standalone, bounded-batch distance/argmin experiment with TorchNPU eager execution.
2. Compare CPU and NPU assignments with identical supplied centroids, preprocessing, and input precision.
3. Train externally, export host FP32 centroid arrays, and use the existing Lance centroids interface.
4. Add PQ codebooks only after residual and metric semantics are verified.
5. Profile launch count, reductions, and data copies before trying graph compilation or custom kernels.

Illustrative **existing Lance handoff**, after an external trainer has produced validated arrays:

```python
# Preparation of centroids_fp32 is external and is not implemented by this report.
# Shape: (num_partitions, vector_dimension); dtype: numpy.float32.
# dataset is an opened LanceDataset; this operation creates an index.
dataset.create_index(
    "vector",
    index_type="IVF_PQ",
    metric="l2",
    num_partitions=centroids_fp32.shape[0],
    num_sub_vectors=96,  # Example for dimension 768; choose a valid divisor.
    ivf_centroids=centroids_fp32,
)
```

This accelerates the supplied coarse-model training only. Lance still performs remaining training/build work. Do not supply `accelerator="npu"` to this local API, and do not describe the example as accelerated physical clustering. Optional `pq_codebook` accepts the documented shape for the selected PQ settings. [Python creation and validation][L4]

### 5.3 Route B: Ascend Index SDK

The official repository describes a Faiss-style C++ retrieval framework on ARM and x86_64, with Ascend C/TBE operators. Its live README lists **26.2.0, released 2026-09-30**. This study pinned inspected SDK files to `6d12b194b5fdad55a50ae56d01a5514e75f1b2ad` (2026-10-29), because indexed web copies still showed 26.1.0. [SDK overview][S17]

There is a real clustering entry point: `AscendIndexIVFConfig.useKmeansPP` enables NPU IVF clustering; its documented default is false. Creating an Ascend index is therefore not evidence that training itself ran on an NPU. [IVF configuration][S18]

The packaged interfaces have material constraints:

| Inspected component | Documented boundary | Consequence |
| --- | --- | --- |
| `AscendIndexIVFPQ` | Dimension 128, L2, 8-bit PQ, specific `nlist` and subspace choices, top-k at most 320 | Not a general drop-in for 768/1536-dimensional Lance workloads |
| 26.2.0 release notes | IVF-PQ support on Atlas A3 inference and Atlas 950PR products | Verify the exact product; generic Ascend support is insufficient |
| `AscendIndexIVFRaBitQ` | Constructor constraint says dimension 128; metric overview and constructor wording differ | Treat broader dimension/metric support as unverified until tested |
| SDK installation guidance | Multiple Faiss library variants; RaBitQ requires the appropriate variant | Pin the correct headers/library/ABI together |

Sources: [IVF-PQ API][S19], [release notes][S20], [RaBitQ API][S21], [installation guide][S22].

The inspected installation guide requests CAN 9.1.0, while the 26.2.0 release notes list CAN 9.2.0 in their product tuple. Select a validated supported combination for the actual package; do not combine independent “latest” downloads. The broad results in a research paper also do not establish identical coverage in every packaged API.

For Lance, use the SDK as a computational implementation or separate experimental comparison engine. Its Faiss-style index files are not Lance index files. Persisting them under `_indices/` would not make Lance able to read them. A production adapter must translate outputs into compatible centroids/codebooks/labels/codes and let Lance finalize, or introduce a separately reviewed index implementation.

### 5.4 Route C: a focused CANN/Ascend C backend

If TorchNPU proves the workload and the packaged SDK cannot support the required dimensions, implement a small backend for the measured primitives. Candidate operations are batched assignment, batched subspace encoding, and supported rotation transforms. Avoid designing a universal device abstraction before the first result.

The boundary should own device contexts, streams, workspaces, asynchronous completion, error translation, and capability validation. Keep validation and durable semantics in Rust, use optional dependencies/features, and retain CPU execution for unsupported requests. Validate dimensions and metric explicitly; never silently reduce dimension, substitute a metric, or quantize user inputs merely to satisfy a kernel.

Use model and snapshot identities in any temporary artifacts. Include centroid bytes/order, normalization, codebook/rotation parameters, and requested arithmetic mode. An integer partition ID has meaning only within its model. Hardware placement should not become part of a stable on-disk format unless it is semantically necessary.

### 5.5 Existing Lance ecosystem precedent

The current [`lance-cuvs` README][S23] describes a narrow external backend: train IVF_PQ, build a partition-local artifact, and hand outputs to Lance for finalization. It requires a particular Lance vector-build API baseline (`6112a34bfe38618f07c099217dc3d89fd39ca6bb` or a compatible descendant), CUDA 12, and cuVS 26.2.0. It does not register the index itself.

That separation is a useful architectural model for Ascend. It is **not** proof that the same artifact APIs exist in this v14 checkout or that cuVS output can be consumed by an NPU. Check the destination branch before adopting the interface. Older CAGRA proposal text is not evidence of the current package's delivered scope.

## 6. Published performance evidence

These examples show that the underlying operations can benefit. They do not predict Lance speedups. Each baseline and quality condition belongs to its own experiment.

### 6.1 Ascend-RaBitQ, June 2026 preprint

Reported build times versus CPU 1-bit IVF-RaBitQ:

| Dataset | CPU | Ascend | Reported ratio |
| --- | --- | --- | --- |
| SIFT1M | 1.65 min | 6.87 s | 14.42× |
| Cohere 10M | 1.55 h | 1.48 min | 62.84× |
| SIFT100M | 4.64 h | 1.53 h | 3.03× |
| SIFT1B | 98.63 h | 4.20 h | 23.49× |

Default: one 64-GB NPU; 192-core/1.5-TB host. Search gains versus the stronger CPU query-SQ baseline: 1.2× (SIFT1M) to 11.7× (SIFT1B). Scoring, selection, and reranking use NPU cores, device AI-CPU, and host CPU respectively. [Paper, §§3–4 and Table 1][S24]

**Assessment:** author-reported preprint; QPS is normalized. Match recall and baseline before comparing. No Lance/S3 measurement. Its 48-GB-index/64-GB-device capacity argument needs clarification.

### 6.2 GaussDB-Vector, VLDB 2025

Section 7.6 reports one-to-two orders of magnitude improvement for index-building tasks and IVF scans. Figure 15 specifically plots **build time** for SIFT IVF and GIST graph indices; it is not a graph-query throughput chart. The text says graph-search gains were not significant because cross-device transformation costs offset limited parallelism. The hardware section lists eight “Ascend-920B” NPUs on one machine; preserve the paper's wording rather than silently substituting another product name. [Paper, §§6, 7.1, 7.6 and Figure 15][S25]

**Assessment:** strong evidence for batching distances during training/graph construction, and a useful negative result for graph traversal. The figure does not justify a precise universal 100× claim. Detailed per-kernel timing and device utilization would be needed for a Lance capacity estimate.

### 6.3 NVIDIA cuVS IVF-Flat, October 2023

NVIDIA reports building a 100-million-vector index in under a minute, about 14× faster than its CPU comparison, and more than 20× search throughput at recall 0.95. The discussed search workload is DEEP100M on H100. Tensor-core k-means, hierarchical clustering, a suitable memory layout, and fused selection all contribute. The article does not fully identify the CPU configuration in the surrounding text. [Technical article, performance section][S26]

**Assessment:** an established GPU analogue for the recommended Ascend primitives. Algorithmic changes contribute alongside hardware; this is not a controlled “same Lance code, different device” result. Its age also makes a current optimized CPU rerun necessary.

### 6.4 TPU-KNN, June 2022

TPU-KNN evaluates dense nearest-neighbor search on TPU v3/v4 against Faiss GPU methods on V100/A100, using SIFT and GloVe. It reports stronger throughput in high-recall regions and explains why selection/elementwise instruction throughput can limit an accelerator despite abundant matrix FLOPs. Its method uses approximate selection and evaluates recall-speed curves. [Paper, §§4–6][S27]

**Assessment:** relevant non-Ascend NPU evidence. It supports co-designing selection with distances, not substituting an approximate selector into Lance's exact-assignment contract without evaluation. No numerical TPU-to-Lance multiplier is inferred.

### 6.5 How to use this evidence

The transferable techniques are dense batching, keeping reusable models resident, reducing score materialization, fusing simple work, and placing irregular tasks on the host. The transferable **speedup is unknown**. Distinguish kernel, training, complete index build, physical rewrite, and subsequent query performance in every result.

## 7. Physical clustering changes the system bottleneck

### 7.1 A valid vector-based rewrite

A proposed address-based workflow can reuse the local stable-partition protocol:

1. Pin the input snapshot and inspect every relevant physical segment/model.
2. Select source fragments and a destination policy; reserve destination IDs.
3. Scan live values in recorded source order, including supported overlays.
4. Send bounded vector batches to the NPU; retain source sequence and row identity on the host.
5. Restore ordered results if device tasks complete out of order; route rows stably into destinations.
6. Write replacement fragments and the immutable FRI label map.
7. Validate values, row counts, labels, coverage, and output ordering; publish the rewrite and FRI intent atomically through the existing Rust machinery.

Existing index payloads can be reused only when the mapping and coverage rules permit it. For arbitrary ordering inside a destination, use a mapping/index maintenance strategy that represents that permutation or rebuild affected indices. The accelerator does not change these requirements. [FRI implementation][L9] [Row-map implementation][L14]

```text
Input source order:       A(partition 1), B(0), C(1), D(0)
NPU assignment labels:    1,             0,    1,    0

Stable destination 0:    B, D
Stable destination 1:    A, C

FRI records source -> destination label; rank gives the destination offset.
One destination containing B,D,A,C is an arbitrary permutation of A,B,C,D.
```

A partition can span multiple output fragments. Destination labels are positions in a transition's destination list, not IVF partition IDs or fragment IDs. The current implementation accepts at most 65,535 destinations per stable-partition mapping, while the format/count utility permits 65,536. Avoid treating this discrepancy as an NPU limit. [Stable-partition mapping][L15]

### 7.2 Avoid the precomputed-map memory trap

The current assignment loader returns `HashMap<u64, u32>`. Producing its input as a stream does not make its consumption streaming. For a billion rows, raw IDs and labels alone occupy 12 GB; hash-table residency is larger. Python also expects a single-fragment/single-file dataset on the ordinary path and requires supplied centroids. Missing entries in a nonempty map cause eligible input rows to be filtered from the builder stream. [Loader][L10] [Builder][L7] [Python validation][L4]

For a small prototype, the map is useful. For large production work, prefer a versioned, validated streaming artifact boundary or attach labels/codes to the same bounded input stream. That is proposed integration work for this checkout.

The FRI row map instead stores nullable uint16 labels and cumulative counts. With `N` physical positions, `M` destinations, and block size `B`, its dense count buffer is `28 + 4 × ceil(N/B) × M` bytes; default `B` is 65,536. Many destinations can make even this compact mapping expensive. [Counts implementation][L16]

### 7.3 Multi-segment and stable-ID correctness

Partition 2 in segment A need not equal partition 2 in segment B. Use the correct model per source/segment or deliberately retrain a shared model. Do not use the first returned IVF model as a substitute for a complete segment inventory. Physical merge compatibility also includes quantizer state. [Distributed index plan][L2]

For stable-row-ID tables, preserve logical IDs and lineage, including hidden-column representations. Do not synthesize a physical address and call it a stable ID. The local tagged FRI path rejects this combination; upgrading requires an explicit version/capability review. Application IDs are ordinary columns and do not replace system row identity.

Null vectors and invalid vectors may be ineligible for a vector index while their complete application rows must remain in the table. Their placement in a physical rewrite must be explicit and deterministic. Deletions must be applied using the selected snapshot and maintained through the commit protocol.

### 7.4 Scalar liquid clustering

The proposed scalar path involves sampling/rank fitting, coordinate encoding, curve keys, and sorting. Most time may reside in spill, shuffle, and rewriting rather than arithmetic. An NPU experiment should isolate numeric key generation from the sort and measure both. Keep integer/float ordering, NaNs, signed zero, null coordinates, and deterministic key encoding consistent with the selected implementation.

Avoid encoding large integer keys through FP16/BF16 merely to use matrix engines. Values can collapse and the curve order can change. NPU-based learning of a clustering policy or automatic key selection would be separate research, not acceleration of the currently proposed algorithm.

### 7.5 Break-even for repeated queries

Physical clustering can reduce later base-column reads during refinement or payload retrieval. Measure that gain independently of faster preprocessing. A useful first approximation is:

```text
queries_to_amortize_rewrite = extra_rewrite_cost / saved_cost_per_query
```

Use consistent units, such as currency or time on a fixed resource allocation. Account for old versions retained alongside new objects, map/index maintenance, future updates, and reclustering frequency. A faster preprocessing device does not guarantee that the chosen layout pays back its write amplification.

## 8. End-to-end performance model

Let `p` be the fraction of original wall time that can actually be accelerated, `s` the speedup of that fraction, and `h` the **additional** transfer/setup/synchronization time as a fraction of original total time:

```text
new_time / old_time = (1 - p) + p/s + h
speedup            = 1 / ((1 - p) + p/s + h)
```

For an illustrative 20× accelerated stage and 5% added overhead:

| Accelerable share `p` | Total speedup |
| --- | --- |
| 30% | 1.31× |
| 70% | 2.60× |
| 90% | 5.13× |

At `p = 70%` and fixed `h = 5%`, even infinitely fast device computation caps the result at 2.86×. The [HTML guide](lance-clustering-npu-ascend-guide.html#model) lets readers change these assumptions. This serial model is a planning bound; overlapping stages and changing bottlenecks require measured pipeline analysis.

For a steady-state overlapping pipeline, throughput tends toward its slowest stage:

```text
batch time >= max(scan/decode, host preparation + transfer,
                 device computation, result consumption + write)
```

Also measure fill/drain, initialization, compilation, and final commit. For the 307.2-GB example, an **assumed** sustained 25 GB/s host-device transfer alone requires 12.29 seconds for a single upload, while an **assumed** sustained 100 TFLOP/s requires 6.29 seconds for the assignment multiply/add work. These are arithmetic lower bounds under hypothetical rates, not Ascend specifications or measured throughput. Object-store reads, host copies, reductions, and writes add further work.

High nominal INT8 TOPS cannot be substituted for measured FP32/FP16/BF16 kernel throughput. Precision, layout, batch shape, memory traffic, and selection determine the achievable rate.

## 9. Numerical and semantic requirements

An approximate index may tolerate some centroid/assignment differences while a metadata claim or row mapping must be exact. Establish which equality is required before optimizing.

| Risk | Required evaluation |
| --- | --- |
| FP16 overflow or BF16 rounding | Test representative magnitudes and near ties; retain suitable accumulation precision; reject unsupported modes |
| L2 cancellation in `x² + c² − 2xc` | Compare with a direct-difference reference; define handling of small roundoff errors and test rankings |
| Ties and non-deterministic reductions | Define tie policy; measure assignment disagreement and clustering objective across seeds |
| Cosine zero vectors, NaNs, infinities | Match Lance's metric preprocessing and eligibility rules; preserve base rows |
| PQ residual or codebook mismatch | Match subspace order, centroid order, bit width, residual semantics, and serialized code layout |
| RaBitQ model mismatch | Preserve rotation, normalization, scale/error metadata, bit order, and estimator semantics |
| Integer ID corruption | Keep uint64 IDs out of lossy floating-point tensors; use typed host-side IDs |
| Invalid or incomplete assignment artifacts | Validate row domain, snapshot, coverage, range, uniqueness, and model identity before publication |
| Model/segment mixing | Reject incompatible merges; do not infer global meaning from a local partition ordinal |
| Asynchronous completion reorders batches | Restore source order before writing a stable-partition map and its destination rows |

Use an FP32 reference initially, then evaluate an explicitly selected mixed-precision mode. A hardware backend may keep the same file format while producing a different trained model; those are separate compatibility questions. New work should target current writers and honor stable format contracts, leaving legacy compatibility paths frozen.

## 10. Recommended implementation sequence

### Phase 0 — establish the baseline

Choose the target Lance commit and Ascend device/software tuple. Decide whether this is a v14-compatible external experiment or work on the newer upstream artifact/metadata APIs. Instrument the actual CPU build or rewrite, identify stage times, and collect host-memory and I/O totals. Record baseline recall and clustering objective.

**Deliverable:** reproducible workload manifest and profile, with evidence that dense assignment/training consumes enough time to justify offload.

### Phase 1 — fixed-model assignment

Use one centroid model and stream representative vector batches through TorchNPU. Measure distance/argmin separately and together with transfers and synchronization. Test dimension 128 plus the real production dimensions. Keep row IDs on the host and return labels in input order.

**Deliverable:** assignment correctness/quality report, batch-size crossover curve, and bounded-memory behavior. Continue only if total assignment time improves at an acceptable precision/quality setting.

### Phase 2 — external training and standard Lance build

Add centroid updates, empty-cluster handling, deterministic initialization options, and convergence accounting. Feed trained centroids to the existing Lance interface. Add PQ training/encoding if profiling justifies it. Compare against Lance's actual hierarchical/coreset strategy, not only flat Lloyd.

**Deliverable:** standard Lance index files readable on a CPU-only installation, with matched-recall query evaluation and complete index-build timing.

### Phase 3 — stable physical routing

Integrate labels with the supported address-based FRI rewrite path or with a validated rebuild strategy. Keep ordering restoration and storage publication in Lance/Rust. Use multiple fragments, multiple logical indices, deletions, nulls, and segment-model variations in validation.

**Deliverable:** successful atomic rewrite preserving row values and correct indexed answers, plus measured cold/warm read locality and total write amplification.

### Phase 4 — production backend and scaling

Replace only measured bottlenecks with Ascend C or SDK primitives. Maintain long-lived worker contexts, bounded queues, cancellation/error handling, and resource accounting. For multi-device training, reduce sums/counts and broadcast the updated model; for assignment, shard inputs and reuse a consistent model. Scheduling retries must not publish duplicate segments or mismatched artifacts.

**Deliverable:** throughput/cost/quality comparison including orchestration and failure recovery. Existing single-node Python Torch restrictions require deliberate integration changes; adding workers does not remove them.

### Phase 5 — query acceleration if justified

Experiment with resident index partitions and query microbatches. Include queueing delay, p95/p99 latency, filters, deletions, uncovered-fragment fallback, host refinement, and cold loads. Optimize graph traversal only after the dense build/search cases produce useful results.

## 11. Benchmark and validation plan

### Workload matrix

Run a small qualification set first, then only expand where results justify it. Large cases belong in benchmarks, not subsecond unit tests.

| Axis | Suggested values / cases |
| --- | --- |
| Corpus | Real production sample plus SIFT128 and representative 768/1536-dimensional embeddings |
| Scale | Qualification at 1M; selected 10M/100M cases; 1B only with a bounded-memory design |
| Centroids | 256, 1024, 4096; larger counts only where useful and supported |
| Batch | 1, 32, 256, 2048, 8192 where relevant; measure device-memory limit |
| Metric/precision | L2 first; cosine/dot separately; FP32 reference and selected mixed precision |
| Distribution | Representative clusters, skew, duplicates, near ties, outliers |
| Layout | Multiple fragments, multiple physical segments, compatible and independent models |
| Storage | Local storage and object storage; warm/cold reads; vector-only and payload-heavy |
| Eligibility | Null vectors/items, zero vectors, NaNs/infinities, deleted rows, empty input |
| Index/state | Existing scalar/vector indices, uncovered data, supported row-ID mode, concurrent writes |

### Record these measurements

- Assignment vectors/s and training iterations/s; distinguish cold compilation from steady state.
- Complete index build and complete rewrite wall time, including model sampling, transfers, shuffle, encoding, upload, and commit.
- CPU utilization, device utilization, peak host RAM, peak HBM, workspace, scratch bytes, and spilled bytes.
- Host-device bytes, storage bytes read/written, range-request counts, and retained storage.
- Assignment disagreement against fixed-centroid reference, weighted objective/inertia where appropriate, cluster-size distribution, empty clusters, and training seed variance.
- Recall@10/@100 at matched query settings and across recall-speed curves; include p50/p95/p99 latency and throughput at stated concurrency.
- Row-value/ID/lineage preservation, index coverage, filter correctness, and snapshot/transaction behavior.
- Currency per completed build/rewrite or per million queries, using actual allocated resources. Add energy only when measured from a defensible device/system source.

Suggested experimental acceptance targets are **at least 2× complete build/rewrite speedup** for the selected use case, no unacceptable matched-recall regression, and bounded memory. These are project decision criteria, not measured results or guarantees. A smaller gain can still be useful if it reduces cost or frees CPU capacity; record that tradeoff explicitly.

For correctness, compare indexed results with scan references across multiple fragments and after rewrites. Include historical fixtures for stable-format compatibility. Keep the project's vector-test recall floor of at least 0.5, but set a substantially higher workload-specific target for a performance comparison where the product requires it. Do not lower quality to manufacture a speedup.

When implementing in this repository, follow the language-specific environment guides. Rust benchmarks use `release-with-debug`; Python tooling uses the prescribed `uv` workflow. Required checks apply to implementation changes. This report itself does not modify the Rust/Python/Java implementation or execute accelerator code.

## 12. Decision and unresolved questions

**Proceed with a bounded assignment/training feasibility experiment if server Ascend hardware is available and profiling shows a meaningful dense-compute fraction.** Use external model artifacts to prove value before coupling the backend to physical clustering or query serving.

Choose a custom tensor/kernel path when production dimensions exceed the verified packaged SDK contract. Choose the SDK directly only when its validated device, dimension, metric, and index settings match the workload. For scalar liquid clustering, prioritize a good planner, ordering semantics, zone-map effectiveness, and I/O efficiency before NPU work.

Open questions that actual experiments must resolve:

1. Which Ascend product and validated software tuple are available, and which required operators run efficiently there?
2. What fraction of this workload is assignment/training versus scan, shuffle, rewrite, and index maintenance?
3. Is the desired outcome faster vector index builds, physical vector locality, or scalar predicate skipping?
4. Which row-ID mode and existing indices must survive physical reorganization?
5. Does a newer Lance artifact boundary provide a cleaner integration than the v14 handoff?
6. How much numerical deviation is acceptable at the required recall, and can the CPU baseline use the same precision?
7. Does the layout's query benefit amortize its rewrite/retention cost under the real update rate?

No public evidence inspected establishes a native Ascend backend in this local Lance version, a generic production clustering API here, or a measured Lance end-to-end NPU speedup. The available code and external results establish a credible engineering opportunity with a testable first step.

## 13. Source register

All external sources were accessed on 2026-10-30. Local links resolve relative to this report. Upstream states were verified through `https://api.github.com/repos/lance-format/lance/pulls/<number>` or the corresponding issue/discussion endpoint; PR prose can describe prerequisites that have since merged. SDK links are pinned where possible. Papers and vendor benchmarks remain author-reported evidence.

### Local plans and code

| ID | Reference | Evidence used |
| --- | --- | --- |
| L1 | [Partitioning and S3 reorganization][L1] | Latest local clustering plan and rewrite boundaries |
| L2 | [Distributed vector index creation][L2] | Models, fragments, worker/coordinator responsibilities |
| L3 | [Vector index architecture][L3] | Index stages and tuning context |
| L4 | [Python dataset APIs][L4], especially `_create_index` and `create_index` | Device restrictions and model/assignment handoff |
| L5 | [Torch device selection][L5], [accelerated vector helpers][L17], [distance helpers][L18] | CUDA/MPS selection, one-pass training/assignment, compile/memory behavior |
| L6 | [Feature flags][L6] and [clustering format][L19] | Reserved versus supported capability |
| L7 | [Rust vector builder][L7] | Precomputed-map consumption, transforms, shuffle, finalization |
| L8 | [Rust k-means][L8] | Current CPU training and membership implementation |
| L9 | [Dataset FRI integration][L9] | Tagged rewrite/commit protocol and rejection boundaries |
| L10 | [IVF builder configuration and assignment loading][L10] | Training configuration and hash-map loading |
| L11 | [FP16 linalg kernels][L11] | Optimized CPU baseline |
| L12 | [Fast RaBitQ rotation][L12] | Butterfly/sign operations versus dense transforms |
| L13 | [Rust development guide][L13] | CPU pool constraints and benchmark environment |
| L14–16 | [FRI row maps][L14], [mapping implementation][L15], [counts utilities][L16] | Stable ordering, limits, map memory |

### External primary sources

| ID | Reference | Type |
| --- | --- | --- |
| S1–2 | [Partitioning discussion][S1]; [liquid clustering proposal][S2] | Upstream design discussions |
| S3–10 | [#9878][S3], [#10383][S4], [#10409][S5], [#10445][S6], [#10415][S7], [#10440][S8], [#10357][S9], [#10472][S10] | Live upstream development records |
| S11 | [Introduction to Cloud TPU][S11] | Official platform documentation |
| S12 | [OpenVINO NPU device][S12] | Official platform documentation |
| S13 | [Core ML performance and placement][S13] | Official platform documentation |
| S14 | [Ascend C decoupled matrix multiplication][S14] | Official operator documentation |
| S15–16 | [TorchNPU][S15]; [TorchAir][S16] | Official framework adapters |
| S17–22 | [SDK overview][S17], [IVF config][S18], [IVF-PQ][S19], [release notes][S20], [RaBitQ][S21], [installation][S22] | Official SDK source/docs, pinned snapshot |
| S23 | [lance-cuvs][S23] | Official Lance ecosystem implementation |
| S24 | [Ascend-RaBitQ, arXiv v2, 2026-06-15][S24] | Preprint; performance claims not independently reproduced |
| S25 | [GaussDB-Vector, PVLDB 18, 4951–4963, 2025][S25] | Published system paper; Figure 15 visually inspected |
| S26 | [cuVS IVF-Flat, 2023-10-02][S26] | Vendor-authored benchmark and implementation explanation |
| S27 | [TPU-KNN, arXiv v2, 2022-06-30][S27] | Research paper; accelerator/selection analysis |

[L1]: docs/lance-partitioning-and-s3-reorganization.md
[L2]: docs/05-distributed-vector-index-creation.md
[L3]: docs/02-vector-indexes.md
[L4]: ../python/python/lance/dataset.py
[L5]: ../python/python/lance/torch/__init__.py
[L6]: ../rust/lance-table/src/feature_flags.rs
[L7]: ../rust/lance/src/index/vector/builder.rs
[L8]: ../rust/lance-index/src/vector/kmeans.rs
[L9]: ../rust/lance/src/index/frag_reuse.rs
[L10]: ../rust/lance-index/src/vector/ivf/builder.rs
[L11]: ../rust/lance-linalg/src/distance/dot_f16.rs
[L12]: ../rust/lance-index/src/vector/bq/rotation.rs
[L13]: ../rust/AGENTS.md
[L14]: ../rust/lance-index/src/frag_reuse/row_map.rs
[L15]: ../rust/lance-index/src/frag_reuse/stable_partition.rs
[L16]: ../rust/lance-core/src/utils/stable_partition.rs
[L17]: ../python/python/lance/vector.py
[L18]: ../python/python/lance/torch/distance.py
[L19]: ../docs/src/format/table/clustering.md
[S1]: https://github.com/lance-format/lance/discussions/4125
[S2]: https://github.com/lance-format/lance/discussions/8921
[S3]: https://github.com/lance-format/lance/pull/9878
[S4]: https://github.com/lance-format/lance/pull/10383
[S5]: https://github.com/lance-format/lance/pull/10409
[S6]: https://github.com/lance-format/lance/pull/10445
[S7]: https://github.com/lance-format/lance/pull/10415
[S8]: https://github.com/lance-format/lance/pull/10440
[S9]: https://github.com/lance-format/lance/pull/10357
[S10]: https://github.com/lance-format/lance/issues/10472
[S11]: https://docs.cloud.google.com/tpu/docs/intro-to-tpu
[S12]: https://docs.openvino.ai/2026/openvino-workflow/running-inference/inference-devices-and-modes/npu-device.html
[S13]: https://developer.apple.com/documentation/coreml/analyzing-a-core-ml-model-s-performance-in-xcode
[S14]: https://www.hiascend.com/document/detail/en/CANNCommunityEdition/910/programug/Ascendcopdevg/docs/en/guide/operator_practice/simd_operator_impl/matrix_basic_api/decoupled_mode.md
[S15]: https://github.com/Ascend/pytorch
[S16]: https://github.com/Ascend/torchair
[S17]: https://github.com/Ascend/IndexSDK/blob/6d12b194b5fdad55a50ae56d01a5514e75f1b2ad/README_en.md
[S18]: https://github.com/Ascend/IndexSDK/blob/6d12b194b5fdad55a50ae56d01a5514e75f1b2ad/docs/en/api/02_approximate_retrieval/04_AscendIndexIVFConfig.md
[S19]: https://github.com/Ascend/IndexSDK/blob/6d12b194b5fdad55a50ae56d01a5514e75f1b2ad/docs/en/api/02_approximate_retrieval/15_AscendIndexIVFPQ.md
[S20]: https://github.com/Ascend/IndexSDK/blob/6d12b194b5fdad55a50ae56d01a5514e75f1b2ad/docs/en/release_notes_index.md
[S21]: https://github.com/Ascend/IndexSDK/blob/6d12b194b5fdad55a50ae56d01a5514e75f1b2ad/docs/en/api/02_approximate_retrieval/16_AscendIndexIVFRaBitQ.md
[S22]: https://github.com/Ascend/IndexSDK/blob/6d12b194b5fdad55a50ae56d01a5514e75f1b2ad/docs/en/04_installation_guide.md
[S23]: https://github.com/lance-format/lance-cuvs
[S24]: https://arxiv.org/html/2605.16007v2
[S25]: https://www.vldb.org/pvldb/vol18/p4951-sun.pdf
[S26]: https://developer.nvidia.com/blog/accelerated-vector-search-approximating-with-nvidia-cuvs-ivf-flat/
[S27]: https://arxiv.org/pdf/2206.14286v2
