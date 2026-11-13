# NPU acceleration without device coupling in Lance core

Status: **proposal**, 2026-11-13. No NPU performance or correctness result is claimed.

## Evidence and scope

Ray can schedule Huawei Ascend resources; Lance's inspected accelerator helpers use CUDA/MPS validation and device-specific assumptions. Ascend TorchNPU supplies a possible execution backend, subject to its matched runtime/hardware matrix. Sources and pins are collected in [07](07-ecosystem-evidence.md#npu-scheduling-exists-lance-acceleration-needs-qualification). Scheduling an NPU task and implementing a supported Lance NPU operation are separate deliverables.

There are also two different product outcomes:

1. **Generate embeddings on NPU, then store/search them with Lance.** This can be implemented as an external Ray pipeline without changing Lance index kernels.
2. **Accelerate Lance operations on NPU.** This needs operation contracts, numerical validation, data-transfer accounting and optional backend integration.

Record these separately in demonstrations and release notes.

## Choose operations by measured cost

| Candidate | Why evaluate it | Principal constraints | Priority |
| --- | --- | --- | --- |
| Embedding generation | Large tensor workloads; weak coupling to storage | Model support, output dimension/dtype, inference batching and backpressure | First end-to-end infrastructure experiment |
| IVF centroid assignment | Dense batched distance/matrix operations | Metric normalization, transfer, centroid residency and memory | First Lance numerical prototype |
| Sampled IVF training | Repeated assignment/reduction may benefit | Determinism, convergence, empty clusters, accumulation precision | After assignment contract is validated |
| PQ training / encoding | Repeated subvector operations | Residual construction, common codebook identity, dtype and packing | After shared-model correctness |
| Exact reranking | Naturally batchable and easy to compare to CPU | Query batching and host/device transfer can dominate small queries | Parallel experiment when query profile warrants |
| Graph traversal, small random reads, metadata commits | Irregular access/control-heavy operations | Device underutilization and transfer/dispatch costs | Keep CPU initially; revisit only with profiling evidence |

Do not add distributed k-means merely because Ray can distribute tasks. First establish whether sampling/training is the bottleneck. If distributed training is justified, use a shared algorithm contract with per-worker sufficient statistics and deterministic reduction rules; evaluate network/reduction costs. HCCL support and collective failure recovery need separate qualification. The earlier distributed-training PR is closed unmerged; see the watchlist in [07](07-ecosystem-evidence.md#current-collaboration-watchlist).

## Proposed execution path

~~~text
Pinned Lance scan
`-- bounded Arrow batch on host
    `-- validate shape / metric / null policy
        `-- convert or stage contiguous tensor (copy may be required)
            `-- transfer to visible NPU
                `-- assignment / training / scoring kernel
                    `-- synchronize completion + validate result
                        `-- host Arrow result / canonical model artifact
                            `-- existing Lance index writer and commit path
~~~

Arrow buffers, device tensors and persisted index files are different contracts. Neither Arrow nor Ray implies zero-copy transfer to Ascend. Measure conversion, pinned-buffer preparation, H2D/D2H copies, synchronization and serialization. Host buffers may not be released while asynchronous device work still references them.

A useful latency decomposition is:

~~~text
T_accelerated = T_read + T_convert + T_H2D + T_device
              + T_D2H + T_write + T_schedule
T_cpu         = T_read + T_cpu_compute + T_write + T_schedule
~~~

Overlap can change the critical path; report both stage totals and measured wall time. The HTML guide includes an explicitly illustrative calculator, not benchmark results.

## Start with an optional backend

Use TorchNPU in an external prototype to learn operation requirements. Do not merely accept `accelerator="npu"` in a string validator: memory estimation, synchronization, random generators, precision flags, tensor operations and error paths must all be exercised. CUDA-specific settings are not portable defaults.

After two working implementations establish the contract, propose the smallest useful batch-compute interface near the numerical owner. Keep Python responsible for external runtime adaptation, while Rust validates persisted model parameters, shapes and index invariants. A native AscendCL/CANN implementation can follow if profiling shows the Python boundary is material. Gate vendor dependencies behind optional features/packages; default CPU-only builds and installations must remain usable without the vendor SDK.

The conceptual selection policy is a single mode:

- `cpu`: CPU implementation only.
- `prefer_accelerator`: use an admitted backend; an explicitly documented unsupported-capability condition may choose CPU before work starts.
- `require_accelerator`: return a descriptive error if the backend/capability is unavailable.

These names are proposed configuration, not existing Lance parameters. Invalid dimensions, incompatible models, corruption and arbitrary device failures must not be silently converted into CPU success. If recovery retries a failed attempt on CPU, record a new attempt and its backend identity; never mix partial device output with another result.

## Numerical and artifact contract

For every operation specify the vector dimension, data type, metric, normalization, accumulation/output precision, treatment of zero vectors, nulls and NaNs, deterministic seed policy and allowed numerical tolerance. Unsupported inputs must produce a clear rejection or the same documented filtering behavior as the selected CPU operation.

For IVF-PQ, residual creation must match the metric and the chosen IVF model. Digest the full parameter/model contract, not just a filename or device type. All workers that will physically merge outputs need compatible model state. Preserve canonical Lance model/index serialization; do not persist opaque NPU runtime handles or vendor-only compiled graph blobs as required dataset state. A vendor cache can remain disposable and external.

Bitwise equality is not required for every floating-point operation; bounded distance error, consistent ordering where stable, and measured recall are. Training must pass convergence/quality tests and the same data visibility rules. Reopening the resulting index with a CPU-only reader is a mandatory experiment for the initial canonical-format backend.

## Qualification matrix and deployment

Pin exact Ascend model, host architecture, driver, firmware, Ascend compute runtime, TorchNPU, PyTorch, Python, Ray, Arrow and Lance versions. Record container image digest and kernel implementation revision. The vendor [TorchNPU compatibility matrix](https://github.com/Ascend/pytorch/blob/2abab4d362af564265203cb3170c0b0f1ad47770/COMPATIBILITY.en.md) is a starting point; it is not a test result for this combined stack.

Ray resource requests identify NPU capacity and establish device visibility. Resolve device numbering according to the selected Ray/runtime combination; do not assume a host physical device ID equals a worker's local device ordinal. Reserve CPU and host memory for each NPU worker as well. Begin with one device and bounded batches, then one device per worker, then multi-node execution. Avoid collective algorithms in the first milestone.

Report host RSS, peak device memory, CPU utilization, device utilization, batch sizes, transfer bytes/time, retry count, queue time, throughput, p50/p95 latency, energy if measured, and recall. Compare end-to-end cost at equal quality, not kernel speed alone. Detailed gates are in [12](12-validation-and-experiments.md).
