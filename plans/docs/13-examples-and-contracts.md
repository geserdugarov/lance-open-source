# Examples and proposed execution records

Date: 2026-11-13. Examples distinguish existing APIs from design sketches. API examples reflect source signatures; they are not qualified integration recipes. Use the repository's prescribed environments and the pinned dependency combinations in [07](07-ecosystem-evidence.md) before running them.

## A. Existing Lance API: a two-worker write protocol, locally

This four-row demonstration executes the two worker bodies sequentially so it needs no cluster. In a real deployment the worker calls execute on different hosts against shared storage; only their metadata is returned to the publisher. The temporary dataset is deleted when the example exits.

~~~python
from pathlib import Path
from tempfile import TemporaryDirectory

import lance
import pyarrow as pa
from lance.fragment import write_fragments

schema = pa.schema([("id", pa.int64()), ("text", pa.string())])
with TemporaryDirectory() as scratch:
    uri = str(Path(scratch) / "example.lance")
    worker_inputs = [
        pa.table({"id": [1, 2], "text": ["one", "two"]}, schema=schema),
        pa.table({"id": [3, 4], "text": ["three", "four"]}, schema=schema),
    ]
    fragments = []
    for batch in worker_inputs:
        fragments.extend(write_fragments(batch, uri, schema=schema))

    operation = lance.LanceOperation.Overwrite(schema, fragments)
    lance.LanceDataset.commit(uri, operation, read_version=0)
    dataset = lance.dataset(uri)
    assert dataset.version == 1
    assert len(dataset.get_fragments()) == 2
    assert dataset.count_rows() == 4
    assert sorted(dataset.to_table(columns=["id"])["id"].to_pylist()) == [1, 2, 3, 4]
~~~

`Overwrite` is appropriate here because the temporary dataset is new. For an existing dataset, use the intended operation and its source-version contract; do not reuse this example as an overwrite-based append retry. Worker retry/deduplication and commit-timeout recovery are deliberately outside this minimal demonstration. Source: [write_fragments](../../python/python/lance/fragment.py) and [distributed write guide](../../docs/src/guide/distributed_write.md).

## B. Published Lance-Ray API: search one pinned snapshot

Prerequisites: a running Ray cluster, shared access to a dataset containing `id` and a 768-dimensional float vector column named `vector`, and a qualified matching installation of Lance-Ray/Lance on driver and workers. `LANCE_DATASET_URI` and `LANCE_DATASET_VERSION` are example application environment variables, not built-in Lance configuration.

~~~python
import os

import lance
import lance_ray as lr
import numpy as np
import ray

ray.init(address="auto")
snapshot = lance.dataset(
    os.environ["LANCE_DATASET_URI"],
    version=int(os.environ["LANCE_DATASET_VERSION"]),
)
result = lr.vector_search(
    uri=snapshot,
    nearest={"column": "vector", "q": np.full(768, 0.01, dtype=np.float32), "k": 10},
    columns=["id"],
    num_workers=2,
    include_unindexed=True,
)
assert result.num_rows <= 10
assert "_distance" in result.column_names
~~~

This illustrates an existing external API, not a new core method. The row-count assertion is an example sanity check, **not** a sufficient ANN test; a real test must assert recall against exact ground truth, handle filters/deletes and check worker ownership. The fallback can materialize task data in the inspected implementation. See the pinned [release search source](https://github.com/lance-format/lance-ray/blob/93457c7ff3895e9068b6748c03e63bac271d20fe/lance_ray/search.py) and [integration design](09-sql-and-ray-integration.md).

## C. Existing pglance SQL shape

After building/installing the pinned extension in a disposable compatible PostgreSQL instance, the following SQL shows its existing import workflow. Replace the dataset path with one the PostgreSQL server can read. Current-Lance compatibility remains a phase-0 experiment.

~~~sql
CREATE EXTENSION lance;
CREATE SERVER lance_demo FOREIGN DATA WRAPPER lance_fdw;
SELECT lance_import('lance_demo', 'public', 'demo_vectors', '/srv/data/demo.lance');
SELECT count(*) FROM public.demo_vectors;
~~~

The PostgreSQL extension is called `lance` even though its repository/crate is `pglance`. This example creates database objects. It demonstrates no distributed writes, top-k pushdown or cross-system transaction support. Source: [pglance README at the inspected commit](https://github.com/lance-format/pglance/blob/6dece2f725624f3496816135d08b9b4eec63b219/README.md).

## D. Proposed job envelope

The following is an **illustrative design record**, not a supported Lance wire format or a finalized JSON Schema. Values and capabilities are examples. Existing native plans are carried as immutable payloads rather than manually re-created in this envelope.

~~~json
{
  "protocol": "lance-execution-draft/1",
  "operation_id": "example-build-0042",
  "operation": "index-build",
  "coordinator_epoch": 7,
  "source": {
    "table_id": ["research", "vectors"],
    "incarnation": "example-table-generation-a",
    "branch": "main",
    "version": 42,
    "schema_digest": "example-sha256-schema"
  },
  "required_capabilities": ["pinned-snapshot", "segment-build", "result-provenance"],
  "native_plan": {
    "producer_revision": "042ccc8376f1ba03ab198a5cbfdd7f243853f954",
    "artifact_uri": "s3://example-plans/build-0042/plan.json",
    "digest": "example-sha256-plan"
  },
  "models": [{"role": "ivf", "digest": "example-sha256-model"}],
  "task": {"id": "group-0", "attempt": 2, "fragment_ids": [0, 1]},
  "budget": {"host_bytes": 1073741824, "inflight_batches": 2},
  "state": "RUNNING"
}
~~~

A production schema must define field types, digest algorithms/canonicalization, bounds, required/optional fields, unknown-feature handling, and validation errors. Object locations are immutable/content-verified; access uses separately provisioned scoped credentials. Table identity and incarnation must come from the selected catalog/authority, not a locally invented label. A coordinator epoch is effective only when both record mutation and publication enforce fencing.

A result descriptor binds `operation_id`, source identity, plan digest, task ID, attempt, output artifact IDs/digests, actual fragment coverage, model parameters/digests and worker/backend revision. Large coverage sets should use a compact representation or immutable artifact reference, with exact validation at the barrier. Do not duplicate serialized Lance metadata by hand.

## E. Proposed coordinator algorithm

This is pseudocode; the capitalized operations below are required behaviors, not existing library functions.

~~~text
Persist plan; tag the source snapshot before dispatch.
Require build plus recovery within the seven-day artifact grace period
    with delete_unverified=false, or suspend cleanup for the job.
Admit workers only after required capabilities match.
For every task:
    execute against the pinned source; use a fresh output identity per attempt;
    persist successful output descriptors;
    select exactly one attempt through a fenced coordinator record.
At the barrier:
    verify task completeness and intended coverage;
    verify source/schema/field/model identities and actual output metadata;
    persist the selected publication intent, including Transaction.uuid for append.
Publish using the table's authoritative Lance commit path.
If the outcome is ambiguous:
    reconcile authoritative publication and selected artifacts;
    retry only when the protocol proves publication did not occur.
Record the authoritative committed version and terminal outcome.
Release retention only after workers are quiescent and recovery is complete.
~~~

Different operations need different reconciliation keys. Index publication can inspect segment identities. For append, first evaluate the existing `Transaction.uuid` and `read_transaction` APIs. [TransactionBuilder](../../rust/lance-table/src/transaction/builder.rs) accepts a caller UUID or generates one; the [commit retry loop](../../rust/lance/src/io/commit.rs) clones the transaction, and the [conflict resolver](../../rust/lance/src/io/commit/conflict_resolver.rs) returns the append transaction with that UUID intact. This establishes UUID preservation through internal retries by code inspection. Python [LanceDataset.commit](../../python/python/lance/dataset.py) accepts a `Transaction`, and `read_transaction(version)` exposes committed transaction identity; Rust [Dataset::read_transaction](../../rust/lance/src/dataset.rs) reads the opened version's transaction.

Persist the transaction before submission and reconcile its UUID against all candidate committed versions, since later commits can hide it from a latest-only check. Exercise conflict retries, lost acknowledgement and coordinator restart before relying on this protocol. Retain the required manifest/transaction history throughout recovery; missing or cleaned history cannot prove non-publication. A newly constructed transaction normally gets a new UUID, and reusing a UUID alone does not establish automatic duplicate suppression on resubmission. An external ledger update and a Lance commit are not atomic unless an explicitly implemented shared protocol makes them so; add another deduplication mechanism only for gaps demonstrated by these experiments.

## F. NPU scheduling probe, not a Lance kernel

This probe uses Ray's documented resource name. It requires a configured cluster advertising NPU resources; it only checks scheduling visibility and performs no numerical work.

~~~python
import os
import ray

ray.init(address="auto")

@ray.remote(resources={"NPU": 1}, num_cpus=1)
def visible_npu_devices():
    devices = os.environ.get("ASCEND_RT_VISIBLE_DEVICES")
    if not devices:
        raise RuntimeError("Ray worker has no configured Ascend device visibility")
    return devices

assigned_devices = ray.get(visible_npu_devices.remote())
assert assigned_devices
~~~

A passing result does not establish TorchNPU readiness, supported kernels, numerical correctness or Lance acceleration. Follow the staged qualification in [10](10-npu-acceleration.md). Source: [Ray accelerator support](https://docs.ray.io/en/latest/ray-core/scheduling/accelerators.html).
