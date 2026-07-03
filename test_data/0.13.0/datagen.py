# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""
Generate a legacy-format IVF_HNSW_SQ index that current Lance can no longer
create in-tree.

pylance 0.13.0 is the newest release that still writes vector indices in the
legacy self-described format (index file version 0.2). Starting with 0.14.0 the
default switched to the v3 format (index file version 0.3), so an IVF_HNSW_SQ
index built by any newer release is read by a different loader. This fixture
keeps the legacy v1 IVF_HNSW_SQ read path covered; that loader expects both
`index.idx` and `auxiliary.idx` under `_indices/<uuid>/`.

To (re)generate this test data:
1. pip install pylance==0.13.0
2. python test_data/0.13.0/datagen.py
"""

import os
import shutil

import lance
import numpy as np
import pyarrow as pa

# To generate the test file, we should be running this version of lance.
assert lance.__version__ == "0.13.0"

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "legacy_hnsw_sq")
if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)

# Keep the fixture tiny: low dimension, a few hundred rows.
NDIM = 16
NROWS = 512

rng = np.random.default_rng(42)
vectors = rng.standard_normal((NROWS, NDIM)).astype(np.float32)

data = pa.table(
    {
        "id": pa.array(range(NROWS), pa.int32()),
        "vec": pa.FixedSizeListArray.from_arrays(pa.array(vectors.reshape(-1)), NDIM),
    }
)

# `max_rows_per_file` forces at least two fragments.
dataset = lance.write_dataset(data, OUTPUT_DIR, max_rows_per_file=256)
assert len(dataset.get_fragments()) >= 2

dataset.create_index(
    "vec",
    "IVF_HNSW_SQ",
    num_partitions=2,
)

indices = dataset.list_indices()
assert len(indices) == 1
uuid = indices[0]["uuid"]

# The legacy v1 IVF_HNSW_SQ loader reads both the index and the auxiliary file.
index_dir = os.path.join(OUTPUT_DIR, "_indices", uuid)
for name in ("index.idx", "auxiliary.idx"):
    assert os.path.exists(os.path.join(index_dir, name)), name

stats = dataset.stats.index_stats("vec_idx")
assert stats["indices"][0]["index_type"] == "IVF"
assert stats["indices"][0]["sub_index"]["index_type"] == "HNSW"

# Sanity check that the index answers a query.
results = dataset.to_table(nearest={"column": "vec", "q": vectors[0], "k": 5})
assert results.num_rows == 5

print("Created legacy IVF_HNSW_SQ fixture at", OUTPUT_DIR)
