# Issue #11 — Memory consumption during IVF train prefetch (nullable columns)

## TL;DR

The claim in the issue holds: when a vector column is `nullable`, IVF
training reads up to **2×** the requested sample size per prefetch round,
while the non-nullable path reads exactly the requested size. The
amplification is explicit in
`rust/lance/src/index/vector/utils.rs::sample_training_data_scan_from_fragments`
(`let target = sample_size_hint.saturating_mul(2)`), and is the dominant
peak-memory term for IVF training on nullable + fragment-limited inputs.
The over-fetch is also propagated into the consumer's output buffer:
`sample_nullable_fsl` only checks `num_non_null < sample_size_hint`
*before* reading the next batch and then appends the whole filtered
batch, so on low-null data the output `MutableBuffer` itself grows to
roughly `2 × sample_size_hint × byte_width` before the post-loop
truncate. A second, smaller amplification lives in
`sample_nullable_fallback` (used for multi-vector / `List<FSL>`
columns whenever they go through the streaming scan — i.e. any
no-fragment-filter multivector, or nullable multivector with a
fragment filter; non-nullable multivector + fragment filter short-circuits
through `dataset.sample` before this code path), where every
prefetched batch is retained in a `Vec<RecordBatch>` and then
materialised into one combined batch via `concat_batches`, doubling
peak memory at the moment of concat.

The dataset-wide nullable path (no `fragment_ids`) does **not** carry
the 2× factor — it streams small range chunks through
`take_scan(buffered(io_parallelism))` and is bounded by
`block_size × io_parallelism`, a few MB at most. So this report is
specifically about:

1. **`sample_training_data_scan_from_fragments`** — explicit 2×
   over-prefetch + a dense-path fallback that materialises a
   `Vec<u64>` of every *unseen* row index (allocation grows with
   `remaining × 8` bytes, not `num_rows × 8`, though the producer still
   iterates the full `0..num_rows` range).
2. **`sample_nullable_fsl`** — unbounded append of the over-fetched
   batch into the output buffer, so the buffer regrows past its
   pre-allocated `sample_size_hint × byte_width` capacity.
3. **`sample_nullable_fallback`** — `Vec<RecordBatch>` + `concat_batches`
   spike for multivector columns.

---

## 1. Where the prefetch happens

IVF training enters `maybe_sample_training_data` in
`rust/lance/src/index/vector/utils.rs` either directly (`builder.rs:455`)
or through `sample_ivf_training_chunk` in
`rust/lance/src/index/vector/ivf.rs:2480` (called from the streaming
coreset / refinement loops at lines 3055, 3818, 4006).

`maybe_sample_training_data` reads `vector_field.nullable` once
(`utils.rs:331`) and uses it to pick a sampling strategy. The relevant
branches in `sample_training_data` (`utils.rs:463`) are:

| Nullable | Vector type            | Fragment filter | Path                                                |
|----------|------------------------|-----------------|-----------------------------------------------------|
| no       | `FSL`                  | none            | `sample_fsl_uniform`                                |
| no       | `FSL`                  | yes             | `dataset.sample(sample_size_hint, …)`               |
| yes      | `FSL`                  | none            | `sample_training_data_scan` → `sample_nullable_fsl` |
| yes      | `FSL`                  | yes             | `sample_training_data_scan_from_fragments` → `sample_nullable_fsl` |
| no       | `List<FSL>` (multivec) | yes             | `dataset.sample(sample_size_hint, …)`               |
| no       | `List<FSL>` (multivec) | none            | `sample_training_data_scan` → `sample_nullable_fallback` |
| yes      | `List<FSL>` (multivec) | yes             | `sample_training_data_scan_from_fragments` → `sample_nullable_fallback` |
| yes      | `List<FSL>` (multivec) | none            | `sample_training_data_scan` → `sample_nullable_fallback` |

Note the early-return branch at `utils.rs:477-488`: when `fragment_ids`
is `Some` and the column is non-nullable, the code goes through
`dataset.sample` **before** the vector-type dispatch — so a
non-nullable multivector with a fragment filter never reaches
`sample_nullable_fallback`.

Concurrently, `ivf.rs:3747` instantiates the optimised
`FixedIvfTrainingSampler` only when the column is **non-nullable** FSL
and `fragment_ids.is_none()` (see `FixedIvfTrainingSampler::try_new`,
`ivf.rs:2768`); nullable columns therefore always fall back to the
general `maybe_sample_training_data` helper above.

## 2. The 2× over-prefetch (root cause)

`sample_training_data_scan_from_fragments` builds an unfold stream that
yields one batch per round (`utils.rs:587`). The relevant lines are
`utils.rs:600-619`:

```rust
let remaining = num_rows.saturating_sub(seen_offsets.len());
let target = sample_size_hint.saturating_mul(2).min(remaining);  // <-- 2×
let mut sampled_offsets = if remaining <= target.saturating_mul(4) {
    let mut unseen_indices = (0..num_rows as u64)              //   dense path:
        .filter(|index| !seen_offsets.contains(index))         //   materialises every
        .collect::<Vec<_>>();                                  //   unseen row index
    unseen_indices.shuffle(&mut rng);
    unseen_indices.truncate(target);
    seen_offsets.extend(unseen_indices.iter().copied());
    unseen_indices
} else {
    let mut sampled_offsets = Vec::with_capacity(target);
    while sampled_offsets.len() < target {
        let index = rng.random_range(0..num_rows as u64);
        if seen_offsets.insert(index) {
            sampled_offsets.push(index);
        }
    }
    sampled_offsets
};
…
let batch = TakeBuilder::try_new_from_addresses(…)?.execute().await?;
Ok(Some((batch, …)))
```

Each yielded batch therefore contains **`target = 2 × sample_size_hint`
rows**, regardless of how many of those rows turn out to be non-null.

The consumer is `sample_nullable_fsl` (`utils.rs:718`). It only
*pre-sizes* the output buffer to `sample_size_hint × byte_width` —
that capacity is a hint, not a cap. The loop reads one batch at a
time and appends **the full filtered batch** via `accumulate_fsl_values`
(`utils.rs:753, 824`); the loop condition `num_non_null < sample_size_hint`
is checked **before** the next read, but never bounds how much of the
current batch is appended. Truncation to `num_rows_out * byte_width`
happens only after the loop exits (`utils.rs:766`). So with a
`2 × sample_size_hint` upstream batch and few or no nulls, the output
`MutableBuffer` grows past its initial capacity and reaches roughly
`2 × sample_size_hint × byte_width` before the post-loop truncate.
Peak memory is therefore approximately:

```
peak ≈ 2 × sample_size_hint × byte_width      (one in-flight batch)
     + 2 × sample_size_hint × byte_width      (output buffer, regrown via push)
     ≈ 4 × sample_size_hint × byte_width
```

For comparison, the non-nullable fragment-limited path (`utils.rs:480`)
calls `dataset.sample(sample_size_hint, &projection, …)` and stays at
roughly `1 × sample_size_hint × byte_width` plus the output FSL.

### Concrete number

For a typical training pass with `num_partitions = 65_536`,
`sample_rate = 256` (the default for large IVFs), and 1024-dim Float32
vectors (`byte_width = 4 KiB`):

- `sample_size_hint = 65_536 × 256 ≈ 16.7 M rows` → `≈ 64 GiB` of raw vectors.
- Non-nullable peak: ~64 GiB.
- Nullable peak: ~128 GiB for the in-flight batch plus ~128 GiB in the
  regrown output buffer when nulls are rare → **~256 GiB**.

(In practice, streaming coreset training uses `streaming_sample_rate`
which keeps each step smaller, but the per-step doubling still
reproduces — and the streaming refinement passes at `ivf.rs:3903-4006`
re-run sampling repeatedly.)

### Why the 2× exists today

`sample_nullable_fsl` stops as soon as it has accumulated
`sample_size_hint` non-null vectors. Over-prefetching by 2× was added so
that a single round with ≤50% nulls satisfies the sampler without a
second `TakeBuilder::execute` round-trip. The optimisation is real but
it has been encoded as a fixed `× 2` blow-up rather than as a bounded
streaming read, so the win on round-trip latency comes at the cost of
double peak memory regardless of the actual null rate (often `~0%` in
production).

## 3. Secondary amplifier — `sample_nullable_fallback`

For multivector columns (`List<FSL>`), the streaming-scan path lands in
`sample_nullable_fallback` (`utils.rs:860`). This covers:

- any multivector with no fragment filter (regardless of nullability),
  via `sample_training_data_scan`, and
- nullable multivector with a fragment filter, via
  `sample_training_data_scan_from_fragments`.

Non-nullable multivector with a fragment filter is the only multivector
case that bypasses this code — the `!is_nullable` branch at
`utils.rs:478-488` returns through `dataset.sample(...)` before the
type dispatch.

The fallback body:

```rust
let mut filtered = Vec::new();
while num_non_null < sample_size_hint {
    let Some(batch) = scan.next().await else { break; };
    let batch = batch?;
    …
    let batch = if is_nullable {
        filter_non_null_rows(array, batch)?
    } else {
        batch
    };
    num_non_null += batch.num_rows();
    filtered.push(batch);                            // <-- retain entire scan
}
…
let batch = arrow::compute::concat_batches(&schema, &filtered)?;  // <-- 2× spike
let num_rows_out = batch.num_rows().min(sample_size_hint);
let batch = batch.slice(0, num_rows_out);
```

At the `concat_batches` call, `filtered` and the new combined batch
co-exist for the duration of the copy. Then the combined batch is
sliced down to `sample_size_hint`; the truncated tail is held by the
batch's underlying buffer until the slice is dropped. This is another
≥2× transient spike, but on multivector data the absolute size is
already very large (`≈ vectors_per_row × dim × elem_size` per row), so
this amplifier is also worth fixing.

## 4. Tertiary amplifier — dense-path index materialisation

Inside `sample_training_data_scan_from_fragments`, the dense path
(`utils.rs:602-609`, triggered when `remaining ≤ 8 × sample_size_hint`)
collects every **unseen** row index into a `Vec<u64>`. The allocated
vector is therefore `remaining × 8` bytes (not `num_rows × 8`), but the
producer still iterates the full `0..num_rows` range and probes
`seen_offsets` for every index — so the CPU cost scales with
`num_rows` while the allocation scales with `remaining`. For very large
fragments this still allocates hundreds of MiB to gigabytes on top of
the existing batch buffers; it is not the dominant term but is the
same family of "allocate up front, throw away most of it" bug as the
2× prefetch.

## 5. Why the dataset-wide nullable path is **not** the OOM source

The no-fragment-filter nullable path goes through
`sample_training_data_scan` → `random_ranges` → `take_scan`
(`utils.rs:530-544`). `random_ranges` yields ranges of
`rows_per_batch = block_size / byte_width` rows each, and
`take_scan(buffered(batch_readahead))` keeps at most
`io_parallelism` reads in flight. With `DEFAULT_LOCAL_IO_PARALLELISM = 8`
(or `DEFAULT_CLOUD_IO_PARALLELISM = 64`) and `block_size` of 4 KiB / 64 KiB,
peak prefetch is on the order of a few MiB. So the fix below should
**not** change this path.

## 6. Fix plan (tasks)

The orchestrator should turn the items below into one focused PR.

### Task A — Cap the per-round prefetch in `sample_training_data_scan_from_fragments` and bound the consumer-side append

- **Files:** `rust/lance/src/index/vector/utils.rs`
- **Lines:** ~600-619 (producer) and ~733-774 (consumer
  `sample_nullable_fsl`).
- **Producer change:** Replace the hard-coded
  `sample_size_hint.saturating_mul(2)` with a bounded over-fetch.
  Concretely:
  - Track how many non-null vectors the consumer still needs (already
    available in `sample_nullable_fsl` as
    `sample_size_hint - num_non_null`); thread it through the stream
    (e.g. via a `BatchSizeHint` parameter on the unfold seed) so each
    round requests just `still_needed + small_buffer` rows rather than
    `2 × sample_size_hint`.
  - A reasonable choice: `target = (still_needed + still_needed.div_ceil(8)).min(remaining)`
    — small over-fetch (~12.5%) for the common low-null case, dropping
    to exactly what's needed on subsequent rounds. The cost of a second
    round-trip when null rate is genuinely high is small compared to
    OOMing the trainer.
  - Replace the dense `Vec<u64>` materialisation. Its allocation is
    `remaining × 8` bytes (every unseen index), and the producer
    additionally scans `0..num_rows` linearly per round. Always use the
    rejection-sampling branch when `remaining > 4 × target`; otherwise
    reservoir-sample lazily across `0..num_rows` without materialising
    the full unseen set (e.g. Algorithm L on the iterator). If retaining
    the dense branch is preferred, gate it on an absolute byte budget
    (e.g. `remaining × 8 ≤ 16 MiB`) rather than a multiplicative factor
    of `sample_size_hint`.
- **Consumer change (mandatory, addresses the output-buffer growth):**
  Inside `sample_nullable_fsl`, compute `remaining_rows =
  sample_size_hint.saturating_sub(num_non_null)` *before* calling
  `accumulate_fsl_values`, and pass that as a cap so the helper only
  appends up to `remaining_rows` filtered vectors per batch. The
  `MutableBuffer` then never exceeds its pre-allocated
  `sample_size_hint × byte_width` capacity, even if the upstream batch
  contains more usable rows. Concretely, either:
  1. extend `accumulate_fsl_values` with a `max_rows: usize` parameter
     and short-circuit the filter/copy at that limit, or
  2. slice the filtered FSL down to `remaining_rows` before the
     extend_from_slice call.
  Without this consumer-side cap, the producer fix alone still lets a
  single late-stage batch nearly double the output buffer.

### Task B — Stream-and-trim `sample_nullable_fallback`

- **File:** `rust/lance/src/index/vector/utils.rs`
- **Lines:** ~860-928
- **Scope:** This helper is only reached for multivector columns going
  through the streaming-scan path — i.e. multivector with no fragment
  filter, or nullable multivector with a fragment filter. Non-nullable
  multivector + fragment filter already short-circuits through
  `dataset.sample` and does not need a fix here.
- **Change:** Avoid keeping every batch in `Vec<RecordBatch>` followed by
  a single `concat_batches` over the full retained set. Two options:
  1. Slice each filtered batch to `sample_size_hint - num_non_null`
     before pushing, so `filtered` never holds more than
     `sample_size_hint` rows in aggregate. `concat_batches` then peaks
     at `~ sample_size_hint` rows instead of `~ 2 × sample_size_hint`.
  2. Better: build the output incrementally — pre-allocate a
     `RecordBatch` builder sized for `sample_size_hint` rows and append
     filtered batches into it directly, dropping the source batch each
     iteration (mirrors what `sample_nullable_fsl` already does for
     FSL).
- Keep the multivector dimension-estimation behaviour (estimated
  `vectors_per_row`) intact when sizing the output.

### Task C — Tests

- **Files:**
  - `rust/lance/src/index/vector/utils.rs` (new unit tests next to the
    existing `test_maybe_sample_training_data_fsl` cases).
- **What:**
  1. Add an `rstest` case to `test_maybe_sample_training_data_fsl`
     covering nullable FSL + fragment-limited sampling
     (`fragment_ids = Some(&[0])`) that asserts the returned FSL has
     exactly `sample_size_hint` non-null rows.
  2. Add a regression test that wraps `sample_training_data_scan_from_fragments`
     directly (via a small public-in-crate helper) and asserts that the
     batch yielded by `Stream::next` contains at most `target` rows,
     where `target` matches the new bounded formula. The current code
     would fail the new bound.
  3. Add a regression test for `sample_nullable_fsl` against a
     non-nullable upstream batch that is artificially `≥ 2 ×
     sample_size_hint` rows wide. Assert that the returned FSL has
     exactly `sample_size_hint` rows **and** that the buffer's
     capacity (`MutableBuffer::capacity`, observable via a small
     test-only seam or by re-deriving it from the produced FSL) is no
     more than ~`1.1 × sample_size_hint × byte_width`. This catches
     regressions where the consumer-side cap is dropped.
  4. Add a memory-cap-style test for `sample_nullable_fallback` (using
     a multivector column with 90% nulls and a small sample size) that
     asserts the function does **not** allocate a transient batch
     larger than ~`1.2 × sample_size_hint` rows. This is best expressed
     by intercepting `concat_batches` arguments through a test-only
     code path or by asserting on `filtered.iter().map(|b| b.num_rows()).sum()`.

### Task D — Optional follow-up: extend `FixedIvfTrainingSampler` to nullable FSL

- **File:** `rust/lance/src/index/vector/ivf.rs`
- **Lines:** ~2768 (`FixedIvfTrainingSampler::try_new`), 2790 (`sample_ranges`).
- **Change:** Today `try_new` early-returns `None` when
  `vector_field.nullable` is true (line 2773). Lifting that restriction
  — by reusing the bounded-prefetch logic added in Task A inside
  `sample_ranges` — would let the streaming coreset path (which already
  uses `streaming_ivf_prefetch_depth = 1` and 8192-row chunks) handle
  nullable columns with the same tight memory envelope as non-nullable.
  This is non-blocking but is the natural shape of the long-term fix.

## 7. Out of scope

- Changing the IVF training algorithm itself (number of partitions,
  sample rate, hierarchical kmeans config). The issue is purely about
  the prefetch stage.
- Changing `take_scan` / `random_ranges` / `dataset.sample` semantics.
  Those paths are not the OOM source.
- Cloud vs. local `io_parallelism` defaults — they are unrelated to the
  nullable-column amplification.
