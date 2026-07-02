// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::ArrayData;
use arrow::datatypes::DataType;
use arrow_array::new_empty_array;
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, RecordBatch, cast::AsArray, types::UInt64Type,
};
use arrow_buffer::{Buffer, MutableBuffer};
use futures::{Stream, StreamExt, TryStreamExt, stream};
use lance_arrow::DataTypeExt;
use lance_core::ROW_ADDR;
use lance_core::datatypes::Schema;
use lance_datafusion::projection::ProjectionPlan;
use lance_linalg::distance::DistanceType;
use log::{info, warn};
use rand::rngs::SmallRng;
use rand::seq::{IteratorRandom, SliceRandom};
use rand::{Rng, SeedableRng};
use tokio::sync::Mutex;

use crate::dataset::{Dataset, ProjectionRequest, TakeBuilder};
use crate::{Error, Result};

/// Rows fetched per `take` when materializing sampled vectors, bounding each
/// take's batch size and peak memory.
const TAKE_CHUNK_SIZE: usize = 8192;

/// Helper function to extract a column from a RecordBatch, supporting nested field paths.
///
/// This function handles:
/// - Simple column names: "column"
/// - Nested paths: "parent.child" or "parent.child.grandchild"
/// - Backtick-escaped field names: "parent.`field.with.dots`"
fn get_column_from_batch(batch: &RecordBatch, column: &str) -> Result<ArrayRef> {
    // Try to get the column directly first (fast path for simple columns)
    if let Some(col) = batch.column_by_name(column) {
        return Ok(col.clone());
    }

    // Parse the field path using Lance's field path parsing logic
    // This properly handles backtick-escaped field names
    let parts = lance_core::datatypes::parse_field_path(column)
        .map_err(|e| Error::index(format!("Failed to parse field path '{}': {}", column, e)))?;

    if parts.is_empty() {
        return Err(Error::index(format!(
            "Invalid empty field path: {}",
            column
        )));
    }

    // Get the root column
    let mut current_array: ArrayRef = batch
        .column_by_name(&parts[0])
        .ok_or_else(|| {
            Error::index(format!(
                "Column '{}' does not exist in batch (looking for root field '{}')",
                column, parts[0]
            ))
        })?
        .clone();

    // Navigate through nested struct fields
    for part in &parts[1..] {
        let struct_array = current_array
            .as_any()
            .downcast_ref::<arrow_array::StructArray>()
            .ok_or_else(|| {
                Error::index(format!(
                    "Cannot access nested field '{}' in column '{}': parent is not a struct",
                    part, column
                ))
            })?;

        current_array = struct_array
            .column_by_name(part)
            .ok_or_else(|| {
                Error::index(format!(
                    "Nested field '{}' does not exist in column '{}'",
                    part, column
                ))
            })?
            .clone();
    }

    Ok(current_array)
}

async fn estimate_multivector_vectors_per_row(
    dataset: &Dataset,
    column: &str,
    num_rows: usize,
    fragments: Option<&[u32]>,
) -> Result<usize> {
    if num_rows == 0 {
        return Ok(1030);
    }

    let projection = dataset.schema().project(&[column])?;

    // Try a few random samples first (fast path).
    let sample_batch_size = std::cmp::min(64, num_rows);
    for _ in 0..8 {
        let batch = dataset
            .sample(sample_batch_size, &projection, fragments)
            .await?;
        let array = get_column_from_batch(&batch, column)?;
        let list_array = array.as_list::<i32>();
        for i in 0..list_array.len() {
            if list_array.is_null(i) {
                continue;
            }
            let len = list_array.value_length(i) as usize;
            if len > 0 {
                return Ok(len);
            }
        }
    }

    // Fallback: scan a small prefix to find a non-null example. This avoids rare
    // flakiness when values are extremely sparse.
    let mut scanner = dataset.scan();
    scanner.project(&[column])?;
    if let Some(fragments) = fragments {
        scanner.with_fragments(resolve_scan_fragments(dataset, fragments)?);
    }
    let column_expr = lance_datafusion::logical_expr::field_path_to_expr(column)?;
    scanner.filter_expr(column_expr.is_not_null());
    scanner.limit(Some(std::cmp::min(num_rows, 1024) as i64), None)?;
    let batch = scanner.try_into_batch().await?;
    let array = get_column_from_batch(&batch, column)?;
    let list_array = array.as_list::<i32>();
    for i in 0..list_array.len() {
        let len = list_array.value_length(i) as usize;
        if len > 0 {
            return Ok(len);
        }
    }

    warn!(
        "Could not find a non-empty multivector value for column {}, falling back to n=1030",
        column
    );
    Ok(1030)
}

/// Get the vector dimension of the given column in the schema.
pub fn get_vector_dim(schema: &Schema, column: &str) -> Result<usize> {
    let field = schema.field(column).ok_or(Error::index(format!(
        "Column {} does not exist in schema {}",
        column, schema
    )))?;
    infer_vector_dim(&field.data_type())
}

/// Infer the vector dimension from the given data type.
pub fn infer_vector_dim(data_type: &arrow::datatypes::DataType) -> Result<usize> {
    infer_vector_dim_impl(data_type, false)
}

fn infer_vector_dim_impl(data_type: &arrow::datatypes::DataType, in_list: bool) -> Result<usize> {
    match (data_type, in_list) {
        (arrow::datatypes::DataType::FixedSizeList(_, dim), _) => Ok(*dim as usize),
        (arrow::datatypes::DataType::List(inner), false) => {
            infer_vector_dim_impl(inner.data_type(), true)
        }
        _ => Err(Error::invalid_input(format!(
            "Data type is not a vector (FixedSizeListArray or List<FixedSizeListArray>), but {:?}",
            data_type
        ))),
    }
}

/// Checks whether the given column is with a valid vector type
/// returns the vector type (FixedSizeList for vectors, or List for multivectors),
/// and element type (Float16/Float32/Float64 or UInt8 for binary vectors).
pub fn get_vector_type(
    schema: &Schema,
    column: &str,
) -> Result<(arrow_schema::DataType, arrow_schema::DataType)> {
    let field = schema.field(column).ok_or(Error::index(format!(
        "column {} does not exist in schema {}",
        column, schema
    )))?;
    Ok((
        field.data_type(),
        infer_vector_element_type(&field.data_type())?,
    ))
}

/// Returns the default distance type for the given vector element type.
pub fn default_distance_type_for(element_type: &arrow_schema::DataType) -> DistanceType {
    match element_type {
        arrow_schema::DataType::UInt8 => DistanceType::Hamming,
        _ => DistanceType::L2,
    }
}

/// Validate that the distance type is supported by the vector element type.
pub fn validate_distance_type_for(
    distance_type: DistanceType,
    element_type: &arrow_schema::DataType,
) -> Result<()> {
    let supported = match element_type {
        arrow_schema::DataType::UInt8 => matches!(distance_type, DistanceType::Hamming),
        arrow_schema::DataType::Int8
        | arrow_schema::DataType::Float16
        | arrow_schema::DataType::Float32
        | arrow_schema::DataType::Float64 => {
            matches!(
                distance_type,
                DistanceType::L2 | DistanceType::Cosine | DistanceType::Dot
            )
        }
        _ => false,
    };

    if supported {
        Ok(())
    } else {
        Err(Error::invalid_input(format!(
            "Distance type {} does not support {} vectors",
            distance_type, element_type
        )))
    }
}

/// If the data type is a fixed size list or list of fixed size list return the inner element type
/// and verify it is a type we can create a vector index on.
///
/// Return an error if the data type is any other type
pub fn infer_vector_element_type(
    data_type: &arrow::datatypes::DataType,
) -> Result<arrow_schema::DataType> {
    infer_vector_element_type_impl(data_type, false)
}

fn infer_vector_element_type_impl(
    data_type: &arrow::datatypes::DataType,
    in_list: bool,
) -> Result<arrow_schema::DataType> {
    match (data_type, in_list) {
        (arrow::datatypes::DataType::FixedSizeList(element_field, _), _) => {
            match element_field.data_type() {
                arrow::datatypes::DataType::Float16
                | arrow::datatypes::DataType::Float32
                | arrow::datatypes::DataType::Float64
                | arrow::datatypes::DataType::UInt8
                | arrow::datatypes::DataType::Int8 => Ok(element_field.data_type().clone()),
                _ => Err(Error::index(format!(
                    "vector element is not expected type (Float16/Float32/Float64 or UInt8): {:?}",
                    element_field.data_type()
                ))),
            }
        }
        (arrow::datatypes::DataType::List(inner), false) => {
            infer_vector_element_type_impl(inner.data_type(), true)
        }
        _ => Err(Error::invalid_input(format!(
            "Data type is not a vector (FixedSizeListArray or List<FixedSizeListArray>), but {:?}",
            data_type
        ))),
    }
}

async fn count_rows(dataset: &Dataset, fragment_ids: Option<&[u32]>) -> Result<usize> {
    match fragment_ids {
        None => dataset.count_rows(None).await,
        Some(fragment_ids) => {
            let sorted_ids: Vec<u32>;
            let sorted_fragment_ids = if fragment_ids.windows(2).all(|w| w[0] <= w[1]) {
                fragment_ids
            } else {
                sorted_ids = {
                    let mut v = fragment_ids.to_vec();
                    v.sort_unstable();
                    v
                };
                &sorted_ids
            };
            let fragments = dataset.get_frags_from_ordered_ids(sorted_fragment_ids);
            let valid_fragments = fragments
                .into_iter()
                .enumerate()
                .map(|(i, frag)| {
                    frag.ok_or_else(|| {
                        Error::index(format!(
                            "Unexpectedly missing fragment {}",
                            sorted_fragment_ids[i]
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let cnts = stream::iter(valid_fragments)
                .map(|f| async move { f.count_rows(None).await })
                .buffer_unordered(16)
                .try_collect::<Vec<usize>>()
                .await?;
            Ok(cnts.iter().sum::<usize>())
        }
    }
}

/// Maybe sample training data from dataset, specified by column name.
///
/// Returns a [FixedSizeListArray], containing the training dataset.
///
pub async fn maybe_sample_training_data(
    dataset: &Dataset,
    column: &str,
    sample_size_hint: usize,
    fragment_ids: Option<&[u32]>,
) -> Result<FixedSizeListArray> {
    let num_rows = count_rows(dataset, fragment_ids).await?;

    let vector_field = dataset.schema().field(column).ok_or(Error::index(format!(
        "Sample training data: column {} does not exist in schema",
        column
    )))?;

    if sample_size_hint == 0 {
        info!("No sampling required, skipping sampling and returning empty array");
        let data_type = vector_field.data_type();
        let dimension = infer_vector_dim(&data_type)?;
        let element_type = infer_vector_element_type(&data_type)?;
        let fsl_type = DataType::FixedSizeList(
            Arc::new(arrow_schema::Field::new("item", element_type, false)),
            dimension as i32,
        );
        return Ok(new_empty_array(&fsl_type).as_fixed_size_list().clone());
    }

    let is_nullable = vector_field.nullable;

    let sample_size_hint = match vector_field.data_type() {
        arrow::datatypes::DataType::List(_) => {
            // for multivector, we need `sample_size_hint` vectors for training,
            // but each multivector is a list of vectors, but we don't know how many
            // vectors are in each multivector. Estimate this by looking at a non-null row.
            // Set a minimum sample size of 128 to avoid too small samples,
            // it's not a problem because 128 multivectors is just about 64 MiB
            let vectors_per_row =
                estimate_multivector_vectors_per_row(dataset, column, num_rows, fragment_ids)
                    .await?;
            sample_size_hint.div_ceil(vectors_per_row).max(128)
        }
        _ => sample_size_hint,
    };

    let should_sample = num_rows > sample_size_hint;
    if should_sample {
        info!(
            "Sample training data: sampling {} rows from {} rows for column {}",
            sample_size_hint, num_rows, column
        );
        sample_training_data(
            dataset,
            column,
            sample_size_hint,
            num_rows,
            vector_field,
            is_nullable,
            fragment_ids,
        )
        .await
    } else {
        // too small to require sampling
        info!(
            "Sample training data: scanning all {} rows for column {}",
            num_rows, column
        );
        let batch = scan_all_training_data(dataset, column, is_nullable, fragment_ids).await?;
        vector_column_to_fsl(&batch, column)
    }
}

/// Filter out non-finite vectors from sampled training data.
///
/// This is a no-op when all rows are finite, avoiding an unnecessary copy.
pub fn filter_finite_training_data(
    training_data: FixedSizeListArray,
) -> Result<FixedSizeListArray> {
    let finite_mask = lance_index::vector::utils::is_finite(&training_data);
    if finite_mask.true_count() == training_data.len() {
        Ok(training_data)
    } else {
        let filtered = arrow::compute::filter(&training_data, &finite_mask)?;
        Ok(filtered.as_fixed_size_list().clone())
    }
}

#[derive(Debug)]
pub struct PartitionLoadLock {
    partition_locks: Vec<Arc<Mutex<()>>>,
}

impl PartitionLoadLock {
    pub fn new(num_partitions: usize) -> Self {
        Self {
            partition_locks: (0..num_partitions)
                .map(|_| Arc::new(Mutex::new(())))
                .collect(),
        }
    }

    pub fn get_partition_mutex(&self, partition_id: usize) -> Arc<Mutex<()>> {
        let mtx = &self.partition_locks[partition_id];

        mtx.clone()
    }
}

/// Extract a vector column from a batch as a flat [`FixedSizeListArray`].
///
/// Handles both regular vector columns (FixedSizeList) and multivector columns
/// (List\<FixedSizeList\>), flattening the latter.
fn vector_column_to_fsl(batch: &RecordBatch, column: &str) -> Result<FixedSizeListArray> {
    let array = get_column_from_batch(batch, column)?;
    match array.data_type() {
        arrow::datatypes::DataType::FixedSizeList(_, _) => Ok(array.as_fixed_size_list().clone()),
        arrow::datatypes::DataType::List(_) => {
            let list_array = array.as_list::<i32>();
            let vectors = list_array.values().as_fixed_size_list();
            Ok(vectors.clone())
        }
        _ => Err(Error::index(format!(
            "Sample training data: column {} is not a vector column",
            column
        ))),
    }
}

/// Scan the entire dataset to collect training data, optionally filtering nulls.
///
/// Used when the dataset is small enough that random sampling is unnecessary.
async fn scan_all_training_data(
    dataset: &Dataset,
    column: &str,
    is_nullable: bool,
    fragment_ids: Option<&[u32]>,
) -> Result<RecordBatch> {
    let mut scanner = dataset.scan();
    scanner.project(&[column])?;
    if let Some(fragment_ids) = fragment_ids {
        scanner.with_fragments(resolve_scan_fragments(dataset, fragment_ids)?);
    }
    if is_nullable {
        let column_expr = lance_datafusion::logical_expr::field_path_to_expr(column)?;
        scanner.filter_expr(column_expr.is_not_null());
    }
    let batch = scanner.try_into_batch().await?;
    info!(
        "Sample training data: retrieved {} rows scanning full dataset",
        batch.num_rows()
    );
    Ok(batch)
}

/// Sample training data from the dataset.
///
/// Dispatches to the most efficient strategy based on column type and nullability:
/// - Non-nullable FSL: [`sample_fsl_uniform`] — true uniform random row indices via chunked `take`.
/// - Nullable FSL: [`sample_nullable_fsl`] — streaming range-based reads with null filtering.
/// - Non-FSL (multivector): [`sample_nullable_fallback`] — streaming range-based reads.
async fn sample_training_data(
    dataset: &Dataset,
    column: &str,
    sample_size_hint: usize,
    num_rows: usize,
    vector_field: &lance_core::datatypes::Field,
    is_nullable: bool,
    fragment_ids: Option<&[u32]>,
) -> Result<FixedSizeListArray> {
    let byte_width = vector_field
        .data_type()
        .byte_width_opt()
        .unwrap_or(4 * 1024);

    if let Some(fragment_ids) = fragment_ids {
        if !is_nullable {
            let projection = dataset.schema().project(&[column])?;
            let batch = dataset
                .sample(sample_size_hint, &projection, Some(fragment_ids))
                .await?;
            info!(
                "Sample training data: retrieved {} rows by fragment-limited random sampling",
                batch.num_rows()
            );
            return vector_column_to_fsl(&batch, column);
        }
        // Rows the consumer still needs. The fragment producer sizes each
        // prefetch round to this outstanding demand, keeping reads bounded by
        // the requested sample size.
        let still_needed = Arc::new(AtomicUsize::new(sample_size_hint));
        let scan = sample_training_data_scan_from_fragments(
            dataset,
            column,
            sample_size_hint,
            fragment_ids,
            still_needed.clone(),
        )?;
        return match vector_field.data_type() {
            DataType::FixedSizeList(_, _) => {
                sample_nullable_fsl(
                    column,
                    sample_size_hint,
                    byte_width,
                    vector_field,
                    scan,
                    Some(still_needed),
                )
                .await
            }
            _ => {
                sample_nullable_fallback(
                    column,
                    sample_size_hint,
                    is_nullable,
                    scan,
                    Some(still_needed),
                )
                .await
            }
        };
    }

    match vector_field.data_type() {
        DataType::FixedSizeList(_, _) if !is_nullable && fragment_ids.is_none() => {
            sample_fsl_uniform(
                dataset,
                column,
                sample_size_hint,
                num_rows,
                byte_width,
                vector_field,
            )
            .await
        }
        DataType::FixedSizeList(_, _) => {
            let scan =
                sample_training_data_scan(dataset, column, sample_size_hint, num_rows, byte_width)?;
            sample_nullable_fsl(
                column,
                sample_size_hint,
                byte_width,
                vector_field,
                scan,
                None,
            )
            .await
        }
        _ => {
            let scan =
                sample_training_data_scan(dataset, column, sample_size_hint, num_rows, byte_width)?;
            sample_nullable_fallback(column, sample_size_hint, is_nullable, scan, None).await
        }
    }
}

/// Create a streaming scan over random ranges for sampling.
fn sample_training_data_scan(
    dataset: &Dataset,
    column: &str,
    sample_size_hint: usize,
    num_rows: usize,
    byte_width: usize,
) -> Result<crate::dataset::scanner::DatasetRecordBatchStream> {
    let block_size = dataset.object_store.as_ref().block_size();
    let ranges = random_ranges(num_rows, sample_size_hint, block_size, byte_width);
    Ok(dataset.take_scan(
        Box::pin(futures::stream::iter(ranges).map(Ok)),
        Arc::new(dataset.schema().project(&[column])?),
        dataset.object_store.as_ref().io_parallelism(),
    ))
}

/// State threaded through the fragment-limited sampling stream.
enum FragmentSampleState {
    /// Runs once on the first poll: a validity-only pre-pass that locates a
    /// random sample of non-null row addresses.
    Locate {
        dataset: Arc<Dataset>,
        column: String,
        projection: Arc<ProjectionPlan>,
        scan_fragments: Vec<lance_table::format::Fragment>,
        sample_size_hint: usize,
        still_needed: Arc<AtomicUsize>,
    },
    /// Emits `take` batches for the sampled non-null rows, `TAKE_CHUNK_SIZE`
    /// rows at a time.
    Take {
        dataset: Arc<Dataset>,
        projection: Arc<ProjectionPlan>,
        row_addrs: Vec<u64>,
        cursor: usize,
        still_needed: Arc<AtomicUsize>,
    },
}

/// Build a batch stream over a random sample of the *non-null* rows in the
/// selected fragments.
///
/// A nullable vector column may be extremely sparse or entirely null. Sampling
/// random offsets and `take`-ing them (as the non-nullable path does) then
/// reads — only to discard — the full vector bytes of every null row it happens
/// to hit, and for an all-null column it must visit every selected row before
/// it can return. To avoid that CPU/I/O cost this first runs a validity-only
/// pre-pass ([`sample_non_null_row_addresses`]): a scan filtered by
/// `column IS NOT NULL` that projects only the `_rowaddr` meta column (no vector
/// bytes), reservoir-sampling the non-null row addresses down to
/// `sample_size_hint`. It then `take`s only those sampled, guaranteed-non-null
/// rows. Reservoir sampling bounds the retained state to the requested sample
/// regardless of how many non-null rows exist, and an all-null column yields an
/// empty sample and issues no takes at all.
///
/// Both the nullable FSL and multivector paths reuse this stream to avoid
/// duplicating fragment sampling logic. `still_needed` (the consumer's
/// outstanding demand) lets the consumer stop the stream early.
fn sample_training_data_scan_from_fragments(
    dataset: &Dataset,
    column: &str,
    sample_size_hint: usize,
    fragment_ids: &[u32],
    still_needed: Arc<AtomicUsize>,
) -> Result<Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>> {
    if fragment_ids.is_empty() {
        return Err(Error::invalid_input(
            "Training fragment filter must not be empty".to_string(),
        ));
    }

    let scan_fragments = resolve_scan_fragments(dataset, fragment_ids)?;
    let dataset = Arc::new(dataset.clone());
    let projection = Arc::new(
        ProjectionRequest::from(dataset.schema().project(&[column])?)
            .into_projection_plan(dataset.clone())?,
    );

    let stream = stream::try_unfold(
        FragmentSampleState::Locate {
            dataset,
            column: column.to_string(),
            projection,
            scan_fragments,
            sample_size_hint,
            still_needed,
        },
        move |state| async move {
            let (dataset, projection, row_addrs, cursor, still_needed) = match state {
                FragmentSampleState::Locate {
                    dataset,
                    column,
                    projection,
                    scan_fragments,
                    sample_size_hint,
                    still_needed,
                } => {
                    if still_needed.load(Ordering::Relaxed) == 0 {
                        return Ok(None);
                    }
                    let row_addrs = sample_non_null_row_addresses(
                        &dataset,
                        &column,
                        sample_size_hint,
                        scan_fragments,
                    )
                    .await?;
                    (dataset, projection, row_addrs, 0, still_needed)
                }
                FragmentSampleState::Take {
                    dataset,
                    projection,
                    row_addrs,
                    cursor,
                    still_needed,
                } => (dataset, projection, row_addrs, cursor, still_needed),
            };

            if cursor >= row_addrs.len() || still_needed.load(Ordering::Relaxed) == 0 {
                return Ok(None);
            }
            let end = (cursor + TAKE_CHUNK_SIZE).min(row_addrs.len());
            let batch = TakeBuilder::try_new_from_addresses(
                dataset.clone(),
                row_addrs[cursor..end].to_vec(),
                projection.clone(),
            )?
            .execute()
            .await?;
            Ok(Some((
                batch,
                FragmentSampleState::Take {
                    dataset,
                    projection,
                    row_addrs,
                    cursor: end,
                    still_needed,
                },
            )))
        },
    );
    Ok(Box::pin(stream))
}

/// Run a validity-only pre-pass over the selected fragments and return a
/// uniform random sample of at most `sample_size_hint` non-null row addresses.
///
/// The scan filters on `column IS NOT NULL` but projects only the `_rowaddr`
/// meta column, so no vector bytes are materialized while locating non-null
/// rows. Reservoir sampling (Algorithm R) keeps the retained set bounded to
/// `sample_size_hint` regardless of how many non-null rows the column has.
async fn sample_non_null_row_addresses(
    dataset: &Dataset,
    column: &str,
    sample_size_hint: usize,
    scan_fragments: Vec<lance_table::format::Fragment>,
) -> Result<Vec<u64>> {
    let mut scanner = dataset.scan();
    scanner.with_fragments(scan_fragments);
    scanner.empty_project()?;
    scanner.with_row_address();
    let column_expr = lance_datafusion::logical_expr::field_path_to_expr(column)?;
    scanner.filter_expr(column_expr.is_not_null());

    let mut stream = scanner.try_into_stream().await?;
    let mut reservoir: Vec<u64> = Vec::new();
    let mut seen: u64 = 0;
    let mut rng = SmallRng::from_os_rng();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let row_addrs = batch
            .column_by_name(ROW_ADDR)
            .ok_or_else(|| {
                Error::internal(
                    "Sample training data: validity pre-pass did not return row addresses"
                        .to_string(),
                )
            })?
            .as_primitive::<UInt64Type>();
        for &addr in row_addrs.values().iter() {
            if reservoir.len() < sample_size_hint {
                reservoir.push(addr);
            } else if sample_size_hint > 0 {
                // Algorithm R: replace a random slot with probability
                // sample_size_hint / (seen + 1).
                let candidate = rng.random_range(0..=seen) as usize;
                if candidate < sample_size_hint {
                    reservoir[candidate] = addr;
                }
            }
            seen += 1;
        }
    }
    // Sorted addresses let `take` read fragments front-to-back.
    reservoir.sort_unstable();
    info!(
        "Sample training data: validity pre-pass located {} non-null rows, sampled {} for column {}",
        seen,
        reservoir.len(),
        column
    );
    Ok(reservoir)
}

fn resolve_scan_fragments(
    dataset: &Dataset,
    fragment_ids: &[u32],
) -> Result<Vec<lance_table::format::Fragment>> {
    let mut ordered_ids = fragment_ids.to_vec();
    ordered_ids.sort_unstable();
    let fragments = dataset.get_frags_from_ordered_ids(&ordered_ids);
    if let Some(missing_id) = fragments
        .iter()
        .zip(ordered_ids.iter())
        .find_map(|(fragment, fragment_id)| fragment.is_none().then_some(*fragment_id))
    {
        return Err(Error::invalid_input(format!(
            "Unknown fragment id {missing_id} in training fragment filter"
        )));
    }
    Ok(fragments
        .into_iter()
        .map(|fragment| fragment.unwrap().metadata().clone())
        .collect())
}

/// Build a FixedSizeListArray from raw flat value bytes.
fn fsl_values_to_array(
    field: &lance_core::datatypes::Field,
    mut values_buf: MutableBuffer,
    num_rows: usize,
) -> Result<FixedSizeListArray> {
    let (inner_field, dim) = match field.data_type() {
        DataType::FixedSizeList(f, d) => (f, d as usize),
        other => {
            return Err(Error::index(format!(
                "Expected FixedSizeList, got {:?}",
                other
            )));
        }
    };

    let elem_size = inner_field.data_type().primitive_width().ok_or_else(|| {
        Error::index(format!(
            "FixedSizeList inner type {:?} has no fixed width",
            inner_field.data_type()
        ))
    })?;

    let expected_bytes = num_rows * dim * elem_size;
    debug_assert_eq!(values_buf.len(), expected_bytes);
    values_buf.truncate(expected_bytes);
    let buf: Buffer = values_buf.into();
    let values_array = arrow_array::make_array(ArrayData::try_new(
        inner_field.data_type().clone(),
        num_rows * dim,
        None,
        0,
        vec![buf],
        vec![],
    )?);

    Ok(FixedSizeListArray::try_new(
        inner_field,
        dim as i32,
        values_array,
        None,
    )?)
}

/// Stream-and-compact sampling for nullable FixedSizeList vector columns.
///
/// Unlike [`sample_nullable_fallback`], which must collect all source batches
/// in memory, this exploits the fixed-width layout of FSL columns to
/// accumulate non-null vector bytes directly into a flat buffer, dropping
/// each source batch immediately. This keeps peak memory proportional to the
/// output sample rather than the input scan.
async fn sample_nullable_fsl<S>(
    column: &str,
    sample_size_hint: usize,
    byte_width: usize,
    vector_field: &lance_core::datatypes::Field,
    mut scan: S,
    still_needed: Option<Arc<AtomicUsize>>,
) -> Result<FixedSizeListArray>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    let mut values_buf = MutableBuffer::with_capacity(sample_size_hint * byte_width);
    let mut num_non_null: usize = 0;
    let mut batch_count: usize = 0;
    let mut rows_scanned: usize = 0;

    while num_non_null < sample_size_hint {
        let remaining_rows = sample_size_hint - num_non_null;
        // A fragment-limited producer sizes its next prefetch round to this
        // outstanding demand.
        if let Some(still_needed) = &still_needed {
            still_needed.store(remaining_rows, Ordering::Relaxed);
        }
        let Some(batch) = scan.next().await else {
            break;
        };
        let batch = batch?;
        batch_count += 1;
        rows_scanned += batch.num_rows();
        let array = get_column_from_batch(&batch, column)?;
        if array.logical_null_count() >= array.len() {
            info!(
                "Sample training data: batch {} read {} rows ({} scanned, {}/{} sampled after null filtering)",
                batch_count,
                batch.num_rows(),
                rows_scanned,
                num_non_null.min(sample_size_hint),
                sample_size_hint
            );
            continue;
        }
        let previous_num_non_null = num_non_null;
        // `remaining_rows` keeps `values_buf` within its pre-allocated
        // `sample_size_hint * byte_width` capacity.
        accumulate_fsl_values(
            &mut values_buf,
            &mut num_non_null,
            &array,
            byte_width,
            true,
            remaining_rows,
        )?;
        info!(
            "Sample training data: batch {} read {} rows, accepted {} rows ({} scanned, {}/{} sampled after null filtering)",
            batch_count,
            batch.num_rows(),
            num_non_null - previous_num_non_null,
            rows_scanned,
            num_non_null.min(sample_size_hint),
            sample_size_hint
        );
    }

    // Zero the demand so any further poll of the producer terminates instead
    // of reading another round.
    if let Some(still_needed) = &still_needed {
        still_needed.store(0, Ordering::Relaxed);
    }
    let num_rows_out = num_non_null.min(sample_size_hint);
    values_buf.truncate(num_rows_out * byte_width);

    info!(
        "Sample training data: retrieved {} rows by sampling after filtering out nulls",
        num_rows_out
    );

    fsl_values_to_array(vector_field, values_buf, num_rows_out)
}
/// True uniform random sampling for non-nullable FixedSizeList columns.
///
/// Generates truly random row indices, sorts them, and fetches via
/// `dataset.take()` in chunks. Each chunk's RecordBatch is consumed into a flat
/// byte buffer and dropped immediately, keeping peak memory proportional to the
/// output sample.
async fn sample_fsl_uniform(
    dataset: &Dataset,
    column: &str,
    sample_size_hint: usize,
    num_rows: usize,
    byte_width: usize,
    vector_field: &lance_core::datatypes::Field,
) -> Result<FixedSizeListArray> {
    let indices = generate_random_indices(num_rows, sample_size_hint);
    let projection = Arc::new(dataset.schema().project(&[column])?);

    let mut values_buf = MutableBuffer::with_capacity(sample_size_hint * byte_width);
    let mut total_rows: usize = 0;

    let total_chunks = indices.len().div_ceil(TAKE_CHUNK_SIZE);
    for (chunk_idx, chunk) in indices.chunks(TAKE_CHUNK_SIZE).enumerate() {
        let batch = dataset.take(chunk, projection.clone()).await?;
        let array = get_column_from_batch(&batch, column)?;
        accumulate_fsl_values(
            &mut values_buf,
            &mut total_rows,
            &array,
            byte_width,
            false,
            usize::MAX,
        )?;
        info!(
            "Sample training data: batch {}/{} read {} rows ({}/{} sampled by uniform random sampling)",
            chunk_idx + 1,
            total_chunks,
            batch.num_rows(),
            total_rows.min(sample_size_hint),
            sample_size_hint
        );
    }

    info!(
        "Sample training data: retrieved {} rows by uniform random sampling",
        total_rows,
    );

    fsl_values_to_array(vector_field, values_buf, total_rows)
}

/// Append values from a FixedSizeList array into a flat byte buffer.
///
/// When `filter_nulls` is false and there are no nulls, copies raw bytes
/// directly from the FSL values buffer (accounting for child array offset).
/// When `filter_nulls` is true, uses Arrow's `filter` kernel to remove nulls.
/// At most `max_rows` rows are appended so callers can stop copying once their
/// sample is full; otherwise one oversized source batch can grow `values_buf`
/// far beyond the intended cap.
fn accumulate_fsl_values(
    values_buf: &mut MutableBuffer,
    num_rows: &mut usize,
    array: &ArrayRef,
    byte_width: usize,
    filter_nulls: bool,
    max_rows: usize,
) -> Result<()> {
    if max_rows == 0 {
        return Ok(());
    }
    let needs_filter = filter_nulls && array.null_count() > 0;

    if needs_filter {
        let nulls = array.nulls().unwrap();
        let mask = arrow_array::BooleanArray::from(nulls.inner().clone());
        let filtered = arrow::compute::filter(array, &mask)?;
        let fsl = filtered.as_fixed_size_list();
        let take = fsl.len().min(max_rows);
        if take == 0 {
            return Ok(());
        }
        let values_data = fsl.values().to_data();
        let value_bytes = &values_data.buffers()[0].as_slice()[..take * byte_width];
        values_buf.extend_from_slice(value_bytes);
        *num_rows += take;
    } else {
        // No nulls: copy raw bytes directly, accounting for child array offset.
        let fsl = array.as_fixed_size_list();
        let take = fsl.len().min(max_rows);
        if take == 0 {
            return Ok(());
        }
        let values = fsl.values();
        let values_data = values.to_data();
        let elem_size = byte_width / fsl.value_length() as usize;
        let offset_bytes = values_data.offset() * elem_size;
        let total_bytes = take * byte_width;
        let buf = &values_data.buffers()[0].as_slice()[offset_bytes..offset_bytes + total_bytes];
        values_buf.extend_from_slice(buf);
        *num_rows += take;
    }
    Ok(())
}

/// Fallback sampling for non-FixedSizeList columns (e.g. multivector List
/// columns). Collects batches and concatenates them. When `is_nullable` is
/// true, filters null rows from each batch.
async fn sample_nullable_fallback<S>(
    column: &str,
    sample_size_hint: usize,
    is_nullable: bool,
    mut scan: S,
    still_needed: Option<Arc<AtomicUsize>>,
) -> Result<FixedSizeListArray>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    let mut schema = None;
    let mut filtered = Vec::new();
    let mut num_non_null: usize = 0;
    let mut batch_count: usize = 0;
    let mut rows_scanned: usize = 0;

    while num_non_null < sample_size_hint {
        let remaining_rows = sample_size_hint - num_non_null;
        // A fragment-limited producer sizes its next prefetch round to this
        // outstanding demand.
        if let Some(still_needed) = &still_needed {
            still_needed.store(remaining_rows, Ordering::Relaxed);
        }
        let Some(batch) = scan.next().await else {
            break;
        };
        let batch = batch?;
        batch_count += 1;
        let batch_rows = batch.num_rows();
        rows_scanned += batch_rows;
        let array = get_column_from_batch(&batch, column)?;
        if is_nullable && array.logical_null_count() >= array.len() {
            info!(
                "Sample training data (fallback): batch {} read {} rows ({} scanned, {}/{} sampled after null filtering)",
                batch_count,
                batch_rows,
                rows_scanned,
                num_non_null.min(sample_size_hint),
                sample_size_hint
            );
            continue;
        }
        schema.get_or_insert_with(|| batch.schema());
        let batch = if is_nullable {
            filter_non_null_rows(array, batch)?
        } else {
            batch
        };
        // Slicing to the outstanding demand keeps the retained batches, and
        // the post-loop `concat_batches`, bounded by the sample size.
        let accepted_rows = batch.num_rows().min(remaining_rows);
        let batch = if accepted_rows < batch.num_rows() {
            batch.slice(0, accepted_rows)
        } else {
            batch
        };
        num_non_null += accepted_rows;
        info!(
            "Sample training data (fallback): batch {} read {} rows, accepted {} rows ({} scanned, {}/{} sampled)",
            batch_count,
            batch_rows,
            accepted_rows,
            rows_scanned,
            num_non_null.min(sample_size_hint),
            sample_size_hint
        );
        filtered.push(batch);
    }

    // Zero the demand so any further poll of the producer terminates instead
    // of reading another round.
    if let Some(still_needed) = &still_needed {
        still_needed.store(0, Ordering::Relaxed);
    }

    let Some(schema) = schema else {
        return Err(Error::index("No non-null training data found".to_string()));
    };
    let batch = arrow::compute::concat_batches(&schema, &filtered)?;
    let num_rows_out = batch.num_rows().min(sample_size_hint);
    let batch = batch.slice(0, num_rows_out);

    info!(
        "Sample training data (fallback): retrieved {} rows by sampling after filtering out nulls",
        num_rows_out
    );

    vector_column_to_fsl(&batch, column)
}

/// Filter a batch to only include rows where `array` is non-null.
fn filter_non_null_rows(array: ArrayRef, batch: RecordBatch) -> Result<RecordBatch> {
    if let Some(nulls) = array.nulls() {
        let mask = arrow_array::BooleanArray::from(nulls.inner().clone());
        Ok(arrow::compute::filter_record_batch(&batch, &mask)?)
    } else {
        Ok(batch)
    }
}

/// Generate `k` unique sorted random row indices from `[0, num_rows)`.
///
/// Uses two strategies depending on sparsity:
/// - Sparse (`k * 2 < num_rows`): HashSet rejection sampling, O(k) expected.
/// - Dense: Fisher-Yates partial shuffle, O(num_rows) allocation.
fn generate_random_indices(num_rows: usize, k: usize) -> Vec<u64> {
    assert!(k <= num_rows);
    let mut rng = SmallRng::from_os_rng();
    let mut indices = if k * 2 < num_rows {
        let mut set = std::collections::HashSet::with_capacity(k);
        while set.len() < k {
            set.insert(rng.random_range(0..num_rows as u64));
        }
        set.into_iter().collect::<Vec<_>>()
    } else {
        let mut all: Vec<u64> = (0..num_rows as u64).collect();
        // Partial Fisher-Yates: only shuffle first k elements.
        for i in 0..k {
            let j = rng.random_range(i..all.len());
            all.swap(i, j);
        }
        all.truncate(k);
        all
    };
    indices.sort_unstable();
    indices
}

/// Generate random ranges to sample from a dataset.
///
/// This will return an iterator of ranges that cover the whole dataset. It
/// provides an unbound iterator so that the caller can decide when to stop.
/// This is useful when the caller wants to sample a fixed number of rows, but
/// has an additional filter that must be applied.
///
/// Parameters:
/// * `num_rows`: number of rows in the dataset
/// * `sample_size_hint`: the target number of rows to be sampled in the end.
///   This is a hint for the minimum number of rows that will be consumed, but
///   the caller may consume more than this.
/// * `block_size`: the byte size of ranges that should be used.
/// * `byte_width`: the byte width of the vectors that will be sampled.
fn random_ranges(
    num_rows: usize,
    sample_size_hint: usize,
    block_size: usize,
    byte_width: usize,
) -> impl Iterator<Item = std::ops::Range<u64>> + Send {
    let rows_per_batch = 1.max(block_size / byte_width);
    let mut rng = SmallRng::from_os_rng();
    let num_bins = num_rows.div_ceil(rows_per_batch);

    let bins_iter: Box<dyn Iterator<Item = usize> + Send> = if sample_size_hint * 5 >= num_rows {
        // It's faster to just allocate and shuffle
        let mut indices = (0..num_bins).collect::<Vec<_>>();
        indices.shuffle(&mut rng);
        Box::new(indices.into_iter())
    } else {
        // If the sample is a small proportion, then we can instead use a set
        // to track which bins we have seen. We start by using the sample_size_hint
        // to provide an efficient start, and from there we randomly choose bins
        // one by one.
        let num_bins = num_rows.div_ceil(rows_per_batch);
        // Start with the minimum number we will need.
        let min_sample_size = sample_size_hint / rows_per_batch;
        let starting_bins = (0..num_bins).choose_multiple(&mut rng, min_sample_size);
        let mut seen = starting_bins
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();

        let additional = std::iter::from_fn(move || {
            loop {
                if seen.len() >= num_bins {
                    break None;
                }
                let next = (0..num_bins).choose(&mut rng).unwrap();
                if seen.contains(&next) {
                    continue;
                } else {
                    seen.insert(next);
                    return Some(next);
                }
            }
        });

        Box::new(starting_bins.into_iter().chain(additional))
    };

    bins_iter.map(move |i| {
        let start = (i * rows_per_batch) as u64;
        let end = ((i + 1) * rows_per_batch) as u64;
        let end = std::cmp::min(end, num_rows as u64);
        start..end
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::dataset::InsertBuilder;
    use arrow_array::{ArrayRef, Float32Array, types::Float32Type};
    use arrow_buffer::{BooleanBufferBuilder, NullBuffer};
    use arrow_schema::{DataType, Field};
    use lance_arrow::FixedSizeListArrayExt;
    use lance_datagen::{ArrayGeneratorExt, Dimension, RowCount, array, gen_batch};

    #[rstest::rstest]
    #[test]
    fn test_random_ranges(
        #[values(99, 100, 102)] num_rows: usize,
        #[values(10, 100)] sample_size: usize,
    ) {
        // We can just assert that the output when sorted is the same as the input
        let block_size = 100;
        let byte_width = 10;

        let bin_size = block_size / byte_width;
        assert_eq!(bin_size, 10);

        let mut ranges =
            random_ranges(num_rows, sample_size, block_size, byte_width).collect::<Vec<_>>();
        ranges.sort_by_key(|r| r.start);
        let expected = (0..num_rows as u64).step_by(bin_size).map(|start| {
            let end = std::cmp::min(start + bin_size as u64, num_rows as u64);
            start..end
        });
        assert_eq!(ranges, expected.collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test_maybe_sample_training_data_multivector_infers_vectors_per_row() {
        let nrows: usize = 2000;
        let dims: u32 = 8;
        let vectors_per_row: u32 = 2;

        let mv = array::cycle_vec_var(
            array::rand_vec::<Float32Type>(Dimension::from(dims)),
            Dimension::from(vectors_per_row),
            Dimension::from(vectors_per_row + 1),
        );

        let data = gen_batch()
            .col("mv", mv)
            .into_batch_rows(RowCount::from(nrows as u64))
            .unwrap();

        let dataset = InsertBuilder::new("memory://")
            .execute(vec![data])
            .await
            .unwrap();

        let training_data = maybe_sample_training_data(&dataset, "mv", 1000, None)
            .await
            .unwrap();
        assert_eq!(training_data.len(), 1000);
    }

    #[rstest::rstest]
    #[case::f16(arrow::datatypes::DataType::Float16, 2)]
    #[case::f32(arrow::datatypes::DataType::Float32, 4)]
    #[case::f64(arrow::datatypes::DataType::Float64, 8)]
    #[test]
    fn test_fsl_values_to_array_roundtrip(
        #[case] elem_type: arrow::datatypes::DataType,
        #[case] elem_size: usize,
    ) {
        let dim = 4;
        let num_rows = 3;
        // Fill with recognizable byte patterns: each element gets its index as bytes.
        let num_elems = num_rows * dim;
        let values_vec: Vec<u8> = (0..num_elems)
            .flat_map(|i| {
                let mut bytes = vec![0u8; elem_size];
                // Write index into the first bytes (little-endian).
                let i_bytes = (i as u32).to_le_bytes();
                bytes[..i_bytes.len().min(elem_size)]
                    .copy_from_slice(&i_bytes[..i_bytes.len().min(elem_size)]);
                bytes
            })
            .collect();
        let expected_bytes = values_vec.clone();
        let values_buf = MutableBuffer::from(values_vec);

        let dt = DataType::FixedSizeList(
            Arc::new(arrow::datatypes::Field::new("item", elem_type, true)),
            dim as i32,
        );
        let field = lance_core::datatypes::Field::new_arrow("vec", dt, true).unwrap();
        let fsl = fsl_values_to_array(&field, values_buf, num_rows).unwrap();
        assert_eq!(fsl.len(), num_rows);
        assert_eq!(fsl.value_length(), dim as i32);

        // Verify the raw bytes round-tripped correctly.
        let out_data = fsl.values().to_data();
        let out_bytes = out_data.buffers()[0].as_slice();
        assert_eq!(&out_bytes[..expected_bytes.len()], &expected_bytes[..]);
    }

    #[rstest::rstest]
    #[case::f32_nullable(array::rand_vec::<Float32Type>(Dimension::from(8)), true)]
    #[case::f64_nullable(array::rand_vec::<arrow_array::types::Float64Type>(Dimension::from(8)), true)]
    #[case::f32_non_nullable(array::rand_vec::<Float32Type>(Dimension::from(8)), false)]
    #[case::f64_non_nullable(array::rand_vec::<arrow_array::types::Float64Type>(Dimension::from(8)), false)]
    #[tokio::test]
    async fn test_maybe_sample_training_data_fsl(
        #[case] vec_gen: Box<dyn lance_datagen::ArrayGenerator>,
        #[case] nullable: bool,
    ) {
        let nrows: usize = 2000;
        let dims: u32 = 8;
        let sample_size: usize = 500;

        let col_gen = if nullable {
            vec_gen.with_random_nulls(0.5)
        } else {
            vec_gen
        };
        let data = gen_batch()
            .col("vec", col_gen)
            .into_batch_rows(RowCount::from(nrows as u64))
            .unwrap();

        let dataset = InsertBuilder::new("memory://fsl_sample_test")
            .execute(vec![data])
            .await
            .unwrap();

        let training_data = maybe_sample_training_data(&dataset, "vec", sample_size, None)
            .await
            .unwrap();

        assert!(training_data.len() > 0 && training_data.len() <= sample_size);
        assert_eq!(training_data.null_count(), 0);
        assert_eq!(training_data.value_length(), dims as i32);
    }

    #[rstest::rstest]
    #[case::sparse(1_000_000, 100)]
    #[case::dense(100, 80)]
    #[case::exact(100, 100)]
    #[test]
    fn test_generate_random_indices(#[case] num_rows: usize, #[case] k: usize) {
        let indices = generate_random_indices(num_rows, k);
        assert_eq!(indices.len(), k);
        assert!(indices.windows(2).all(|w| w[0] < w[1]));
        assert!(indices.iter().all(|&i| (i as usize) < num_rows));
    }

    #[test]
    fn test_accumulate_fsl_values_with_sliced_array() {
        let dim = 4usize;
        let values: Vec<f32> = (0..40).map(|i| i as f32).collect();
        let fsl = FixedSizeListArray::try_new_from_values(
            arrow_array::Float32Array::from(values),
            dim as i32,
        )
        .unwrap();
        let sliced = fsl.slice(3, 4);

        let byte_width = dim * std::mem::size_of::<f32>();
        let mut buf = MutableBuffer::new(0);
        let mut num_rows = 0usize;
        let sliced_ref: ArrayRef = Arc::new(sliced);
        accumulate_fsl_values(
            &mut buf,
            &mut num_rows,
            &sliced_ref,
            byte_width,
            false,
            usize::MAX,
        )
        .unwrap();

        assert_eq!(num_rows, 4);
        let result: &[f32] =
            unsafe { std::slice::from_raw_parts(buf.as_ptr() as *const f32, 4 * dim) };
        let expected: Vec<f32> = (12..28).map(|i| i as f32).collect();
        assert_eq!(result, &expected[..]);
    }

    #[test]
    fn test_filter_finite_training_data() {
        let values = Float32Array::from_iter_values([
            1.0,
            2.0, // finite
            f32::NAN,
            0.0, // non-finite
            3.0,
            4.0, // finite
        ]);
        let field = Arc::new(Field::new("item", DataType::Float32, true));
        let training_data = FixedSizeListArray::try_new(field, 2, Arc::new(values), None).unwrap();

        let filtered = filter_finite_training_data(training_data).unwrap();
        assert_eq!(filtered.len(), 2);
        let vals = filtered.values().as_primitive::<Float32Type>();
        assert_eq!(vals.values(), &[1.0, 2.0, 3.0, 4.0]);
    }

    #[tokio::test]
    async fn test_estimate_multivector_vectors_per_row_fallback_1030() {
        let nrows: usize = 256;
        let dims: u32 = 8;

        let mv = array::cycle_vec_var(
            array::rand_vec::<Float32Type>(Dimension::from(dims)),
            Dimension::from(2),
            Dimension::from(3),
        )
        .with_random_nulls(1.0);

        let data = gen_batch()
            .col("mv", mv)
            .into_batch_rows(RowCount::from(nrows as u64))
            .unwrap();

        let dataset = InsertBuilder::new("memory://")
            .execute(vec![data])
            .await
            .unwrap();

        let n = estimate_multivector_vectors_per_row(&dataset, "mv", nrows, None)
            .await
            .unwrap();
        assert_eq!(n, 1030);
    }

    // Creates a dataset with three fragments holding 100, 200, and 150 rows.
    async fn make_three_fragment_dataset() -> Dataset {
        use arrow_array::{RecordBatch, RecordBatchIterator};
        use arrow_schema::Schema as ArrowSchema;

        let schema = Arc::new(ArrowSchema::new(vec![arrow_schema::Field::new(
            "x",
            arrow_schema::DataType::Float32,
            false,
        )]));

        let make_batch = |n: usize| -> RecordBatch {
            let arr: ArrayRef = Arc::new(Float32Array::from_iter_values((0..n).map(|i| i as f32)));
            RecordBatch::try_new(schema.clone(), vec![arr]).unwrap()
        };

        let mut dataset = InsertBuilder::new("memory://test_count_rows_util")
            .execute(vec![make_batch(100)])
            .await
            .unwrap();
        dataset
            .append(
                RecordBatchIterator::new(vec![Ok(make_batch(200))], schema.clone()),
                None,
            )
            .await
            .unwrap();
        dataset
            .append(
                RecordBatchIterator::new(vec![Ok(make_batch(150))], schema.clone()),
                None,
            )
            .await
            .unwrap();

        dataset
    }

    #[tokio::test]
    async fn test_count_rows_none() {
        let dataset = make_three_fragment_dataset().await;
        assert_eq!(dataset.get_fragments().len(), 3);
        assert_eq!(count_rows(&dataset, None).await.unwrap(), 450);
    }

    #[tokio::test]
    async fn test_count_rows_sorted_fragment_ids() {
        let dataset = make_three_fragment_dataset().await;
        let ids: Vec<u32> = dataset
            .get_fragments()
            .iter()
            .map(|f| f.id() as u32)
            .collect();
        // Skip the middle fragment (200 rows); expect 100 + 150 = 250.
        let result = count_rows(&dataset, Some(&[ids[0], ids[2]])).await.unwrap();
        assert_eq!(result, 250);
    }

    #[tokio::test]
    async fn test_count_rows_unsorted_fragment_ids() {
        let dataset = make_three_fragment_dataset().await;
        let ids: Vec<u32> = dataset
            .get_fragments()
            .iter()
            .map(|f| f.id() as u32)
            .collect();
        // Pass the same two fragments in reverse (unsorted) order; result must match.
        let result = count_rows(&dataset, Some(&[ids[2], ids[0]])).await.unwrap();
        assert_eq!(result, 250);
    }

    /// Nullable FSL with fragment-limited sampling must fill the requested sample
    /// size when enough non-null rows exist, and terminate cleanly when all
    /// selected rows are null.
    #[tokio::test]
    async fn test_maybe_sample_training_data_fsl_nullable_fragment_limited() {
        let nrows: usize = 2000;
        let dims: u32 = 8;
        let sample_size: usize = 500;

        for (case, null_probability, expected_len) in
            [("partial_nulls", 0.5, sample_size), ("all_nulls", 1.0, 0)]
        {
            let col_gen = array::rand_vec::<Float32Type>(Dimension::from(dims))
                .with_random_nulls(null_probability);
            let data = gen_batch()
                .col("vec", col_gen)
                .into_batch_rows(RowCount::from(nrows as u64))
                .unwrap();

            let dataset = InsertBuilder::new("memory://")
                .execute(vec![data])
                .await
                .unwrap();

            let fragment_ids: Vec<u32> = dataset
                .get_fragments()
                .iter()
                .map(|f| f.id() as u32)
                .collect();

            let training_data =
                maybe_sample_training_data(&dataset, "vec", sample_size, Some(&fragment_ids))
                    .await
                    .unwrap();

            assert_eq!(training_data.len(), expected_len, "{case}");
            assert_eq!(training_data.null_count(), 0, "{case}");
            assert_eq!(training_data.value_length(), dims as i32, "{case}");
        }
    }

    /// Scan-side regression: the validity pre-pass must `take` only non-null
    /// rows and never emit more than the requested sample. Driving the producer
    /// directly and inspecting every batch catches both a pre-pass that leaks
    /// null rows and one that over-reads past the sample size.
    #[tokio::test]
    async fn test_sample_fragment_scan_takes_only_non_null_rows() {
        let nrows: usize = 4000;
        let dims: u32 = 8;
        let sample_size: usize = 500;

        let col_gen = array::rand_vec::<Float32Type>(Dimension::from(dims)).with_random_nulls(0.5);
        let data = gen_batch()
            .col("vec", col_gen)
            .into_batch_rows(RowCount::from(nrows as u64))
            .unwrap();

        let dataset = InsertBuilder::new("memory://fsl_scan_non_null_test")
            .execute(vec![data])
            .await
            .unwrap();

        let fragment_ids: Vec<u32> = dataset
            .get_fragments()
            .iter()
            .map(|f| f.id() as u32)
            .collect();

        // The consumer's demand never drops to zero here, so the stream runs to
        // exhaustion and total output reflects the sample cap alone.
        let still_needed = Arc::new(AtomicUsize::new(sample_size));
        let mut scan = sample_training_data_scan_from_fragments(
            &dataset,
            "vec",
            sample_size,
            &fragment_ids,
            still_needed.clone(),
        )
        .unwrap();

        let mut total_rows = 0;
        while let Some(batch) = scan.next().await {
            let batch = batch.unwrap();
            let array = get_column_from_batch(&batch, "vec").unwrap();
            assert_eq!(
                array.logical_null_count(),
                0,
                "validity pre-pass took a null row"
            );
            total_rows += batch.num_rows();
        }
        assert!(
            total_rows <= sample_size,
            "pre-pass took {total_rows} rows but only {sample_size} were requested"
        );
        // With ~2000 non-null rows available, the sample must fill completely.
        assert_eq!(total_rows, sample_size);
    }

    /// An all-null column must terminate the fragment-limited stream without a
    /// single `take`: the validity pre-pass finds no non-null rows.
    #[tokio::test]
    async fn test_sample_fragment_scan_all_null_emits_nothing() {
        let nrows: usize = 2000;
        let dims: u32 = 8;
        let sample_size: usize = 500;

        let col_gen = array::rand_vec::<Float32Type>(Dimension::from(dims)).with_random_nulls(1.0);
        let data = gen_batch()
            .col("vec", col_gen)
            .into_batch_rows(RowCount::from(nrows as u64))
            .unwrap();

        let dataset = InsertBuilder::new("memory://fsl_scan_all_null_test")
            .execute(vec![data])
            .await
            .unwrap();

        let fragment_ids: Vec<u32> = dataset
            .get_fragments()
            .iter()
            .map(|f| f.id() as u32)
            .collect();

        let still_needed = Arc::new(AtomicUsize::new(sample_size));
        let mut scan = sample_training_data_scan_from_fragments(
            &dataset,
            "vec",
            sample_size,
            &fragment_ids,
            still_needed,
        )
        .unwrap();

        assert!(
            scan.next().await.is_none(),
            "all-null column should yield no take batches"
        );
    }

    #[test]
    fn test_accumulate_fsl_values_respects_max_rows() {
        let dim: usize = 4;
        let total_rows: usize = 100;
        let max_rows: usize = 16;
        let byte_width = dim * std::mem::size_of::<f32>();

        let values: Vec<f32> = (0..total_rows * dim).map(|i| i as f32).collect();
        let fsl = FixedSizeListArray::try_new_from_values(Float32Array::from(values), dim as i32)
            .unwrap();
        let arr: ArrayRef = Arc::new(fsl);

        let mut buf = MutableBuffer::new(0);
        let mut num_rows = 0usize;
        accumulate_fsl_values(&mut buf, &mut num_rows, &arr, byte_width, true, max_rows).unwrap();

        assert_eq!(num_rows, max_rows);
        assert_eq!(buf.len(), max_rows * byte_width);

        let values: Vec<f32> = (0..total_rows * dim).map(|i| i as f32).collect();
        let item_field = Arc::new(Field::new("item", DataType::Float32, true));

        // Every other row is null, leaving 50 non-null rows.
        let mut nulls_builder = BooleanBufferBuilder::new(total_rows);
        for i in 0..total_rows {
            nulls_builder.append(i % 2 == 0);
        }
        let nulls = NullBuffer::new(nulls_builder.finish());

        let fsl = FixedSizeListArray::try_new(
            item_field,
            dim as i32,
            Arc::new(Float32Array::from(values)),
            Some(nulls),
        )
        .unwrap();
        let arr: ArrayRef = Arc::new(fsl);

        let mut buf = MutableBuffer::new(0);
        let mut num_rows = 0usize;
        accumulate_fsl_values(&mut buf, &mut num_rows, &arr, byte_width, true, max_rows).unwrap();

        assert_eq!(num_rows, max_rows);
        assert_eq!(buf.len(), max_rows * byte_width);
    }

    /// Build a nullable FSL dataset with one fragment per `(total_rows,
    /// non_null_indices)` entry, controlling exactly which rows are non-null.
    async fn make_sparse_nullable_fsl_dataset(
        dim: usize,
        fragments: &[(usize, Vec<usize>)],
    ) -> Dataset {
        use arrow_array::{RecordBatch, RecordBatchIterator};
        use arrow_schema::Schema as ArrowSchema;

        let item_field = Arc::new(Field::new("item", DataType::Float32, true));
        let list_type = DataType::FixedSizeList(item_field.clone(), dim as i32);
        let schema = Arc::new(ArrowSchema::new(vec![Field::new("vec", list_type, true)]));

        let make_batch = |total: usize, non_null: &[usize]| -> RecordBatch {
            let values = Float32Array::from_iter_values((0..total * dim).map(|i| i as f32));
            let mut nulls_builder = BooleanBufferBuilder::new(total);
            for i in 0..total {
                nulls_builder.append(non_null.contains(&i));
            }
            let nulls = NullBuffer::new(nulls_builder.finish());
            let fsl = FixedSizeListArray::try_new(
                item_field.clone(),
                dim as i32,
                Arc::new(values),
                Some(nulls),
            )
            .unwrap();
            RecordBatch::try_new(schema.clone(), vec![Arc::new(fsl)]).unwrap()
        };

        let (first_total, first_non_null) = &fragments[0];
        let mut dataset = InsertBuilder::new("memory://sparse_nullable_fsl")
            .execute(vec![make_batch(*first_total, first_non_null)])
            .await
            .unwrap();
        for (total, non_null) in &fragments[1..] {
            dataset
                .append(
                    RecordBatchIterator::new(
                        vec![Ok(make_batch(*total, non_null))],
                        schema.clone(),
                    ),
                    None,
                )
                .await
                .unwrap();
        }
        dataset
    }

    /// The issue's core scenario: an extremely sparse nullable column spread
    /// across multiple fragments (one entirely null). The sampler must return
    /// every non-null row — no more, no fewer — even though far fewer non-null
    /// rows exist than the requested sample size.
    #[tokio::test]
    async fn test_maybe_sample_training_data_fsl_sparse_multi_fragment() {
        let dim = 8usize;
        let sample_size = 100usize;
        let dataset = make_sparse_nullable_fsl_dataset(
            dim,
            &[
                (3000, vec![10, 1500, 2999]),
                (3000, vec![]),
                (3000, vec![0, 42]),
            ],
        )
        .await;
        assert_eq!(dataset.get_fragments().len(), 3);

        let fragment_ids: Vec<u32> = dataset
            .get_fragments()
            .iter()
            .map(|f| f.id() as u32)
            .collect();

        let training_data =
            maybe_sample_training_data(&dataset, "vec", sample_size, Some(&fragment_ids))
                .await
                .unwrap();

        assert_eq!(training_data.len(), 5);
        assert_eq!(training_data.null_count(), 0);
        assert_eq!(training_data.value_length(), dim as i32);
    }
}
