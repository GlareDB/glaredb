//! Casting utilies.
use datafusion::arrow::compute::kernels::cast::cast;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;

/// Cast a batch to the given schema.
///
/// The schema must contain the same number of columns as the batch.
pub fn cast_record_batch(batch: RecordBatch, schema: SchemaRef) -> Result<RecordBatch, ArrowError> {
    if batch.num_columns() != schema.fields.len() {
        return Err(ArrowError::CastError(format!(
            "number of columns mismatch, batch: {}, schema: {}",
            batch.num_columns(),
            schema.fields.len()
        )));
    }

    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields.iter())
        .map(|(col, field)| cast(col, field.data_type()))
        .collect::<Result<Vec<_>, _>>()?;
    let batch = RecordBatch::try_new(schema, columns)?;

    Ok(batch)
}
