pub mod alter_database;
pub mod alter_table;
pub mod alter_tunnel_rotate_keys;
pub mod client_recv;
pub mod client_send;
pub mod copy_to;
pub mod create_credential;
pub mod create_credentials;
pub mod create_external_database;
pub mod create_external_table;
pub mod create_schema;
pub mod create_table;
pub mod create_temp_table;
pub mod create_tunnel;
pub mod create_view;
pub mod delete;
pub mod describe_table;
pub mod drop_credentials;
pub mod drop_database;
pub mod drop_schemas;
pub mod drop_tables;
pub mod drop_temp_tables;
pub mod drop_tunnel;
pub mod drop_views;
pub mod insert;
pub mod remote_exec;
pub mod remote_scan;
pub mod send_recv;
pub mod set_var;
pub mod show_var;
pub mod update;
pub mod values;

use std::sync::Arc;

use datafusion::arrow::array::{StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    Partitioning,
    Statistics,
};
use datafusion::scalar::ScalarValue;
use futures::{stream, StreamExt};
use once_cell::sync::Lazy;

pub static GENERIC_OPERATION_PHYSICAL_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "$operation",
        DataType::Utf8,
        false,
    )]))
});

/// Create a new single-row record batch representing the ouptput for some
/// command (most DDL).
pub fn new_operation_batch(operation: impl Into<String>) -> RecordBatch {
    RecordBatch::try_new(
        GENERIC_OPERATION_PHYSICAL_SCHEMA.clone(),
        vec![Arc::new(StringArray::from(vec![Some(operation.into())]))],
    )
    .unwrap()
}

/// Arrow schema for dml (excluding select) output streams.
pub static GENERIC_OPERATION_AND_COUNT_PHYSICAL_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("$operation", DataType::Utf8, false),
        Field::new("$count", DataType::UInt64, false),
    ]))
});

/// Create a new single-row record batch representing the ouptput for updates
/// and deletes where count is rows affected.
pub fn new_operation_with_count_batch(operation: impl Into<String>, count: u64) -> RecordBatch {
    RecordBatch::try_new(
        GENERIC_OPERATION_AND_COUNT_PHYSICAL_SCHEMA.clone(),
        vec![
            Arc::new(StringArray::from(vec![Some(operation.into())])),
            Arc::new(UInt64Array::from(vec![count])),
        ],
    )
    .unwrap()
}

pub fn get_operation_from_batch(batch: &RecordBatch) -> Option<String> {
    if batch.columns().is_empty() {
        return None;
    }
    if let Ok(ScalarValue::Utf8(Some(val))) = ScalarValue::try_from_array(batch.column(0), 0) {
        return Some(val);
    }
    None
}

pub fn get_count_from_batch(batch: &RecordBatch) -> Option<u64> {
    if batch.columns().len() < 2 {
        return None;
    }
    if let Ok(ScalarValue::UInt64(Some(val))) = ScalarValue::try_from_array(batch.column(1), 0) {
        return Some(val);
    }
    None
}
