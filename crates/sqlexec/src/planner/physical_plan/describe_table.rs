use super::*;
use std::{any::Any, sync::Arc};

// use arrow_util::pretty::fmt_dtype;
use datafusion::{
    arrow::{
        array::{BooleanBuilder, StringBuilder},
        record_batch::RecordBatch,
    },
    error::DataFusionError,
    error::Result,
    execution::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan,
        Partitioning, SendableRecordBatchStream, Statistics,
    },
};
use futures::stream;
use protogen::metastore::types::catalog::TableEntry;

use crate::planner::{errors::PlanError, logical_plan::DESCRIBE_TABLE_SCHEMA};

#[derive(Debug, Clone)]
pub struct DescribeTableExec {
    pub entry: TableEntry,
}

impl DisplayAs for DescribeTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DescribeTableExec")
    }
}

impl ExecutionPlan for DescribeTableExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        DESCRIBE_TABLE_SCHEMA.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "Cannot change children for DescribeTableExec".to_string(),
        ))
    }

    fn execute(&self, _: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let entry = self.entry.clone();
        let stream = stream::once(async move {
            let internal_cols = match entry.get_internal_columns() {
                Some(cols) => cols,
                None => {
                    return Err(PlanError::UnsupportedFeature(
                        "'DESCRIBE' not yet supported for external tables",
                    )
                    .into())
                }
            };

            let mut column_names = StringBuilder::new();
            let mut data_types = StringBuilder::new();
            let mut is_nullables = BooleanBuilder::new();

            for col in internal_cols {
                let name = col.name.clone();
                let data_type = col.arrow_type.clone();

                column_names.append_value(name);

                unimplemented!();
                // data_types.append_value(fmt_dtype(&data_type));

                is_nullables.append_value(col.nullable);
            }

            let output_schema = DESCRIBE_TABLE_SCHEMA.clone();

            let record_batch = RecordBatch::try_new(
                output_schema,
                vec![
                    Arc::new(column_names.finish()),
                    Arc::new(data_types.finish()),
                    Arc::new(is_nullables.finish()),
                ],
            )?;
            Ok(record_batch)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}
