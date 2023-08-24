use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{
    stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    SendableRecordBatchStream, Statistics,
};
use datafusion::prelude::Expr;
use datasources::native::access::NativeTableStorage;
use futures::stream;
use protogen::metastore::types::catalog::TableEntry;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct UpdateExec {
    pub table: TableEntry,
    pub updates: Vec<(String, Expr)>,
    pub where_expr: Option<Expr>,
}

impl ExecutionPlan for UpdateExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]))
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
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "Cannot change children for UpdateExec".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "UpdateExec only supports 1 partition".to_string(),
            ));
        }

        let storage = context
            .session_config()
            .get_extension::<NativeTableStorage>()
            .expect("context should have native table storage");

        let stream = stream::once(update(self.clone(), storage));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for UpdateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UpdateExec")
    }
}

async fn update(
    plan: UpdateExec,
    storage: impl AsRef<NativeTableStorage>,
) -> DataFusionResult<RecordBatch> {
    let storage = storage.as_ref();

    let schema = plan.schema();
    let num_updated = storage
        .update_rows_where(&plan.table, plan.updates, plan.where_expr)
        .await
        .map_err(|e| DataFusionError::Execution(format!("failed to update: {e}")))?;

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(UInt64Array::from(vec![num_updated as u64]))],
    )?;

    Ok(batch)
}
