use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{
    stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    SendableRecordBatchStream, Statistics,
};
use datafusion::scalar::ScalarValue;
use datasources::native::access::NativeTableStorage;
use futures::{stream, StreamExt};
use once_cell::sync::Lazy;
use protogen::metastore::types::catalog::TableEntry;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

pub static INSERT_COUNT_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
});

#[derive(Debug, Clone)]
pub struct InsertExec {
    pub table: TableEntry,
    pub source: Arc<dyn ExecutionPlan>,
}

impl ExecutionPlan for InsertExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        INSERT_COUNT_SCHEMA.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.source.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(InsertExec {
            table: self.table.clone(),
            source: children.get(0).unwrap().clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "InsertExec only supports 1 partition".to_string(),
            ));
        }

        let this = self.clone();
        let stream = stream::once(this.insert_native(context));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for InsertExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "InsertExec")
    }
}

impl InsertExec {
    async fn insert_native(self, context: Arc<TaskContext>) -> DataFusionResult<RecordBatch> {
        // TODO: Instead of simply inserting into a native table, try to get the
        // table provider from dispatch.
        let storage = context
            .session_config()
            .get_extension::<NativeTableStorage>()
            .expect("context should have native table storage");

        let table = storage
            .load_table(&self.table)
            .await
            .map_err(|e| DataFusionError::Execution(format!("failed to Insert: {e}")))?
            .into_table_provider();

        Self::do_insert(table, self.source, context).await

        // TODO: Add background job for storage size
    }

    pub async fn do_insert(
        table: Arc<dyn TableProvider>,
        source: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<RecordBatch> {
        let state =
            SessionState::with_config_rt(context.session_config().clone(), context.runtime_env());

        let exec = table.insert_into(&state, source).await?;

        let mut stream = exec.execute(0, context)?;

        let mut inserted_rows = 0_u64;
        while let Some(res) = stream.next().await {
            // Drain stream to write everything.
            let res = res?;
            // Each res should have the count of rows inserted.
            let count =
                datafusion::arrow::compute::cast(res.column(0).as_ref(), &DataType::UInt64)?;
            for row_idx in 0..count.len() {
                let s = ScalarValue::try_from_array(&count, row_idx)?;
                match s {
                    ScalarValue::UInt64(Some(v)) => {
                        inserted_rows += v;
                    }
                    _ => unreachable!("scalar value should be of UInt64 type"),
                };
            }
        }

        let batch = RecordBatch::try_new(
            INSERT_COUNT_SCHEMA.clone(),
            vec![Arc::new(UInt64Array::from(vec![inserted_rows]))],
        )?;

        Ok(batch)
    }
}
