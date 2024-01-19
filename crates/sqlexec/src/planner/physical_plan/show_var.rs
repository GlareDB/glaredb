use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{
    stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    SendableRecordBatchStream, Statistics,
};
use datafusion_ext::vars::SessionVars;
use futures::stream;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

pub fn create_show_var_schema(var: impl Into<String>) -> Schema {
    Schema::new(vec![Field::new(var, DataType::Utf8, false)])
}

#[derive(Debug, Clone)]
pub struct ShowVarExec {
    pub variable: String,
}

impl ExecutionPlan for ShowVarExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::new(create_show_var_schema(&self.variable))
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
            "cannot change children for ShowVarExec".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "ShowVarExec only supports 1 partition".to_string(),
            ));
        }

        let this = self.clone();
        let stream = stream::once(async move {
            let vars = context
                .session_config()
                .options()
                .extensions
                .get::<SessionVars>()
                .expect("context should have SessionVars extension");
            let values = vars.read().get(&this.variable)?.formatted_value();
            let values = StringArray::from_iter_values([values]);
            Ok(RecordBatch::try_new(this.schema(), vec![Arc::new(values)])?)
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

impl DisplayAs for ShowVarExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ShowVarExec")
    }
}
