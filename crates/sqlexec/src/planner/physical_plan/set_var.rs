use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    Partitioning,
    SendableRecordBatchStream,
    Statistics,
};
use datafusion::variable::VarType;
use datafusion_ext::vars::SessionVars;
use futures::stream;

use super::{new_operation_batch, GENERIC_OPERATION_PHYSICAL_SCHEMA};

#[derive(Debug, Clone)]
pub struct SetVarExec {
    pub variable: String,
    pub values: String,
}

impl ExecutionPlan for SetVarExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        GENERIC_OPERATION_PHYSICAL_SCHEMA.clone()
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
            "cannot change children for SetVarExec".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "SetVarExec only supports 1 partition".to_string(),
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

            let mut vars = vars.write();
            vars.set(&this.variable, &this.values, VarType::UserDefined)?;

            Ok(new_operation_batch("set"))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }
}

impl DisplayAs for SetVarExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SetVarExec")
    }
}
