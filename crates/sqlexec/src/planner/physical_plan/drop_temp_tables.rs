use std::any::Any;
use std::fmt;
use std::sync::Arc;

use catalog::session_catalog::TempCatalog;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
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
use futures::stream;

use super::{new_operation_batch, GENERIC_OPERATION_PHYSICAL_SCHEMA};
use crate::planner::logical_plan::OwnedFullObjectReference;

#[derive(Debug, Clone)]
pub struct DropTempTablesExec {
    #[allow(dead_code)]
    pub catalog_version: u64,
    pub tbl_references: Vec<OwnedFullObjectReference>,

    #[allow(dead_code)]
    pub if_exists: bool,
}

impl ExecutionPlan for DropTempTablesExec {
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Plan(
                "Cannot change children for DropTempTablesExec".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "DropTempTablesExec only supports 1 partition".to_string(),
            ));
        }
        let stream = stream::once(drop_tables(self.clone(), context));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }
}

impl DisplayAs for DropTempTablesExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DropTempTablesExec")
    }
}

async fn drop_tables(
    plan: DropTempTablesExec,
    context: Arc<TaskContext>,
) -> DataFusionResult<RecordBatch> {
    let temp_objects = context
        .session_config()
        .get_extension::<TempCatalog>()
        .unwrap();

    for temp_table in plan.tbl_references {
        temp_objects.drop_table(&temp_table.name);
    }

    Ok(new_operation_batch("drop_temp_tables"))
}
