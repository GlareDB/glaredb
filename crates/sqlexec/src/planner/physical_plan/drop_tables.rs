use crate::planner::logical_plan::OwnedFullObjectReference;

use super::{new_operation_batch, GENERIC_OPERATION_PHYSICAL_SCHEMA};
use catalog::mutator::CatalogMutator;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{
    stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    SendableRecordBatchStream, Statistics,
};
use futures::{stream, StreamExt};
use protogen::metastore::types::catalog::TableEntry;
use protogen::metastore::types::service::{self, Mutation};
use sqlbuiltins::functions::table::system::remove_delta_tables::DeleteDeltaTablesOperation;
use sqlbuiltins::functions::table::system::SystemOperationExec;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DropTablesExec {
    pub catalog_version: u64,
    pub tbl_references: Vec<OwnedFullObjectReference>,
    pub tbl_entries: Vec<TableEntry>,
    pub if_exists: bool,
}

impl ExecutionPlan for DropTablesExec {
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
            "Cannot change children for DropTablesExec".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "DropTablesExec only supports 1 partition".to_string(),
            ));
        }

        let stream = stream::once(drop_tables(context, self.clone()));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for DropTablesExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DropTablesExec")
    }
}

async fn drop_tables(
    context: Arc<TaskContext>,
    plan: DropTablesExec,
) -> DataFusionResult<RecordBatch> {
    let mutator = context
        .session_config()
        .get_extension::<CatalogMutator>()
        .expect("context should have catalog mutator");

    let drops = plan.tbl_references.into_iter().map(|r| {
        Mutation::DropObject(service::DropObject {
            schema: r.schema.into_owned(),
            name: r.name.into_owned(),
            if_exists: plan.if_exists,
        })
    });

    // we want to make sure that the catalog is updated before we delete the delta tables
    mutator
        .mutate(plan.catalog_version, drops)
        .await
        .map_err(|e| DataFusionError::Execution(format!("failed to drop tables: {e}")))?;

    // only after the catalog is updated, we can delete the delta tables
    // TODO: this should be done in the scheduler.
    let sys_exec =
        SystemOperationExec::new(DeleteDeltaTablesOperation::new(plan.tbl_entries.clone()).into());
    let _ = sys_exec
        .execute(0, context.clone())?
        .collect::<Vec<_>>()
        .await;

    Ok(new_operation_batch("drop_tables"))
}
