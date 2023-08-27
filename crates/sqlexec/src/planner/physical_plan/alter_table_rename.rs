use crate::metastore::catalog::CatalogMutator;
use crate::planner::logical_plan::OwnedFullObjectReference;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{
    stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    SendableRecordBatchStream, Statistics,
};
use futures::stream;
use protogen::metastore::types::service::{self, Mutation};
use std::any::Any;
use std::fmt;
use std::sync::Arc;

use super::{new_operation_batch, GENERIC_OPERATION_PHYSICAL_SCHEMA};

#[derive(Debug, Clone)]
pub struct AlterTableRenameExec {
    pub catalog_version: u64,
    pub reference: OwnedFullObjectReference,
    pub new_reference: OwnedFullObjectReference,
}

impl ExecutionPlan for AlterTableRenameExec {
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
            "Cannot change children for AlterTableRenameExec".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "AlterTableRenameExec only supports 1 partition".to_string(),
            ));
        }

        let mutator = context
            .session_config()
            .get_extension::<CatalogMutator>()
            .expect("context should have catalog mutator");

        let stream = stream::once(alter_table_rename(mutator, self.clone()));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for AlterTableRenameExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AlterTableRenameExec")
    }
}

async fn alter_table_rename(
    mutator: Arc<CatalogMutator>,
    plan: AlterTableRenameExec,
) -> DataFusionResult<RecordBatch> {
    // TODO: Error if schemas between references differ.
    mutator
        .mutate(
            plan.catalog_version,
            [Mutation::AlterTableRename(service::AlterTableRename {
                name: plan.reference.name.into_owned(),
                new_name: plan.new_reference.name.into_owned(),
                schema: plan.reference.schema.into_owned(),
            })],
        )
        .await
        .map_err(|e| DataFusionError::Execution(format!("failed to rename table: {e}")))?;

    Ok(new_operation_batch("alter_table_rename"))
}
