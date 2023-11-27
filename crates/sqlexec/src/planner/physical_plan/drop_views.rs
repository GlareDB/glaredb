use crate::planner::logical_plan::OwnedFullObjectReference;
use catalog::session_catalog::CatalogMutator;
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
pub struct DropViewsExec {
    pub catalog_version: u64,
    pub view_references: Vec<OwnedFullObjectReference>,
    pub if_exists: bool,
}

impl ExecutionPlan for DropViewsExec {
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
            "Cannot change children for DropViewsExec".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "DropViewsExec only supports 1 partition".to_string(),
            ));
        }

        let mutator = context
            .session_config()
            .get_extension::<CatalogMutator>()
            .expect("context should have catalog mutator");

        let stream = stream::once(drop_views(mutator, self.clone()));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for DropViewsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DropViewsExec")
    }
}

async fn drop_views(
    mutator: Arc<CatalogMutator>,
    plan: DropViewsExec,
) -> DataFusionResult<RecordBatch> {
    let drops: Vec<_> = plan
        .view_references
        .into_iter()
        .map(|r| {
            Mutation::DropObject(service::DropObject {
                name: r.name.into_owned(),
                schema: r.schema.into_owned(),
                if_exists: plan.if_exists,
            })
        })
        .collect();

    mutator
        .mutate(plan.catalog_version, drops)
        .await
        .map_err(|e| DataFusionError::Execution(format!("failed to drop views: {e}")))?;

    Ok(new_operation_batch("drop_views"))
}
