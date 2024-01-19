use std::any::Any;
use std::fmt;
use std::sync::Arc;

use catalog::mutator::CatalogMutator;
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
use protogen::metastore::types::service::{self, Mutation};

use super::{new_operation_batch, GENERIC_OPERATION_PHYSICAL_SCHEMA};
use crate::planner::logical_plan::OwnedFullObjectReference;

#[derive(Debug, Clone)]
pub struct DropTablesExec {
    pub catalog_version: u64,
    pub tbl_references: Vec<OwnedFullObjectReference>,
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

        let mutator = context
            .session_config()
            .get_extension::<CatalogMutator>()
            .expect("context should have catalog mutator");

        let stream = stream::once(drop_tables(mutator, self.clone()));

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
    mutator: Arc<CatalogMutator>,
    plan: DropTablesExec,
) -> DataFusionResult<RecordBatch> {
    let drops = plan.tbl_references.into_iter().map(|r| {
        Mutation::DropObject(service::DropObject {
            schema: r.schema.into_owned(),
            name: r.name.into_owned(),
            if_exists: plan.if_exists,
        })
    });

    mutator
        .mutate(plan.catalog_version, drops)
        .await
        .map_err(|e| DataFusionError::Execution(format!("failed to drop tables: {e}")))?;

    // // Run background jobs _after_ tables get removed from the catalog.
    // //
    // // TODO: If/when we have transactions, background jobs should be stored
    // // on the session until transaction commit.
    // self.background_jobs.add_many(jobs)?;

    Ok(new_operation_batch("drop_tables"))
}
