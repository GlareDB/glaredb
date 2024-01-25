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
use futures::stream;
use protogen::metastore::types::options::TunnelOptions;
use protogen::metastore::types::service::{self, Mutation};
use std::any::Any;
use std::fmt;
use std::sync::Arc;

use super::{new_operation_batch, GENERIC_OPERATION_PHYSICAL_SCHEMA};

#[derive(Debug, Clone)]
pub struct CreateTunnelExec {
    pub catalog_version: u64,
    pub name: String,
    pub if_not_exists: bool,
    pub options: TunnelOptions,
}

impl ExecutionPlan for CreateTunnelExec {
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
            "Cannot change children for CreateTunnelExec".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "CreateTunnelExec only supports 1 partition".to_string(),
            ));
        }

        let mutator = context
            .session_config()
            .get_extension::<CatalogMutator>()
            .expect("context should have catalog mutator");

        let stream = stream::once(create_tunnel(mutator, self.clone()));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }
}

impl DisplayAs for CreateTunnelExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CreateTunnelExec")
    }
}

async fn create_tunnel(
    mutator: Arc<CatalogMutator>,
    plan: CreateTunnelExec,
) -> DataFusionResult<RecordBatch> {
    mutator
        .mutate(
            plan.catalog_version,
            [Mutation::CreateTunnel(service::CreateTunnel {
                name: plan.name,
                if_not_exists: plan.if_not_exists,
                options: plan.options,
            })],
        )
        .await
        .map_err(|e| DataFusionError::Execution(format!("failed to create tunnel: {e}")))?;

    Ok(new_operation_batch("create_tunnel"))
}
