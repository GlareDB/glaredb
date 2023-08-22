use core::fmt;
use std::sync::Arc;

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    error::DataFusionError,
    execution::context::SessionState,
    physical_plan::{
        coalesce_partitions::CoalescePartitionsExec, stream::RecordBatchStreamAdapter, DisplayAs,
        DisplayFormatType, ExecutionPlan,
    },
};
use futures::{stream, TryStreamExt};
use once_cell::sync::Lazy;
use uuid::Uuid;

use super::{client_recv::ClientExchangeRecvExec, remote_scan::ProviderReference};

pub static INSERT_PLAN_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        /* nullable = */ false,
    )]))
});

/// Executes an insert on the cached table provider on remote side.
///
/// Only valid to execute this on remote side.
#[derive(Debug, Clone)]
pub struct RemoteInsertExec {
    pub provider: ProviderReference,
    pub input: Arc<ClientExchangeRecvExec>,
}

impl RemoteInsertExec {
    pub fn from_broadcast(broadcast_id: Uuid, provider_ref: ProviderReference) -> Self {
        Self {
            provider: provider_ref,
            input: Arc::new(ClientExchangeRecvExec {
                broadcast_id,
                schema: INSERT_PLAN_SCHEMA.clone(),
            }),
        }
    }
}

impl ExecutionPlan for RemoteInsertExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        INSERT_PLAN_SCHEMA.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "Cannot replace children for RemoteInsertExec".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "cannot execute RemoteInsertExec for more than 1 partition".to_string(),
            ));
        }

        let provider = match &self.provider {
            ProviderReference::Provider(p) => p.clone(),
            ProviderReference::RemoteReference(_) => {
                return Err(DataFusionError::Execution(
                    "Cannot execute remote scan with a remote reference".to_string(),
                ))
            }
        };

        let input = self.input.clone();
        let stream = stream::once(async move {
            let state = SessionState::with_config_rt(
                context.session_config().clone(),
                context.runtime_env(),
            );

            let physical = provider.insert_into(&state, input).await?;
            let physical: Arc<dyn ExecutionPlan> =
                match physical.output_partitioning().partition_count() {
                    1 => physical,
                    _ => Arc::new(CoalescePartitionsExec::new(physical)),
                };
            physical.execute(0, context)
        })
        .try_flatten();

        let schema = self.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn statistics(&self) -> datafusion::physical_plan::Statistics {
        Default::default()
    }
}

impl DisplayAs for RemoteInsertExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RemoteInsertExec")
    }
}
