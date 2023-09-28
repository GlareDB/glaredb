use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::display::ProjectSchemaDisplay;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    Statistics,
};
use datafusion::prelude::Expr;
use futures::{stream, TryStreamExt};
use std::any::Any;
use std::fmt;
use std::sync::Arc;
use uuid::Uuid;

/// Reference to a table provider.
///
/// During planning, the local context will only have the ID of the cached
/// provider. It will serialize that and send to the remote context. The remote
/// context will then get the actual table provider.
#[derive(Clone)]
pub enum ProviderReference {
    /// Reference to a provider that's been cached on the remote node.
    RemoteReference(Uuid),
    /// We have the provider ready to go.
    Provider(Arc<dyn TableProvider>),
}

impl fmt::Debug for ProviderReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProviderReference::RemoteReference(id) => {
                write!(f, "ProviderReference::RemoteReference({id})")
            }
            ProviderReference::Provider(_) => write!(f, "ProviderReference::Provider(<table>)"),
        }
    }
}

/// Executes a scan on a cached table provider on the remote side.
///
/// It's only valid to execute this on the remote side.
#[derive(Debug, Clone)]
pub struct RemoteScanExec {
    pub provider: ProviderReference,
    pub projected_schema: Arc<Schema>,
    pub projection: Option<Vec<usize>>,
    pub filters: Vec<Expr>,
    pub limit: Option<usize>,
}

impl ExecutionPlan for RemoteScanExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.projected_schema.clone()
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
            "Cannot replace children for RemoteScanExec".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "RemoteScanExec only supports 1 partition".to_string(),
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

        let projection = self.projection.clone();
        let filters = self.filters.clone();
        let limit = self.limit;
        let stream = stream::once(async move {
            // Recreate a session state from a task context. This is a bit hacky,
            // but all we really need to care about is the object stores in runtime
            // (I believe).
            let state = SessionState::with_config_rt(
                context.session_config().clone(),
                context.runtime_env(),
            );

            // TODO: We probably want to store the plan instead of
            // unconditionally creating a new one.
            let plan = provider
                .scan(&state, projection.as_ref(), &filters, limit)
                .await?;

            // NOTE: RemoteScanExec can only have 1 partition since we don't
            // know about the paritions until we "execute" the remote table
            // which happens, unfortunately, during execution here (see above).
            // Hence, to execute the complete plan we need to coalesce the plan.
            let plan = CoalescePartitionsExec::new(plan);
            plan.execute(0, context)
        })
        .try_flatten();

        let schema = self.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for RemoteScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RemoteScanExec: projection={}",
            ProjectSchemaDisplay(&self.projected_schema)
        )?;
        Ok(())
    }
}
