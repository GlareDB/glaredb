use crate::errors::Result;
use datafusion::arrow::datatypes::Schema;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_ext::functions::FuncParamValue;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use protogen::metastore::types::catalog::CatalogState;
use protogen::rpcsrv::types::service::ResolvedTableReference;
use sqlexec::context::remote::RemoteSessionContext;
use sqlexec::remote::batch_stream::ExecutionBatchStream;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// A wrapper around a remote session context for physical plan execution.
#[derive(Clone)]
pub struct RemoteSession {
    /// Inner context.
    ///
    /// Wrapped in an Arc and Mutex since the lifetime of the session is not
    /// tied to a single connection, and so needs to be tracked in a shared map.
    session: Arc<RemoteSessionContext>,
}

impl RemoteSession {
    pub fn new(context: RemoteSessionContext) -> Self {
        RemoteSession {
            session: Arc::new(context),
        }
    }

    /// Get the catalog state suitable for sending back to the requesting
    /// session.
    pub async fn get_refreshed_catalog_state(&self) -> Result<CatalogState> {
        self.session.refresh_catalog().await?;
        Ok(self
            .session
            .get_session_catalog()
            .get_state()
            .as_ref()
            .clone())
    }

    pub async fn dispatch_access(
        &self,
        table_ref: ResolvedTableReference,
        args: Option<Vec<FuncParamValue>>,
        opts: Option<HashMap<String, FuncParamValue>>,
    ) -> Result<(Uuid, Schema)> {
        let (id, prov) = self
            .session
            .load_and_cache_table(table_ref, args, opts)
            .await?;
        let schema = prov.schema().as_ref().clone();

        Ok((id, schema))
    }

    pub async fn physical_plan_execute(
        &self,
        physical_plan: impl AsRef<[u8]>,
    ) -> Result<SendableRecordBatchStream> {
        let codec = self.session.extension_codec();
        let plan = PhysicalPlanNode::try_decode(physical_plan.as_ref())?.try_into_physical_plan(
            self.session.get_datafusion_context(),
            self.session.get_datafusion_context().runtime_env().as_ref(),
            &codec,
        )?;

        let stream = self.session.execute_physical(plan)?;
        Ok(stream)
    }

    pub async fn register_broadcast_stream(&self, stream: ExecutionBatchStream) -> Result<()> {
        let streams = self.session.staged_streams();
        streams.put_stream(stream.work_id(), stream);
        Ok(())
    }
}
