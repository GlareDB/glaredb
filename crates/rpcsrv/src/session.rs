use crate::errors::Result;
use datafusion::arrow::datatypes::Schema;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_ext::functions::FuncParamValue;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use protogen::metastore::types::catalog::CatalogState;
use protogen::rpcsrv::types::service::ResolvedTableReference;
use sqlexec::context::remote::RemoteSessionContext;
use sqlexec::remote::exchange_stream::ClientExchangeRecvStream;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

/// A wrapper around a remote session context for physical plan execution.
#[derive(Clone)]
pub struct RemoteSession {
    /// Inner context.
    ///
    /// Wrapped in an Arc and Mutex since the lifetime of the session is not
    /// tied to a single connection, and so needs to be tracked in a shared map.
    session: Arc<Mutex<RemoteSessionContext>>,

    /// Database this context is for.
    database_id: Uuid,
}

impl RemoteSession {
    pub fn new(context: RemoteSessionContext, db_id: Uuid) -> Self {
        RemoteSession {
            session: Arc::new(Mutex::new(context)),
            database_id: db_id,
        }
    }

    pub fn get_db_id(&self) -> &Uuid {
        &self.database_id
    }

    /// Get the catalog state suitable for sending back to the requesting
    /// session.
    pub async fn get_refreshed_catalog_state(&self) -> Result<CatalogState> {
        let mut session = self.session.lock().await;
        session.refresh_catalog().await?;
        Ok(session.get_session_catalog().get_state().as_ref().clone())
    }

    pub async fn dispatch_access(
        &self,
        table_ref: ResolvedTableReference,
        args: Option<Vec<FuncParamValue>>,
        opts: Option<HashMap<String, FuncParamValue>>,
    ) -> Result<(Uuid, Schema)> {
        let mut session = self.session.lock().await;
        let (id, prov) = session.load_and_cache_table(table_ref, args, opts).await?;
        let schema = prov.schema().as_ref().clone();

        Ok((id, schema))
    }

    pub async fn physical_plan_execute(
        &self,
        physical_plan: impl AsRef<[u8]>,
    ) -> Result<SendableRecordBatchStream> {
        let session = self.session.lock().await;

        let codec = session.extension_codec();
        let plan = PhysicalPlanNode::try_decode(physical_plan.as_ref())?.try_into_physical_plan(
            session.get_datafusion_context(),
            session.get_datafusion_context().runtime_env().as_ref(),
            &codec,
        )?;

        let stream = session.execute_physical(plan)?;
        Ok(stream)
    }

    pub async fn register_broadcast_stream(&self, stream: ClientExchangeRecvStream) -> Result<()> {
        let session = self.session.lock().await;
        let streams = session.staged_streams();
        streams.put_stream(stream.broadcast_id(), stream);
        Ok(())
    }
}
