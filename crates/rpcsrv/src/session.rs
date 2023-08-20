use crate::errors::{Result, RpcsrvError};
use datafusion::arrow::datatypes::Schema;
use datafusion::common::{OwnedTableReference, TableReference};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use protogen::metastore::types::catalog::CatalogState;
use sqlexec::context::remote::RemoteSessionContext;
use sqlexec::remote::exchange_stream::ClientExchangeRecvStream;
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
}

impl RemoteSession {
    pub fn new(context: RemoteSessionContext) -> Self {
        RemoteSession {
            session: Arc::new(Mutex::new(context)),
        }
    }

    /// Get the catalog state suitable for sending back to the requesting
    /// session.
    pub async fn get_catalog_state(&self) -> CatalogState {
        let session = self.session.lock().await;
        session.get_session_catalog().get_state().as_ref().clone()
    }

    pub async fn dispatch_access(&self, table_ref: OwnedTableReference) -> Result<(Uuid, Schema)> {
        let (db, schema, name) = match table_ref {
            TableReference::Bare { .. } | TableReference::Partial { .. } => {
                return Err(RpcsrvError::Internal(
                    "Full table reference required".to_string(),
                ));
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => (catalog, schema, table),
        };

        let mut session = self.session.lock().await;
        let (id, prov) = session.load_and_cache_table(&db, &schema, &name).await?;
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
