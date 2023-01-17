use crate::catalog::Catalog;
use crate::errors::Result;
use crate::metastore::Supervisor;
use crate::session::Session;
use metastore::proto::service::metastore_service_client::MetastoreServiceClient;
use metastore::session::SessionCatalog;
use std::sync::Arc;
use tonic::transport::Channel;
use uuid::Uuid;

/// Wrapper around the database catalog.
pub struct Engine {
    supervisor: Supervisor,
}

impl Engine {
    /// Create a new engine using the provided access runtime.
    pub async fn new(metastore: MetastoreServiceClient<Channel>) -> Result<Engine> {
        Ok(Engine {
            supervisor: Supervisor::new(metastore),
        })
    }

    /// Create a new session with the given id.
    pub async fn new_session(&self, conn_id: Uuid, db_id: Uuid) -> Result<Session> {
        let metastore = self.supervisor.init_client(conn_id, db_id).await?;

        let state = metastore.get_cached_state().await?;
        let catalog = SessionCatalog::new(state);

        let session = Session::new(conn_id, catalog, metastore)?;
        Ok(session)
    }
}
