use crate::errors::Result;
use crate::metastore::Supervisor;
use crate::session::Session;
use metastore::proto::service::metastore_service_client::MetastoreServiceClient;
use metastore::session::SessionCatalog;
use std::sync::Arc;
use telemetry::Tracker;
use tonic::transport::Channel;
use uuid::Uuid;

/// Static information for database sessions.
#[derive(Debug, Clone)]
pub struct SessionInfo {
    /// Database id that this session is connected to.
    pub database_id: Uuid,
    pub database_id_string: String,
    /// ID of the user who initiated the connection.
    pub user_id: Uuid,
    pub user_id_string: String,
    /// Unique connection id.
    pub conn_id: Uuid,
    pub conn_id_string: String,
    // TODO: Limits go here.
}

impl SessionInfo {
    fn new(database_id: Uuid, user_id: Uuid, conn_id: Uuid) -> SessionInfo {
        SessionInfo {
            database_id,
            database_id_string: database_id.to_string(),
            user_id,
            user_id_string: user_id.to_string(),
            conn_id,
            conn_id_string: conn_id.to_string(),
        }
    }
}

/// Wrapper around the database catalog.
pub struct Engine {
    supervisor: Supervisor,
    tracker: Arc<Tracker>,
}

impl Engine {
    /// Create a new engine using the provided access runtime.
    pub async fn new(
        metastore: MetastoreServiceClient<Channel>,
        tracker: Arc<Tracker>,
    ) -> Result<Engine> {
        Ok(Engine {
            supervisor: Supervisor::new(metastore),
            tracker,
        })
    }

    /// Create a new session with the given id.
    pub async fn new_session(
        &self,
        user_id: Uuid,
        conn_id: Uuid,
        database_id: Uuid,
    ) -> Result<Session> {
        let metastore = self.supervisor.init_client(conn_id, database_id).await?;

        let info = Arc::new(SessionInfo::new(database_id, user_id, conn_id));

        let state = metastore.get_cached_state().await?;
        let catalog = SessionCatalog::new(state);

        let session = Session::new(info, catalog, metastore, self.tracker.clone())?;
        Ok(session)
    }
}
