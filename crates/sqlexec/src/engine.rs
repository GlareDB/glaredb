use crate::errors::Result;
use crate::metastore::Supervisor;
use crate::session::Session;
use metastore::proto::service::metastore_service_client::MetastoreServiceClient;
use metastore::session::SessionCatalog;
use std::path::PathBuf;
use std::sync::Arc;
use telemetry::Tracker;
use tonic::transport::Channel;
use uuid::Uuid;

/// Static information for database sessions.
#[derive(Debug, Clone)]
pub struct SessionInfo {
    /// Database id that this session is connected to.
    pub database_id: Uuid,
    // Name of the database we're connected to.
    pub database_name: String,
    /// ID of the user who initiated the connection. This maps to a user account
    /// on Cloud.
    pub user_id: Uuid,
    /// Name of the (database) user.
    pub user_name: String,
    /// Unique connection id.
    pub conn_id: Uuid,
    // Max datasource count allowed.
    pub max_datasource_count: usize,
    // Memory limit applied to the session.
    pub memory_limit_bytes: usize,
}

/// Wrapper around the database catalog.
pub struct Engine {
    supervisor: Supervisor,
    tracker: Arc<Tracker>,
    spill_path: Option<PathBuf>,
}

impl Engine {
    /// Create a new engine using the provided access runtime.
    pub async fn new(
        metastore: MetastoreServiceClient<Channel>,
        tracker: Arc<Tracker>,
        spill_path: Option<PathBuf>,
    ) -> Result<Engine> {
        Ok(Engine {
            supervisor: Supervisor::new(metastore),
            tracker,
            spill_path,
        })
    }

    /// Create a new session with the given id.
    #[allow(clippy::too_many_arguments)]
    pub async fn new_session(
        &self,
        user_id: Uuid,
        user_name: String,
        conn_id: Uuid,
        database_id: Uuid,
        database_name: String,
        max_datasource_count: usize,
        memory_limit_bytes: usize,
    ) -> Result<Session> {
        let metastore = self.supervisor.init_client(conn_id, database_id).await?;

        let info = Arc::new(SessionInfo {
            database_id,
            database_name,
            user_id,
            user_name,
            conn_id,
            max_datasource_count,
            memory_limit_bytes,
        });

        let state = metastore.get_cached_state().await?;
        let catalog = SessionCatalog::new(state);

        let session = Session::new(
            info,
            catalog,
            metastore,
            self.tracker.clone(),
            self.spill_path.clone(),
        )?;
        Ok(session)
    }
}
