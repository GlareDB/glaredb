use crate::errors::Result;
use crate::metastore::{Supervisor, DEFAULT_WORKER_CONFIG};
use crate::session::Session;
use metastore::proto::service::metastore_service_client::MetastoreServiceClient;
use metastore::session::SessionCatalog;
use pushexec::runtime::ExecRuntime;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use telemetry::Tracker;
use tonic::transport::Channel;
use tracing::debug;
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
    pub limits: SessionLimits,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SessionLimits {
    // Max datasource count allowed.
    pub max_datasource_count: Option<usize>,
    // Memory limit applied to the session.
    pub memory_limit_bytes: Option<usize>,
    // Max tunnel count allowed.
    pub max_tunnel_count: Option<usize>,
}

/// Wrapper around the database catalog.
pub struct Engine {
    exec_runtime: ExecRuntime,
    /// Metastore client supervisor.
    supervisor: Supervisor,
    /// Telemetry.
    tracker: Arc<Tracker>,
    /// Path to spill temp files.
    spill_path: Option<PathBuf>,
    /// Number of active sessions.
    session_counter: Arc<AtomicU64>,
}

impl Engine {
    /// Create a new engine using the provided access runtime.
    pub async fn new(
        exec_runtime: ExecRuntime,
        metastore: MetastoreServiceClient<Channel>,
        tracker: Arc<Tracker>,
        spill_path: Option<PathBuf>,
    ) -> Result<Engine> {
        Ok(Engine {
            exec_runtime,
            supervisor: Supervisor::new(metastore, DEFAULT_WORKER_CONFIG),
            tracker,
            spill_path,
            session_counter: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Get the current number of sessions.
    pub fn session_count(&self) -> u64 {
        self.session_counter.load(Ordering::Relaxed)
    }

    pub fn exec_runtime(&self) -> &ExecRuntime {
        &self.exec_runtime
    }

    /// Create a new session with the given id.
    pub async fn new_session(
        &self,
        user_id: Uuid,
        user_name: String,
        conn_id: Uuid,
        database_id: Uuid,
        database_name: String,
        limits: SessionLimits,
    ) -> Result<TrackedSession> {
        let metastore = self.supervisor.init_client(conn_id, database_id).await?;

        let info = Arc::new(SessionInfo {
            database_id,
            database_name,
            user_id,
            user_name,
            conn_id,
            limits,
        });

        let state = metastore.get_cached_state().await?;
        let catalog = SessionCatalog::new(state);

        let session = Session::new(
            self.exec_runtime.clone(),
            info,
            catalog,
            metastore,
            self.tracker.clone(),
            self.spill_path.clone(),
        )?;

        let prev = self.session_counter.fetch_add(1, Ordering::Relaxed);
        debug!(session_count = prev + 1, "new session opened");

        Ok(TrackedSession {
            inner: session,
            session_counter: self.session_counter.clone(),
        })
    }
}

/// A thin wrapper around a session.
pub struct TrackedSession {
    inner: Session,
    session_counter: Arc<AtomicU64>,
}

impl Deref for TrackedSession {
    type Target = Session;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for TrackedSession {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Drop for TrackedSession {
    fn drop(&mut self) {
        let prev = self.session_counter.fetch_sub(1, Ordering::Relaxed);
        debug!(session_counter = prev - 1, "session closed");
    }
}
