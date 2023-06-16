use crate::errors::{ExecError, Result};
use crate::metastore::{Supervisor, DEFAULT_WORKER_CONFIG};
use crate::session::Session;
use datasources::native::access::NativeTableStorage;
use metastoreproto::proto::service::metastore_service_client::MetastoreServiceClient;
use metastoreproto::session::SessionCatalog;
use object_store_util::conf::StorageConfig;
use std::fs;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use telemetry::Tracker;
use tonic::transport::Channel;
use tracing::{debug, info};
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
    /// Limits that should be applied for this session.
    pub limits: SessionLimits,
    /// Storage for this session.
    pub storage: SessionStorageConfig,
}

#[derive(Debug, Clone, Default)]
pub struct SessionStorageConfig {
    /// The bucket that should be used for database storage for a session.
    ///
    /// If this is omitted, the engine storage config should either be set to
    /// local or in-memory.
    pub gcs_bucket: Option<String>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SessionLimits {
    /// Max datasource count allowed.
    pub max_datasource_count: Option<usize>,
    /// Memory limit applied to the session.
    pub memory_limit_bytes: Option<usize>,
    /// Max tunnel count allowed.
    pub max_tunnel_count: Option<usize>,
    /// Max credentials count.
    pub max_credentials_count: Option<usize>,
}

/// Storage configuration for the compute engine.
///
/// The configuration defined here alongside the configuration passed in through
/// the proxy will be used to connect to database storage.
#[derive(Debug, Clone)]
pub enum EngineStorageConfig {
    Gcs { service_account_key: String },
    Local { path: PathBuf },
    Memory,
}

impl EngineStorageConfig {
    /// Create a native table storage config from values in the engine and
    /// session configs.
    ///
    /// Errors if the engine config is incompatible with the session config.
    fn storage_config(&self, session_conf: &SessionStorageConfig) -> Result<StorageConfig> {
        Ok(match (self.clone(), session_conf.gcs_bucket.clone()) {
            // GCS bucket storage.
            (
                EngineStorageConfig::Gcs {
                    service_account_key,
                },
                Some(bucket),
            ) => StorageConfig::Gcs {
                service_account_key,
                bucket,
            },
            // Expected gcs config opts for the session.
            (EngineStorageConfig::Gcs { .. }, None) => {
                return Err(ExecError::InvalidStorageConfig(
                    "Missing bucket on session configuration",
                ))
            }
            // Local disk storage.
            (EngineStorageConfig::Local { path }, None) => StorageConfig::Local { path },
            // In-memory storage.
            (EngineStorageConfig::Memory, None) => StorageConfig::Memory,
            // Bucket provided on session, but engine not configured to use it.
            (_, Some(_)) => {
                return Err(ExecError::InvalidStorageConfig(
                    "Engine not configured for GCS table storage",
                ))
            }
        })
    }

    /// Create a new native tables storage for a session for a given database.
    fn new_native_tables_storage(
        &self,
        db_id: Uuid,
        session_conf: &SessionStorageConfig,
    ) -> Result<NativeTableStorage> {
        let conf = self.storage_config(session_conf)?;
        let native = NativeTableStorage::from_config(db_id, conf)?;
        Ok(native)
    }
}

/// Hold configuration and clients needed to create database sessions.
pub struct Engine {
    /// Metastore client supervisor.
    supervisor: Supervisor,
    /// Telemetry.
    tracker: Arc<Tracker>,
    /// Storage configuration.
    storage: EngineStorageConfig,
    /// Path to spill temp files.
    spill_path: Option<PathBuf>,
    /// Number of active sessions.
    session_counter: Arc<AtomicU64>,
}

impl Engine {
    /// Create a new engine using the provided access runtime.
    pub async fn new(
        metastore: MetastoreServiceClient<Channel>,
        storage: EngineStorageConfig,
        tracker: Arc<Tracker>,
        spill_path: Option<PathBuf>,
    ) -> Result<Engine> {
        Ok(Engine {
            supervisor: Supervisor::new(metastore, DEFAULT_WORKER_CONFIG),
            tracker,
            storage,
            spill_path,
            session_counter: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Get the current number of sessions.
    pub fn session_count(&self) -> u64 {
        self.session_counter.load(Ordering::Relaxed)
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
        limits: SessionLimits,
        storage: SessionStorageConfig,
    ) -> Result<TrackedSession> {
        let metastore = self.supervisor.init_client(conn_id, database_id).await?;
        let native = self
            .storage
            .new_native_tables_storage(database_id, &storage)?;

        let info = Arc::new(SessionInfo {
            database_id,
            database_name,
            user_id,
            user_name,
            conn_id,
            limits,
            storage,
        });

        let state = metastore.get_cached_state().await?;
        let catalog = SessionCatalog::new(state);

        let session = Session::new(
            info,
            catalog,
            metastore,
            native,
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

/// Ensure that the spill path exists and that it's writable if provided.
pub fn ensure_spill_path<P: AsRef<Path>>(path: Option<P>) -> Result<()> {
    if let Some(p) = path {
        let path = p.as_ref();
        info!(?path, "checking spill path");

        fs::create_dir_all(path)?;

        let file = path.join("glaredb_startup_spill_check");
        fs::write(&file, vec![0, 1, 2, 3])?;
        fs::remove_file(&file)?;
    }
    Ok(())
}
