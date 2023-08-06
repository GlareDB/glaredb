use crate::background_jobs::JobRunner;
use crate::errors::{ExecError, Result};
use crate::metastore::client::{Supervisor, DEFAULT_WORKER_CONFIG};
use crate::session::Session;
use datafusion_ext::vars::SessionVars;

use std::fs;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::metastore::catalog::SessionCatalog;
use datasources::native::access::NativeTableStorage;
use object_store_util::conf::StorageConfig;
use protogen::gen::metastore::service::metastore_service_client::MetastoreServiceClient;
use telemetry::Tracker;
use tonic::transport::Channel;
use tracing::{debug, info};
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub struct SessionStorageConfig {
    /// The bucket that should be used for database storage for a session.
    ///
    /// If this is omitted, the engine storage config should either be set to
    /// local or in-memory.
    pub gcs_bucket: Option<String>,
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
            (_, Some(_)) => StorageConfig::Memory,
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
    /// Background jobs to run.
    background_jobs: JobRunner,
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
            background_jobs: JobRunner::new(Default::default()),
        })
    }

    /// Attempts to shutdown the engine gracefully.
    pub async fn shutdown(&self) -> Result<()> {
        self.background_jobs.close().await?;
        Ok(())
    }

    /// Get the current number of sessions.
    pub fn session_count(&self) -> u64 {
        self.session_counter.load(Ordering::Relaxed)
    }

    /// Create a new session, initializing it with the provided session
    /// variables.
    pub async fn new_session(
        &self,
        vars: SessionVars,
        storage: SessionStorageConfig,
    ) -> Result<TrackedSession> {
        let conn_id = *vars.connection_id.value();
        let database_id = *vars.database_id.value();
        let metastore = self.supervisor.init_client(conn_id, database_id).await?;
        let native = self
            .storage
            .new_native_tables_storage(database_id, &storage)?;

        let state = metastore.get_cached_state().await?;
        let catalog = SessionCatalog::new_with_client(state, metastore);

        let session = Session::new(
            vars,
            catalog,
            native,
            self.tracker.clone(),
            self.spill_path.clone(),
            self.background_jobs.clone(),
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
