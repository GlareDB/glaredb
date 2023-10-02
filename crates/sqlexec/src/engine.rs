use crate::background_jobs::JobRunner;
use crate::context::remote::RemoteSessionContext;
use crate::errors::{ExecError, Result};
use crate::metastore::client::{MetastoreClientSupervisor, DEFAULT_METASTORE_CLIENT_CONFIG};
use crate::session::Session;
use std::collections::HashMap;

use object_store::aws::AmazonS3ConfigKey;
use object_store::gcp::GoogleConfigKey;
use std::fs;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::metastore::catalog::SessionCatalog;
use datafusion_ext::vars::SessionVars;
use datasources::common::url::{DatasourceUrl, DatasourceUrlType};
use datasources::native::access::NativeTableStorage;
use metastore::local::start_inprocess;
use metastore::util::MetastoreClientMode;
use object_store_util::conf::StorageConfig;
use object_store_util::shared::SharedObjectStore;
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

// TODO: There's a significant amount of overlap with `StorageConfig`, would be good to consider
// consolidating them into one
/// Storage configuration for the compute engine.
///
/// The configuration defined here alongside the configuration passed in through
/// the proxy will be used to connect to database storage.
#[derive(Debug, Clone)]
pub enum EngineStorageConfig {
    S3 {
        access_key_id: String,
        secret_access_key: String,
        region: Option<String>,
        endpoint: Option<String>,
        bucket: Option<String>,
    },
    Gcs {
        service_account_key: String,
        bucket: Option<String>,
    },
    Local {
        path: PathBuf,
    },
    Memory,
}

impl EngineStorageConfig {
    pub fn try_from_options(location: &String, opts: HashMap<String, String>) -> Result<Self> {
        if location.starts_with("memory://") {
            return Ok(EngineStorageConfig::Memory);
        }

        let datasource_url = DatasourceUrl::try_new(location)?;
        Ok(match datasource_url {
            DatasourceUrl::File(path) => EngineStorageConfig::Local { path },
            DatasourceUrl::Url(ref url) => {
                // Buket potentially provided as a part of the location URL, try to extract it.
                let bucket = url.host_str().map(|h| h.to_string());

                let url_type = datasource_url.datasource_url_type();
                match url_type {
                    DatasourceUrlType::Gcs => {
                        let service_account_path =
                            opts.get("service_account_path").cloned().unwrap_or_else(|| {
                                std::env::var(GoogleConfigKey::ServiceAccount.as_ref().to_uppercase())
                                    .expect(
                                        "'service_account_path' in provided storage options or 'GOOGLE_SERVICE_ACCOUNT' as env var",
                                    )
                            });

                        let service_account_key = fs::read_to_string(service_account_path)?;

                        let bucket = bucket.or(opts.get("bucket").cloned());
                        EngineStorageConfig::Gcs {
                            service_account_key,
                            bucket,
                        }
                    }
                    DatasourceUrlType::S3 | DatasourceUrlType::Http => {
                        let access_key_id = opts.get("access_key_id").cloned().unwrap_or_else(|| {
                            std::env::var(AmazonS3ConfigKey::AccessKeyId.as_ref().to_uppercase())
                                .expect("'access_key_id' in provided storage options or 'AWS_ACCESS_KEY_ID' as env var")
                        });
                        let secret_access_key =
                            opts.get("secret_access_key").cloned().unwrap_or_else(|| {
                                std::env::var(AmazonS3ConfigKey::SecretAccessKey.as_ref().to_uppercase())
                                    .expect("'secret_access_key' in provided storage options or 'AWS_SECRET_ACCESS_KEY' as env var")
                            });

                        let mut endpoint = opts.get("endpoint").cloned();
                        let region = opts.get("region").cloned();

                        let bucket = if url_type != DatasourceUrlType::S3
                            && !location.contains("amazonaws.com")
                        {
                            // For now we don't allow proper HTTP object stores as storage locations,
                            // so interpret this case as either Cloudflare R2 or a MinIO instance
                            endpoint = Some(location.clone());
                            opts.get("bucket").cloned()
                        } else {
                            bucket.or(opts.get("bucket").cloned())
                        };

                        EngineStorageConfig::S3 {
                            access_key_id,
                            secret_access_key,
                            region,
                            endpoint,
                            bucket,
                        }
                    }
                    _ => unreachable!(),
                }
            }
        })
    }

    /// Create a native table storage config from values in the engine and
    /// session configs.
    ///
    /// Errors if the engine config is incompatible with the session config.
    pub fn storage_config(&self, session_conf: &SessionStorageConfig) -> Result<StorageConfig> {
        Ok(match (self.clone(), session_conf.gcs_bucket.clone()) {
            // GCS bucket defined via session config or at the engine config level
            (
                EngineStorageConfig::Gcs {
                    service_account_key,
                    ..
                },
                Some(bucket),
            )
            | (
                EngineStorageConfig::Gcs {
                    service_account_key,
                    bucket: Some(bucket),
                },
                None,
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
            (
                EngineStorageConfig::S3 {
                    access_key_id,
                    secret_access_key,
                    region,
                    endpoint,
                    bucket,
                },
                _,
            ) => StorageConfig::S3 {
                access_key_id,
                secret_access_key,
                region,
                endpoint,
                bucket,
            },
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
    supervisor: MetastoreClientSupervisor,
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
            supervisor: MetastoreClientSupervisor::new(metastore, DEFAULT_METASTORE_CLIENT_CONFIG),
            tracker,
            storage,
            spill_path,
            session_counter: Arc::new(AtomicU64::new(0)),
            background_jobs: JobRunner::new(Default::default()),
        })
    }

    /// Create a new `Engine` instance from the provided storage configuration with a in-process metastore
    pub async fn from_storage_options(
        location: &String,
        opts: &HashMap<String, String>,
    ) -> Result<Engine> {
        let conf = EngineStorageConfig::try_from_options(location, opts.clone())?;
        let store = conf
            .storage_config(&SessionStorageConfig::default())?
            .new_object_store()?;

        // Wrap up the store with a shared one, so that we get to use the non-atomic
        // copy-if-not-exists that is defined there when initializing the lease
        let store = SharedObjectStore::new(store);
        let client = start_inprocess(Arc::new(store)).await.map_err(|e| {
            ExecError::String(format!("Failed to start an in-process metastore: {e}"))
        })?;

        Engine::new(client, conf, Arc::new(Tracker::Nop), None).await
    }

    /// Create a new `Engine` instance from the provided data directory. This can be removed once the
    /// `--data-dir` option gets consolidated into the storage options above.
    pub async fn from_data_dir(data_dir: &Option<PathBuf>) -> Result<Engine> {
        let conf = match data_dir {
            Some(path) => EngineStorageConfig::Local { path: path.clone() },
            None => EngineStorageConfig::Memory,
        };

        let mode = MetastoreClientMode::new_local(data_dir.clone());
        let client = mode.into_client().await.map_err(|e| {
            ExecError::String(format!(
                "Failed creating a local metastore client for data dir {data_dir:?}: {e}"
            ))
        })?;

        Engine::new(client, conf, Arc::new(Tracker::Nop), None).await
    }

    pub fn with_spill_path(mut self, spill_path: Option<PathBuf>) -> Engine {
        self.spill_path = spill_path;
        self
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

    /// Create a new local session, initializing it with the provided session
    /// variables.
    pub async fn new_local_session_context(
        &self,
        vars: SessionVars,
        storage: SessionStorageConfig,
    ) -> Result<TrackedSession> {
        let conn_id = vars.connection_id();
        let database_id = vars.database_id();
        let metastore = self.supervisor.init_client(conn_id, database_id).await?;
        let native = self
            .storage
            .new_native_tables_storage(database_id, &storage)?;

        let state = metastore.get_cached_state().await?;
        let catalog = SessionCatalog::new(state);

        let session = Session::new(
            vars,
            catalog,
            metastore.into(),
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

    /// Create a new remote session for plan execution.
    ///
    /// Note that this isn't wrapped in a tracked session yet (to avoid hanging
    /// since we don't guarantee that sessions get closed).
    pub async fn new_remote_session_context(
        &self,
        vars: SessionVars,
        storage: SessionStorageConfig,
    ) -> Result<RemoteSessionContext> {
        let conn_id = vars.connection_id();
        let database_id = vars.database_id();
        let metastore = self.supervisor.init_client(conn_id, database_id).await?;
        let native = self
            .storage
            .new_native_tables_storage(database_id, &storage)?;

        let state = metastore.get_cached_state().await?;
        let catalog = SessionCatalog::new(state);

        let context = RemoteSessionContext::new(
            vars,
            catalog,
            metastore.into(),
            native,
            self.background_jobs.clone(),
            self.spill_path.clone(),
        )?;

        Ok(context)
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
