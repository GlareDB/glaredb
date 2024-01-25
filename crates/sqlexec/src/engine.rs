use crate::context::remote::RemoteSessionContext;
use crate::errors::{ExecError, Result};
use crate::session::Session;
use catalog::client::{MetastoreClientSupervisor, DEFAULT_METASTORE_CLIENT_CONFIG};
use object_store::azure::AzureConfigKey;
use sqlbuiltins::builtins::{SCHEMA_CURRENT_SESSION, SCHEMA_DEFAULT};
use std::collections::HashMap;

use ioutil::ensure_dir;
use object_store::aws::AmazonS3ConfigKey;
use object_store::{path::Path as ObjectPath, prefix::PrefixStore};
use object_store::{Error as ObjectStoreError, ObjectStore};
use std::fs;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use catalog::session_catalog::{ResolveConfig, SessionCatalog};
use datafusion_ext::vars::SessionVars;
use datasources::common::errors::DatasourceCommonError;
use datasources::common::url::{DatasourceUrl, DatasourceUrlType};
use datasources::native::access::NativeTableStorage;
use distexec::executor::TaskExecutor;
use distexec::scheduler::Scheduler;
use metastore::local::start_inprocess;
use metastore::util::MetastoreClientMode;
use object_store_util::conf::StorageConfig;
use object_store_util::shared::SharedObjectStore;
use protogen::gen::metastore::service::metastore_service_client::MetastoreServiceClient;
use protogen::rpcsrv::types::common;
use telemetry::Tracker;
use tonic::transport::Channel;
use tracing::{debug, info};
use url::Url;
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub struct SessionStorageConfig {
    /// The bucket that should be used for database storage for a session.
    ///
    /// If this is omitted, the engine storage config should either be set to
    /// local, in-memory or provided via CLI location and storage options.
    pub gcs_bucket: Option<String>,
}

impl SessionStorageConfig {
    pub fn new<T: Into<String>>(bucket: Option<T>) -> Self {
        Self {
            gcs_bucket: bucket.map(Into::into),
        }
    }
}

impl From<common::SessionStorageConfig> for SessionStorageConfig {
    fn from(value: common::SessionStorageConfig) -> Self {
        SessionStorageConfig {
            gcs_bucket: value.gcs_bucket,
        }
    }
}

/// Storage configuration for the compute node.
///
/// The configuration defined here alongside the configuration passed in through
/// the proxy will be used to connect to database storage.
#[derive(Debug, Clone)]
pub struct EngineStorageConfig {
    location: Url,
    conf: StorageConfig,
}

impl EngineStorageConfig {
    pub fn try_from_path_buf(path: &PathBuf) -> Result<Self> {
        ensure_dir(path)?;
        let path = fs::canonicalize(path)?;
        Ok(Self {
            location: Url::from_directory_path(&path).map_err(|_| {
                ExecError::String(format!(
                    "Failed to generate a file:// URL from path: {}",
                    path.display()
                ))
            })?,
            conf: StorageConfig::Local { path },
        })
    }
    pub fn try_from_options(location: &str, opts: HashMap<String, String>) -> Result<Self> {
        if location.starts_with("memory://") {
            return Ok(EngineStorageConfig {
                location: Url::parse(location).map_err(DatasourceCommonError::from)?,
                conf: StorageConfig::Memory,
            });
        }

        let datasource_url = DatasourceUrl::try_new(location)?;
        Ok(match datasource_url {
            DatasourceUrl::File(path) => EngineStorageConfig::try_from_path_buf(&path)?,
            DatasourceUrl::Url(ref url) => {
                let url_type = datasource_url.datasource_url_type();
                match url_type {
                    DatasourceUrlType::Gcs => {
                        let service_account_path =
                            opts.get("service_account_path").cloned().unwrap_or_else(|| {
                                std::env::var("GOOGLE_APPLICATION_CREDENTIALS")
                                    .expect(
                                        "'service_account_path' in provided storage options or 'GOOGLE_APPLICATION_CREDENTIALS' as env var",
                                    )
                            });

                        let service_account_key = fs::read_to_string(service_account_path)?;

                        // Extract bucket from the location URL
                        let bucket = opts
                            .get("bucket")
                            .cloned()
                            .or(url.host_str().map(|h| h.to_string()));

                        EngineStorageConfig {
                            location: url.clone(),
                            conf: StorageConfig::Gcs {
                                service_account_key,
                                bucket,
                            },
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

                        let mut endpoint = None;
                        let region = opts.get("region").cloned();
                        let mut bucket = opts
                            .get("bucket")
                            .cloned()
                            .or(url.host_str().map(|h| h.to_string()));

                        if url_type == DatasourceUrlType::Http
                            && !location.contains("amazonaws.com")
                        {
                            // For now we don't allow proper HTTP object stores as storage locations,
                            // so interpret this case as either Cloudflare R2 or a MinIO instance.
                            // Extract the endpoint...
                            endpoint = Some(url[..url::Position::BeforePath].to_string());
                            // ... and the bucket, which is the first path segment (if one exists)
                            let segments = url.path_segments().unwrap().collect::<Vec<_>>();
                            if segments.len() > 1 {
                                bucket = Some(segments[0].to_string());
                            }
                        }

                        EngineStorageConfig {
                            location: url.clone(),
                            conf: StorageConfig::S3 {
                                access_key_id,
                                secret_access_key,
                                region,
                                endpoint,
                                bucket,
                            },
                        }
                    }
                    DatasourceUrlType::Azure => {
                        let account_name = opts.get("account_name").cloned().unwrap_or_else(|| {
                            std::env::var(AzureConfigKey::AccountName.as_ref().to_uppercase())
                                .expect(
                                    "'account_name' in provided storage options or 'AZURE_STORAGE_ACCOUNT_NAME' as env var"
                                )
                        });

                        let access_key = opts.get("access_key").cloned().unwrap_or_else(|| {
                            std::env::var(AzureConfigKey::AccessKey.as_ref().to_uppercase())
                                .expect(
                                    "'access_key' in provided storage options or 'AZURE_STORAGE_ACCOUNT_KEY' as env var"
                                )
                        });

                        // Extract bucket (azure container) from the location URL
                        let container_name = opts
                            .get("container_name")
                            .cloned()
                            .or(url.host_str().map(|h| h.to_string()));

                        EngineStorageConfig {
                            location: url.clone(),
                            conf: StorageConfig::Azure {
                                account_name,
                                access_key,
                                container_name,
                            },
                        }
                    }
                    DatasourceUrlType::File => unreachable!(), // Handled as Datasource::File(_)
                }
            }
        })
    }

    /// Return the currently configured URL.
    pub fn url(&self) -> Url {
        self.location.clone()
    }

    /// Merges `self` and the provided session config to create a new config.
    ///
    /// Errors if the engine config is incompatible with the session config.
    fn with_session_config(&self, session_conf: &SessionStorageConfig) -> Result<Self> {
        Ok(match (self.conf.clone(), session_conf.gcs_bucket.clone()) {
            (
                StorageConfig::Gcs {
                    service_account_key,
                    ..
                },
                Some(bucket),
            ) => {
                // A session-level bucket overrides the inherent configuration.
                // Setup the storage config and the location accordingly.
                EngineStorageConfig {
                    location: Url::parse(&format!("gs://{bucket}"))
                        .map_err(DatasourceCommonError::from)?,
                    conf: StorageConfig::Gcs {
                        service_account_key,
                        bucket: Some(bucket),
                    },
                }
            }
            // Expected gcs config opts for the session.
            (StorageConfig::Gcs { bucket: None, .. }, None) => {
                return Err(ExecError::InvalidStorageConfig(
                    "Missing bucket on session configuration",
                ))
            }
            (_, Some(_)) => EngineStorageConfig {
                location: Url::parse("memory://").map_err(DatasourceCommonError::from)?,
                conf: StorageConfig::Memory,
            },
            _ => self.clone(),
        })
    }

    /// Create a (potentially prefixed) object store rooted at the location URL.
    ///
    /// First creates an object store from the underlying storage config, and then
    /// wraps that up with a `PrefixStore` for non-local storage if there is a path
    /// specified.
    pub fn new_object_store(&self) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
        let store = self.conf.new_object_store()?;

        if matches!(
            self.conf,
            StorageConfig::Local { .. } | StorageConfig::Memory
        ) {
            // Local storage (FS and memory) is already properly prefixed
            return Ok(store);
        }

        let prefix = if let StorageConfig::S3 {
            endpoint: Some(_), ..
        } = self.conf
        {
            // In case when there's an endpoint specified, the bucket is not the host but the
            // first element in the path, so we need to discard it when creating a prefix.
            let mut segments = self.location.path_segments().unwrap().collect::<Vec<_>>();

            if segments.len() <= 1 {
                // The endpoint doesn't include a path, no need for prefixing
                return Ok(store);
            }

            // Remove the first path segment which is actually the bucket
            segments.remove(0);
            let path = segments.join("/");
            ObjectPath::parse(path)?
        } else {
            ObjectPath::parse(self.location.path())?
        };

        if prefix != ObjectPath::from("/") {
            // A path was specified, root the store under it
            debug!(?prefix, "Prefixing native table storage root");
            Ok(Arc::new(PrefixStore::new(store, prefix)))
        } else {
            Ok(store)
        }
    }

    /// Create a new native tables storage for a session for a given database.
    fn new_native_tables_storage(
        &self,
        db_id: Uuid,
        session_conf: &SessionStorageConfig,
    ) -> Result<NativeTableStorage> {
        let conf = self.with_session_config(session_conf)?;
        let store = conf.new_object_store()?;
        let native = NativeTableStorage::new(db_id, conf.location, store);
        Ok(native)
    }
}

/// Hold configuration and clients needed to create database sessions.
/// An engine is able to support multiple [`Session`]'s across multiple db instances
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
    /// Scheduler for running tasks (physical plan).
    task_scheduler: Scheduler,
    /// Task executors.
    _task_executors: Vec<TaskExecutor>,
}

impl Engine {
    /// Create a new engine using the provided access runtime.
    pub async fn new(
        metastore: MetastoreServiceClient<Channel>,
        storage: EngineStorageConfig,
        tracker: Arc<Tracker>,
        spill_path: Option<PathBuf>,
    ) -> Result<Engine> {
        let (task_scheduler, executor_builder) = Scheduler::new();
        // Build executors.
        let num_executors = num_cpus::get();
        let task_executors: Vec<_> = (0..num_executors)
            .map(|_| executor_builder.build_only_local())
            .collect();

        assert!(
            !task_executors.is_empty(),
            "there should be at least one executor"
        );

        Ok(Engine {
            supervisor: MetastoreClientSupervisor::new(metastore, DEFAULT_METASTORE_CLIENT_CONFIG),
            tracker,
            storage,
            spill_path,
            session_counter: Arc::new(AtomicU64::new(0)),
            task_scheduler,
            _task_executors: task_executors,
        })
    }

    /// Returns the telemetry tracker used by this engine.
    pub fn get_tracker(&self) -> Arc<Tracker> {
        self.tracker.clone()
    }

    /// Create a new `Engine` instance from the provided storage configuration with a in-process metastore
    pub async fn from_storage_options(
        location: &str,
        opts: &HashMap<String, String>,
    ) -> Result<Engine> {
        let conf = EngineStorageConfig::try_from_options(location, opts.clone())?;
        let store = conf.new_object_store()?;

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
    pub async fn from_data_dir(data_dir: Option<&PathBuf>) -> Result<Engine> {
        let conf = match data_dir {
            Some(path) => EngineStorageConfig::try_from_path_buf(path)?,
            None => EngineStorageConfig::try_from_options("memory://", Default::default())?,
        };

        let mode = MetastoreClientMode::new_local(data_dir.cloned());
        let client = mode.into_client().await.map_err(|e| {
            ExecError::String(format!(
                "Failed creating a local metastore client for data dir {data_dir:?}: {e}"
            ))
        })?;

        Engine::new(client, conf, Arc::new(Tracker::Nop), None).await
    }

    pub fn with_tracker(mut self, tracker: Arc<Tracker>) -> Engine {
        self.tracker = tracker;
        self
    }

    pub fn with_spill_path(mut self, spill_path: Option<PathBuf>) -> Engine {
        self.spill_path = spill_path;
        self
    }

    /// Get the current number of sessions.
    pub fn session_count(&self) -> u64 {
        self.session_counter.load(Ordering::Relaxed)
    }

    /// Create a new local session, initializing it with the provided session
    /// variables.
    // TODO: This is _very_ easy to mess up with the vars since we implement
    // default (which defaults to the nil uuid), but using default would is
    // incorrect in any case we're running Cloud.
    pub async fn new_local_session_context(
        &self,
        vars: SessionVars,
        storage: SessionStorageConfig,
    ) -> Result<TrackedSession> {
        let session = self.new_untracked_session(vars, storage).await?;

        let prev = self.session_counter.fetch_add(1, Ordering::Relaxed);
        debug!(session_count = prev + 1, "new session opened");

        Ok(TrackedSession {
            inner: session,
            session_counter: self.session_counter.clone(),
        })
    }

    /// Create a new untracked session.
    ///
    /// This does not increment the session counter.
    /// So any session created with this method will not prevent the engine from shutting down.
    pub async fn new_untracked_session(
        &self,
        vars: SessionVars,
        storage: SessionStorageConfig,
    ) -> Result<Session> {
        let database_id = vars.database_id();
        let metastore = self.supervisor.init_client(database_id).await?;
        let native = self
            .storage
            .new_native_tables_storage(database_id, &storage)?;
        let state = metastore.get_cached_state().await?;
        let catalog = SessionCatalog::new(
            state,
            ResolveConfig {
                default_schema_oid: SCHEMA_DEFAULT.oid,
                session_schema_oid: SCHEMA_CURRENT_SESSION.oid,
            },
        );

        Session::new(
            vars,
            catalog,
            metastore.into(),
            native,
            self.tracker.clone(),
            self.spill_path.clone(),
            self.task_scheduler.clone(),
        )
    }

    /// Create a new remote session for plan execution.
    ///
    /// Note that this isn't wrapped in a tracked session yet (to avoid hanging
    /// since we don't guarantee that sessions get closed).
    pub async fn new_remote_session_context(
        &self,
        database_id: Uuid,
        storage: SessionStorageConfig,
    ) -> Result<RemoteSessionContext> {
        let metastore = self.supervisor.init_client(database_id).await?;
        let native = self
            .storage
            .new_native_tables_storage(database_id, &storage)?;

        let state = metastore.get_cached_state().await?;
        let catalog = SessionCatalog::new(
            state,
            ResolveConfig {
                default_schema_oid: SCHEMA_DEFAULT.oid,
                session_schema_oid: SCHEMA_CURRENT_SESSION.oid,
            },
        );

        let context = RemoteSessionContext::new(
            catalog,
            metastore.into(),
            native,
            self.spill_path.clone(),
            self.task_scheduler.clone(),
        )?;

        Ok(context)
    }
}

/// A thin wrapper around a session.
///
/// This is used to allow the engine to track the number of active sessions.
/// When this session is no longer used (dropped), the resulting counter will
/// decrement. This is useful to allow the engine to wait for active sessions to
/// complete on shutdown.
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

#[cfg(test)]
mod tests {
    use crate::engine::{EngineStorageConfig, SessionStorageConfig};
    use crate::errors::Result;
    use object_store_util::conf::StorageConfig;
    use std::collections::HashMap;

    #[test]
    fn merged_conf_session_bucket() -> Result<()> {
        let access_key_id = "my_key".to_string();
        let secret_access_key = "my_secret".to_string();
        let conf = EngineStorageConfig::try_from_options(
            "s3://some-bucket",
            HashMap::from_iter([
                ("access_key_id".to_string(), access_key_id.clone()),
                ("secret_access_key".to_string(), secret_access_key.clone()),
            ]),
        )?;

        assert_eq!(
            conf.conf,
            StorageConfig::S3 {
                access_key_id,
                secret_access_key,
                region: None,
                endpoint: None,
                bucket: Some("some-bucket".to_string()),
            }
        );

        let merged_conf = conf.with_session_config(&SessionStorageConfig {
            gcs_bucket: Some("my-other-bucket".to_string()),
        })?;
        assert_eq!(merged_conf.conf, StorageConfig::Memory,);
        Ok(())
    }
}
