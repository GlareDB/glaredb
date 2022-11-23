use access::runtime::AccessRuntime;
use anyhow::Result;
use background::{storage::DatabaseStorageUsageJob, BackgroundJob, BackgroundWorker, DebugJob};
use cloud::client::CloudClient;
use cloud::errors::CloudError;
use common::config::DbConfig;
use pgsrv::handler::{Handler, PostgresHandler};
use sqlexec::engine::Engine;
use std::env;
use std::fs;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::trace;
use tracing::{debug, info};

pub struct ServerConfig {
    pub pg_listener: TcpListener,
}

pub struct Server {
    pg_handler: Arc<Handler>,
    /// Join handle for the background worker.
    ///
    /// Exists on the struct to avoid dropping the future after the call to
    /// `connect`.
    #[allow(dead_code)]
    bg_handle: JoinHandle<()>,

    /// Oneshot for shutting down the background worker.
    ///
    /// Not currently used. Exists on the struct so that the worker will
    /// continue to run for the lifetime of this struct.
    #[allow(dead_code)]
    bg_shutdown_sender: oneshot::Sender<()>,
}

impl Server {
    /// Connect to the given source, performing any bootstrap steps as
    /// necessary.
    pub async fn connect(config: DbConfig) -> Result<Self> {
        // Our bare container image doesn't have a '/tmp' dir on startup (nor
        // does it specify an alternate dir to use via `TMPDIR`).
        //
        // The `TempDir` call below will not attempt to create that directory
        // for us.
        //
        // This also happens in the `TempObjectStore`.
        let env_tmp = env::temp_dir();
        trace!(?env_tmp, "ensuring temp dir for cache directory");
        fs::create_dir_all(&env_tmp)?;

        // Get access runtime.
        let access = Arc::new(AccessRuntime::new(config.access).await?);

        // Create cloud client if configured.
        let cloud_client = match CloudClient::try_from_config(config.cloud).await {
            Ok(client) => Some(Arc::new(client)),
            Err(CloudError::CloudCommsDisabled) => None,
            Err(e) => return Err(e.into()),
        };

        // Spin up background worker jobs.
        let jobs: Vec<Box<dyn BackgroundJob>> = vec![
            Box::new(DebugJob),
            Box::new(DatabaseStorageUsageJob::new(
                access.clone(),
                cloud_client.clone(),
                config.background.storage_reporting.interval,
            )),
        ];
        let (tx, rx) = oneshot::channel(); // Note we don't use the shutdown sender yet.
        let worker = BackgroundWorker::new(jobs, rx);
        let bg_handle = tokio::spawn(worker.begin());

        let engine = Engine::new(access).await?;
        Ok(Server {
            pg_handler: Arc::new(Handler::new(engine)),
            bg_handle,
            bg_shutdown_sender: tx,
        })
    }

    /// Serve using the provided config.
    pub async fn serve(self, conf: ServerConfig) -> Result<()> {
        info!("GlareDB listening...");
        loop {
            let (conn, client_addr) = conf.pg_listener.accept().await?;
            let pg_handler = self.pg_handler.clone();
            tokio::spawn(async move {
                debug!(%client_addr, "client connected (pg)");
                match pg_handler.handle_connection(conn).await {
                    Ok(_) => debug!(%client_addr, "client disconnected"),
                    Err(e) => debug!(%e, %client_addr, "client disconnected with error"),
                }
            });
        }
    }
}
