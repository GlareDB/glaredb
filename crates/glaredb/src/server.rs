use access::runtime::{AccessConfig, AccessRuntime};
use anyhow::Result;
use pgsrv::handler::{Handler, PostgresHandler};
use sqlexec::engine::Engine;
use std::env;
use std::fs;
use std::{path::PathBuf, sync::Arc};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tracing::trace;
use tracing::{debug, info};

pub struct ServerConfig {
    pub pg_listener: TcpListener,
}

pub struct Server {
    pg_handler: Arc<Handler>,
}

//TODO: Make this configured in a config file
//TODO: Update default size 1 GiB for disk cache
const DEFAULT_MAX_CACHE_SIZE: u64 = 1024 * 1024 * 1024;

impl Server {
    /// Connect to the given source, performing any bootstrap steps as
    /// necessary.
    pub async fn connect(db_name: impl Into<String>, object_store: &str) -> Result<Self> {
        // TODO: Provide the access runtime to the server.
        // TODO: Have cache_dir path come from a config file

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

        let cache_dir = PathBuf::from(TempDir::new()?.path());
        let object_store = object_store.parse()?;
        let db_name = db_name.into();

        let config = AccessConfig {
            db_name,
            object_store,
            cached: true,
            max_object_store_cache_size: Some(DEFAULT_MAX_CACHE_SIZE),
            cache_path: Some(cache_dir),
        };

        info!(?config, "Access Config");
        let access = Arc::new(AccessRuntime::new(config).await?);

        let engine = Engine::new(access)?;
        Ok(Server {
            pg_handler: Arc::new(Handler::new(engine)),
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
                    Err(e) => debug!(%e, %client_addr, "client disconnected with error."),
                }
            });
        }
    }
}
