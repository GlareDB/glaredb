use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{env, fs};

use anyhow::{anyhow, Result};
use metastore::local::{start_inprocess_inmemory, start_inprocess_local};
use metastore::proto::service::metastore_service_client::MetastoreServiceClient;
use pgsrv::handler::ProtocolHandler;
use sqlexec::engine::Engine;
use telemetry::{SegmentTracker, Tracker};
use tokio::net::TcpListener;
use tracing::{debug, debug_span, info, warn, Instrument};
use uuid::Uuid;

pub struct ServerConfig {
    pub pg_listener: TcpListener,
}

pub struct Server {
    pg_handler: Arc<ProtocolHandler>,
}

impl Server {
    /// Connect to the given source, performing any bootstrap steps as
    /// necessary.
    pub async fn connect(
        metastore_addr: Option<String>,
        segment_key: Option<String>,
        local: bool,
        local_file_path: Option<String>,
        spill_path: Option<PathBuf>,
    ) -> Result<Self> {
        // Our bare container image doesn't have a '/tmp' dir on startup (nor
        // does it specify an alternate dir to use via `TMPDIR`).
        let env_tmp = env::temp_dir();
        info!(?env_tmp, "ensuring temp dir");
        fs::create_dir_all(&env_tmp)?;

        if let Some(spill_path) = &spill_path {
            info!(?spill_path, "checking spill path");
            check_spill_path(spill_path)?;
        }

        // Connect to metastore.
        let metastore_client = match (metastore_addr, local) {
            (Some(addr), _) => {
                info!(%addr, "connecting to remote metastore");
                MetastoreServiceClient::connect(addr).await?
            }
            (None, true) => {
                info!("starting in-process metastore");
                if let Some(path) = local_file_path {
                    let path: PathBuf = path.into();
                    if !path.exists() {
                        fs::create_dir_all(&path)?;
                    }
                    if path.exists() && !path.is_dir() {
                        warn!(
                            ?path,
                            "Path is not a valid directory: starting in memory store"
                        );
                        start_inprocess_inmemory().await?
                    } else {
                        start_inprocess_local(path).await?
                    }
                } else {
                    start_inprocess_inmemory().await?
                }
            }
            (None, false) => {
                return Err(anyhow!(
                    "Metastore address not provided and GlareDB not configured to run locally."
                ))
            }
        };

        let tracker = match segment_key {
            Some(key) => {
                info!("initializing segment telemeetry tracker");
                SegmentTracker::new(key).into()
            }
            None => {
                info!("skipping telementry initialization");
                Tracker::Nop
            }
        };

        let engine = Engine::new(metastore_client, Arc::new(tracker), spill_path).await?;
        Ok(Server {
            pg_handler: Arc::new(ProtocolHandler::new(engine, local)),
        })
    }

    /// Serve using the provided config.
    pub async fn serve(self, conf: ServerConfig) -> Result<()> {
        info!("GlareDB listening...");
        loop {
            let (conn, client_addr) = conf.pg_listener.accept().await?;
            let pg_handler = self.pg_handler.clone();
            let conn_id = Uuid::new_v4();
            let span = debug_span!("glaredb_connection", %conn_id);
            tokio::spawn(
                async move {
                    debug!(%client_addr, "client connected (pg)");
                    match pg_handler.handle_connection(conn_id, conn).await {
                        Ok(_) => debug!(%client_addr, "client disconnected"),
                        Err(e) => debug!(%e, %client_addr, "client disconnected with error"),
                    }
                }
                .instrument(span),
            );
        }
    }
}

/// Check that the spill path exists and that it's writable.
fn check_spill_path(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;

    let file = path.join("glaredb_startup_spill_check");
    fs::write(&file, vec![0, 1, 2, 3])?;
    fs::remove_file(&file)?;
    Ok(())
}
