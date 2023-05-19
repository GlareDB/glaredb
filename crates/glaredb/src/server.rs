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
use tokio::signal;
use tokio::sync::oneshot;
use tracing::{debug, debug_span, error, info, warn, Instrument};
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
        integration_testing: bool,
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
            pg_handler: Arc::new(ProtocolHandler::new(engine, local, integration_testing)),
        })
    }

    /// Serve using the provided config.
    pub async fn serve(self, conf: ServerConfig) -> Result<()> {
        info!("GlareDB listening...");

        // Shutdown handler.
        let (tx, mut rx) = oneshot::channel();
        let pg_hander = self.pg_handler.clone();
        tokio::spawn(async move {
            match signal::ctrl_c().await {
                Ok(()) => {
                    info!("shutdown triggered");
                    loop {
                        let sess_count = pg_hander.engine.session_count();
                        if sess_count == 0 {
                            // Shutdown!
                            let _ = tx.send(());
                            return;
                        }

                        info!(%sess_count, "shutdown prevented, active sessions");

                        // Still have sessions. Keep looping with some sleep in
                        // between.
                        tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await;
                    }
                }
                Err(err) => {
                    error!(%err, "unable to listen for shutdown signal");
                }
            }
        });

        loop {
            tokio::select! {
                _ = &mut rx => {
                    info!("shutting down");
                    return Ok(())
                }

                result = conf.pg_listener.accept() => {
                    let (conn, client_addr) = result?;

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
