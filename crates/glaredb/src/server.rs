use std::path::PathBuf;
use std::sync::Arc;
use std::{env, fs};

use crate::util::{ensure_spill_path, MetastoreMode};
use anyhow::Result;
use pgsrv::handler::ProtocolHandler;
use pushexec::runtime::ExecRuntime;
use sqlexec::engine::Engine;
use telemetry::{SegmentTracker, Tracker};
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::oneshot;
use tracing::{debug, debug_span, error, info, Instrument};
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
        exec_runtime: ExecRuntime,
        metastore_addr: Option<String>,
        segment_key: Option<String>,
        local: bool,
        local_file_path: Option<PathBuf>,
        spill_path: Option<PathBuf>,
        integration_testing: bool,
    ) -> Result<Self> {
        // Our bare container image doesn't have a '/tmp' dir on startup (nor
        // does it specify an alternate dir to use via `TMPDIR`).
        let env_tmp = env::temp_dir();
        info!(?env_tmp, "ensuring temp dir");
        fs::create_dir_all(&env_tmp)?;

        ensure_spill_path(spill_path.as_ref())?;

        // Connect to metastore.
        let mode = MetastoreMode::new_from_options(metastore_addr, local_file_path, local)?;
        let metastore_client = mode.into_client().await?;

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

        let engine = Engine::new(
            exec_runtime,
            metastore_client,
            Arc::new(tracker),
            spill_path,
        )
        .await?;
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
