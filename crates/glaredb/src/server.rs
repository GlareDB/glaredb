use std::path::PathBuf;
use std::sync::Arc;
use std::{env, fs};

use crate::util::MetastoreClientMode;
use anyhow::{anyhow, Result};
use pgsrv::auth::LocalAuthenticator;
use pgsrv::handler::{ProtocolHandler, ProtocolHandlerConfig};
use sqlexec::engine::{Engine, EngineStorageConfig};
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
    integration_testing: bool,
    pg_handler: Arc<ProtocolHandler>,
}

impl Server {
    /// Connect to the given source, performing any bootstrap steps as
    /// necessary.
    pub async fn connect(
        metastore_addr: Option<String>,
        segment_key: Option<String>,
        authenticator: Box<dyn LocalAuthenticator>,
        data_dir: Option<PathBuf>,
        service_account_key: Option<String>,
        spill_path: Option<PathBuf>,
        integration_testing: bool,
    ) -> Result<Self> {
        // Our bare container image doesn't have a '/tmp' dir on startup (nor
        // does it specify an alternate dir to use via `TMPDIR`).
        let env_tmp = env::temp_dir();
        info!(?env_tmp, "ensuring temp dir");
        fs::create_dir_all(&env_tmp)?;

        // Connect to metastore.
        let mode = MetastoreClientMode::new_from_options(metastore_addr, data_dir.clone())?;
        let metastore_client = mode.into_client().await?;

        let tracker = match segment_key {
            Some(key) => {
                info!("initializing segment telemetry tracker");
                SegmentTracker::new(key).into()
            }
            None => {
                info!("skipping telementry initialization");
                Tracker::Nop
            }
        };

        // TODO: There's going to need to more validation needed to ensure we're
        // using a metastore that makes sense. E.g. using a remote metastore and
        // in-memory table storage would cause inconsistency.
        //
        // We don't want to end up in a situation where a metastore thinks a
        // table exists but it really doesn't (or the other way around).
        let storage_conf = match (data_dir, service_account_key) {
            (None, Some(key)) => EngineStorageConfig::Gcs {
                service_account_key: key,
            },
            (Some(dir), None) => EngineStorageConfig::Local { path: dir },
            (None, None) => EngineStorageConfig::Memory,
            (Some(_), Some(_)) => {
                return Err(anyhow!(
                    "Data directory and service account both provided. Expected at most one."
                ))
            }
        };

        let engine = Engine::new(
            metastore_client,
            storage_conf,
            Arc::new(tracker),
            spill_path,
        )
        .await?;
        let handler_conf = ProtocolHandlerConfig {
            authenticator,
            // TODO: Allow specifying SSL/TLS on the GlareDB side as well. I
            // want to hold off on doing that until we have a shared config
            // between the proxy and GlareDB.
            ssl_conf: None,
            integration_testing,
        };
        Ok(Server {
            integration_testing,
            pg_handler: Arc::new(ProtocolHandler::new(engine, handler_conf)),
        })
    }

    /// Serve using the provided config.
    pub async fn serve(self, conf: ServerConfig) -> Result<()> {
        info!("GlareDB listening...");

        // Shutdown handler.
        let (tx, mut rx) = oneshot::channel();
        let pg_handler = self.pg_handler.clone();
        tokio::spawn(async move {
            match signal::ctrl_c().await {
                Ok(()) => {
                    info!("shutdown triggered");
                    let engine_shutdown = pg_handler.engine.shutdown();

                    // Don't wait for active-sessions if integration testing is
                    // not set. This helps when doing "CTRL-C" during testing.
                    if !self.integration_testing {
                        loop {
                            let sess_count = pg_handler.engine.session_count();
                            if sess_count == 0 {
                                break;
                            }

                            info!(%sess_count, "shutdown prevented, active sessions");

                            // Still have sessions. Keep looping with some sleep in
                            // between.
                            tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await;
                        }
                    }

                    match engine_shutdown.await {
                        Ok(()) => {}
                        Err(err) => {
                            error!(%err, "unable to shutdown the engine gracefully");
                        }
                    };

                    // Shutdown!
                    let _ = tx.send(());
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
