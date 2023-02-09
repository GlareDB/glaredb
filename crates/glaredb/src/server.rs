use std::sync::Arc;
use std::{env, fs};

use anyhow::Result;
use metastore::proto::service::metastore_service_client::MetastoreServiceClient;
use pgsrv::handler::ProtocolHandler;
use sqlexec::engine::Engine;
use telemetry::{SegmentTracker, Tracker};
use tokio::net::TcpListener;
use tracing::{debug, debug_span, info, Instrument};
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
        metastore_addr: String,
        segment_key: Option<String>,
        local: bool,
    ) -> Result<Self> {
        // Our bare container image doesn't have a '/tmp' dir on startup (nor
        // does it specify an alternate dir to use via `TMPDIR`).
        let env_tmp = env::temp_dir();
        info!(?env_tmp, "ensuring temp dir");
        fs::create_dir_all(&env_tmp)?;

        // Connect to Metstore.
        let metastore = MetastoreServiceClient::connect(metastore_addr).await?;

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

        let engine = Engine::new(metastore, Arc::new(tracker)).await?;
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
