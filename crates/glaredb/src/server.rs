use anyhow::{anyhow, Result};
use metastore::util::MetastoreClientMode;
use pgsrv::auth::LocalAuthenticator;
use pgsrv::handler::{ProtocolHandler, ProtocolHandlerConfig};
use protogen::gen::rpcsrv::service::execution_service_server::ExecutionServiceServer;
use protogen::gen::rpcsrv::simple::simple_service_server::SimpleServiceServer;
use rpcsrv::{handler::RpcHandler, simple::SimpleHandler};
use sqlexec::engine::{Engine, EngineStorageConfig};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::{env, fs};
use telemetry::{SegmentTracker, Tracker};
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::oneshot;
use tonic::transport::server::Router;
use tonic::transport::Server;
use tracing::{debug, debug_span, error, info, Instrument};
use uuid::Uuid;

#[derive(Debug)]
pub struct ServerConfig {
    /// Listener to use for pg handler.
    pub pg_listener: TcpListener,

    /// Address to use for the rpc handler. If not provided, an rpc handler will
    /// not be started.
    pub rpc_addr: Option<SocketAddr>,
}

pub struct ComputeServer {
    integration_testing: bool,
    disable_rpc_auth: bool,
    enable_simple_query_rpc: bool,
    pg_handler: Arc<ProtocolHandler>,
    engine: Arc<Engine>,
}
pub struct ComputeServerBuilder {
    metastore_addr: Option<String>,
    segment_key: Option<String>,
    authenticator: Box<dyn LocalAuthenticator>,
    data_dir: Option<PathBuf>,
    service_account_path: Option<String>,
    location: Option<String>,
    storage_options: HashMap<String, String>,
    spill_path: Option<PathBuf>,
    integration_testing: bool,
    disable_rpc_auth: bool,
    enable_simple_query_rpc: bool,
}

impl ComputeServerBuilder {
    fn new_with_authenticator(authenticator: Box<dyn LocalAuthenticator>) -> Self {
        ComputeServerBuilder {
            metastore_addr: None,
            segment_key: None,
            authenticator,
            data_dir: None,
            service_account_path: None,
            location: None,
            storage_options: HashMap::new(),
            spill_path: None,
            integration_testing: false,
            disable_rpc_auth: false,
            enable_simple_query_rpc: false,
        }
    }
    pub fn with_metastore_addr(mut self, metastore_addr: String) -> Self {
        self.metastore_addr = Some(metastore_addr);
        self
    }
    pub fn with_metastore_addr_opt(mut self, metastore_addr: Option<String>) -> Self {
        self.metastore_addr = metastore_addr;
        self
    }
    pub fn with_segment_key(mut self, segment_key: String) -> Self {
        self.segment_key = Some(segment_key);
        self
    }
    pub fn with_segment_key_opt(mut self, segment_key: Option<String>) -> Self {
        self.segment_key = segment_key;
        self
    }
    pub fn with_data_dir(mut self, data_dir: PathBuf) -> Self {
        self.data_dir = Some(data_dir);
        self
    }
    pub fn with_data_dir_opt(mut self, data_dir: Option<PathBuf>) -> Self {
        self.data_dir = data_dir;
        self
    }
    pub fn with_service_account_path(mut self, service_account_path: String) -> Self {
        self.service_account_path = Some(service_account_path);
        self
    }
    pub fn with_service_account_path_opt(mut self, service_account_path: Option<String>) -> Self {
        self.service_account_path = service_account_path;
        self
    }
    pub fn with_location(mut self, location: String) -> Self {
        self.location = Some(location);
        self
    }

    pub fn with_location_opt(mut self, location: Option<String>) -> Self {
        self.location = location;
        self
    }

    pub fn with_storage_options(mut self, storage_options: HashMap<String, String>) -> Self {
        self.storage_options = storage_options;
        self
    }
    pub fn with_spill_path(mut self, spill_path: PathBuf) -> Self {
        self.spill_path = Some(spill_path);
        self
    }
    pub fn with_spill_path_opt(mut self, spill_path: Option<PathBuf>) -> Self {
        self.spill_path = spill_path;
        self
    }
    pub fn integration_testing_mode(mut self, integration_testing: bool) -> Self {
        self.integration_testing = integration_testing;
        self
    }
    pub fn disable_rpc_auth(mut self, disable_rpc_auth: bool) -> Self {
        self.disable_rpc_auth = disable_rpc_auth;
        self
    }
    pub fn enable_simple_query_rpc(mut self, enable_simple_query_rpc: bool) -> Self {
        self.enable_simple_query_rpc = enable_simple_query_rpc;
        self
    }
    pub async fn connect(self) -> Result<ComputeServer> {
        let ComputeServerBuilder {
            metastore_addr,
            segment_key,
            authenticator,
            data_dir,
            service_account_path,
            location,
            storage_options,
            spill_path,
            integration_testing,
            disable_rpc_auth,
            enable_simple_query_rpc,
        } = self;
        // Our bare container image doesn't have a '/tmp' dir on startup (nor
        // does it specify an alternate dir to use via `TMPDIR`).
        let env_tmp = env::temp_dir();
        debug!(?env_tmp, "ensuring temp dir");
        fs::create_dir_all(&env_tmp)?;

        let tracker = match segment_key {
            Some(key) => {
                debug!("initializing segment telemetry tracker");
                SegmentTracker::new(key).into()
            }
            None => {
                debug!("skipping telemetry initialization");
                Tracker::Nop
            }
        };

        // Create the `Engine` instance
        let engine = if let Some(location) = location {
            // TODO: try to consolidate with --data-dir and --metastore-addr options
            let engine = Engine::from_storage_options(
                &location,
                &HashMap::from_iter(storage_options.clone()),
            )
            .await?;
            Arc::new(engine.with_tracker(Arc::new(tracker)))
        } else {
            // Connect to metastore.
            let mode = match (metastore_addr, &data_dir) {
                (Some(_), Some(_)) => {
                    return Err(anyhow!(
                        "Only one of metastore address or metastore path may be provided."
                    ))
                }
                (Some(addr), None) => MetastoreClientMode::Remote { addr },
                _ => MetastoreClientMode::new_local(data_dir.clone()),
            };
            let metastore_client = mode.into_client().await?;

            // TODO: There's going to need to more validation needed to ensure we're
            // using a metastore that makes sense. E.g. using a remote metastore and
            // in-memory table storage would cause inconsistency.
            //
            // We don't want to end up in a situation where a metastore thinks a
            // table exists but it really doesn't (or the other way around).
            let storage_conf = match (data_dir, service_account_path) {
                (None, Some(path)) => EngineStorageConfig::try_from_options(
                    "gs://",
                    HashMap::from_iter([("service_account_path".to_string(), path)]),
                )?,
                (Some(dir), None) => EngineStorageConfig::try_from_path_buf(&dir)?,
                (None, None) => {
                    EngineStorageConfig::try_from_options("memory://", Default::default())?
                }
                (Some(_), Some(_)) => {
                    return Err(anyhow!(
                        "Data directory and service account both provided. Expected at most one."
                    ))
                }
            };

            Arc::new(
                Engine::new(
                    metastore_client,
                    storage_conf,
                    Arc::new(tracker),
                    spill_path,
                )
                .await?,
            )
        };

        let handler_conf = ProtocolHandlerConfig {
            authenticator,
            // TODO: Allow specifying SSL/TLS on the GlareDB side as well. I
            // want to hold off on doing that until we have a shared config
            // between the proxy and GlareDB.
            ssl_conf: None,
            integration_testing,
        };
        Ok(ComputeServer {
            integration_testing,
            disable_rpc_auth,
            enable_simple_query_rpc,
            pg_handler: Arc::new(ProtocolHandler::new(engine.clone(), handler_conf)),
            engine,
        })
    }
}

impl ComputeServer {
    pub fn with_authenticator<T: LocalAuthenticator + 'static>(
        authenticator: T,
    ) -> ComputeServerBuilder {
        ComputeServerBuilder::new_with_authenticator(Box::new(authenticator))
    }
    fn build_rpc_service(&self) -> Router {
        // Start rpc service.
        let handler = RpcHandler::new(
            self.engine.clone(),
            self.disable_rpc_auth,
            self.integration_testing,
        );
        let mut server = Server::builder()
            .trace_fn(|_| debug_span!("rpc_service_request"))
            .add_service(ExecutionServiceServer::new(handler));
        // Add in the simple interface if requested.
        if self.enable_simple_query_rpc {
            info!("enabling simple query rpc service");
            let handler = SimpleHandler::new(self.engine.clone());
            server = server.add_service(SimpleServiceServer::new(handler));
        }
        server
    }
    /// Serve using the provided config.
    pub async fn serve(self, conf: ServerConfig) -> Result<()> {
        let rpc_msg = if let Some(addr) = conf.rpc_addr {
            format!("\nConnect via RPC: {}\n", addr)
        } else {
            "".to_string()
        };

        info!(
            "Starting GlareDB {}\nConnect via Postgres: postgresql://{}{}",
            env!("CARGO_PKG_VERSION"),
            conf.pg_listener.local_addr()?,
            rpc_msg
        );

        // Shutdown handler.
        let (tx, mut rx) = oneshot::channel();
        let engine = self.engine.clone();
        tokio::spawn(async move {
            match signal::ctrl_c().await {
                Ok(()) => {
                    info!("shutdown triggered");

                    // Don't wait for active-sessions if integration testing is
                    // not set. This helps when doing "CTRL-C" during testing.
                    if !self.integration_testing {
                        loop {
                            let sess_count = engine.session_count();
                            if sess_count == 0 {
                                break;
                            }

                            info!(%sess_count, "shutdown prevented, active sessions");

                            // Still have sessions. Keep looping with some sleep in
                            // between.
                            tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await;
                        }
                    }

                    // Shutdown!
                    let _ = tx.send(());
                }
                Err(err) => {
                    error!(%err, "unable to listen for shutdown signal");
                }
            }
        });

        // Start rpc service.
        if let Some(addr) = conf.rpc_addr {
            let server = self.build_rpc_service();
            tokio::spawn(async move {
                if let Err(e) = server.serve(addr).await {
                    // TODO: Maybe panic instead? Revisit once we have
                    // everything working.
                    error!(%e, "rpc service died");
                }
            });
        }

        // Postgres handler loop.
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use pgsrv::auth::SingleUserAuthenticator;
    use tokio_postgres::{Config as ClientConfig, NoTls};

    use super::*;

    // TODO: Add tests for:
    //
    // - Running a sql query over the postgres protocol
    // - Running a physical plan over rpc
    // - Running a sql query over rpc (simple)
    //
    // The tests should assert that we're properly starting things up based on
    // the config values provided (and inversely, that we don't start things up
    // if the config indicates we shouldn't).

    #[tokio::test]
    async fn no_hang_on_rpc_service_start() {
        let pg_listener = TcpListener::bind("localhost:0").await.unwrap();
        let pg_addr = pg_listener.local_addr().unwrap();
        let server_conf = ServerConfig {
            pg_listener,
            rpc_addr: Some("0.0.0.0:0".parse().unwrap()),
        };

        let server = ComputeServer::with_authenticator(SingleUserAuthenticator {
            user: "glaredb".to_string(),
            password: "glaredb".to_string(),
        })
        .connect()
        .await
        .unwrap();

        tokio::spawn(server.serve(server_conf));

        let (client, conn) = tokio::time::timeout(
            Duration::from_secs(5),
            ClientConfig::new()
                .user("glaredb")
                .password("glaredb")
                .dbname("glaredb")
                .host("localhost")
                .port(pg_addr.port())
                .connect(NoTls),
        )
        .await
        .unwrap() // Timeout error
        .unwrap(); // Connect error

        let (conn_err_tx, _conn_err_rx) = oneshot::channel();
        tokio::spawn(async move { conn_err_tx.send(conn.await) });

        tokio::time::timeout(Duration::from_secs(5), client.simple_query("select 1"))
            .await
            .unwrap() // Timeout error
            .unwrap(); // Query error
    }
}
