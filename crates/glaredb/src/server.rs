use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::{env, fs};

use anyhow::{anyhow, Result};
use ioutil::ensure_dir;
use metastore::local::start_inprocess;
use object_store_util::conf::StorageConfig;
use pgsrv::auth::LocalAuthenticator;
use pgsrv::handler::{ProtocolHandler, ProtocolHandlerConfig};
use protogen::gen::rpcsrv::service::execution_service_server::ExecutionServiceServer;
use protogen::gen::rpcsrv::simple::simple_service_server::SimpleServiceServer;
use rpcsrv::flight::handler::{FlightServiceServer, FlightSessionHandler};
use rpcsrv::handler::RpcHandler;
use rpcsrv::simple::SimpleHandler;
use sqlexec::engine::{Engine, EngineStorageConfig};
use telemetry::{SegmentTracker, Tracker};
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::oneshot;
use tonic::transport::server::{Router, TcpIncoming};
use tonic::transport::Server;
use tracing::{debug, debug_span, error, info, Instrument};
use uuid::Uuid;

/// Configuration for initializing the postgres api
pub struct PostgresProtocolConfig {
    /// Listener to use for pg handler.
    listener: TcpListener,
    /// Handler to use for pg connections.
    handler: Arc<ProtocolHandler>,
}

pub struct ComputeServer {
    integration_testing: bool,
    disable_rpc_auth: bool,
    enable_simple_query_rpc: bool,
    enable_flight_api: bool,
    engine: Arc<Engine>,
    pg_config: Option<PostgresProtocolConfig>,
    rpc_listener: Option<TcpListener>,
}

pub struct ComputeServerBuilder {
    /// Listener to use for pg handler.
    pg_listener: Option<TcpListener>,
    /// Listener to use for rpc handler.
    rpc_listener: Option<TcpListener>,
    segment_key: Option<String>,
    authenticator: Option<Box<dyn LocalAuthenticator>>,
    data_dir: Option<PathBuf>,
    service_account_path: Option<String>,
    location: Option<String>,
    storage_options: HashMap<String, String>,
    spill_path: Option<PathBuf>,
    integration_testing: bool,
    disable_rpc_auth: bool,
    enable_simple_query_rpc: bool,
    enable_flight_api: bool,
    metastore_bucket: Option<String>,
    metastore_service_account_path: Option<String>,
    metastore_local_file_path: Option<PathBuf>,
}

impl ComputeServerBuilder {
    fn new() -> Self {
        ComputeServerBuilder {
            pg_listener: None,
            rpc_listener: None,
            segment_key: None,
            authenticator: None,
            data_dir: None,
            service_account_path: None,
            location: None,
            storage_options: HashMap::new(),
            spill_path: None,
            integration_testing: false,
            disable_rpc_auth: false,
            enable_simple_query_rpc: false,
            enable_flight_api: false,
            metastore_local_file_path: None,
            metastore_service_account_path: None,
            metastore_bucket: None,
        }
    }
    /// Set the authenticator to use for the pg handler.
    pub fn with_authenticator<T: LocalAuthenticator + 'static>(mut self, authenticator: T) -> Self {
        self.authenticator = Some(Box::new(authenticator));
        self
    }
    /// Add a tcp listener to use for serving over the pg protocol.
    pub fn with_pg_listener(mut self, pg_listener: TcpListener) -> Self {
        self.pg_listener = Some(pg_listener);
        self
    }
    /// Optionally add a tcp listener to use for serving over the pg protocol.
    pub fn with_pg_listener_opt(mut self, pg_listener: Option<TcpListener>) -> Self {
        self.pg_listener = pg_listener;
        self
    }
    /// Add a tcp listener to use for serving over the rpc protocol.
    pub fn with_rpc_listener(mut self, rpc_listener: TcpListener) -> Self {
        self.rpc_listener = Some(rpc_listener);
        self
    }
    /// Optionally add a tcp listener to use for serving over the rpc protocol.
    pub fn with_rpc_listener_opt(mut self, rpc_listener: Option<TcpListener>) -> Self {
        self.rpc_listener = rpc_listener;
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


    pub fn with_metastore_bucket(mut self, name: String) -> Self {
        self.metastore_bucket = Some(name);
        self
    }

    pub fn with_metastore_bucket_opt(mut self, bucket: Option<String>) -> Self {
        self.metastore_bucket = bucket;
        self
    }


    pub fn with_metastore_local_file_path(mut self, path: PathBuf) -> Self {
        self.metastore_local_file_path = Some(path);
        self
    }

    pub fn with_metastore_local_file_path_opt(mut self, path: Option<PathBuf>) -> Self {
        self.metastore_local_file_path = path;
        self
    }

    pub fn with_metastore_service_account_path(mut self, path: String) -> Self {
        self.metastore_service_account_path = Some(path);
        self
    }

    pub fn with_metastore_service_account_path_opt(mut self, path: Option<String>) -> Self {
        self.metastore_service_account_path = path;
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
    pub fn enable_flight_api(mut self, enable_flight_api: bool) -> Self {
        self.enable_flight_api = enable_flight_api;
        self
    }

    pub async fn connect(self) -> Result<ComputeServer> {
        // Invalid state if we have a pg_listener but no authenticator.
        if self.pg_listener.is_some() && self.authenticator.is_none() {
            return Err(anyhow!("pg_listener provided but no authenticator"));
        }

        // Our bare container image doesn't have a '/tmp' dir on startup (nor
        // does it specify an alternate dir to use via `TMPDIR`).
        let env_tmp = env::temp_dir();
        debug!(?env_tmp, "ensuring temp dir");
        fs::create_dir_all(&env_tmp)?;

        let tracker = match self.segment_key.clone() {
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
        let engine = self.create_engine_from_opts(tracker).await?;

        let pg_config = if let Some(listener) = self.pg_listener {
            let handler_conf = ProtocolHandlerConfig {
                authenticator: self.authenticator.unwrap(),
                // TODO: Allow specifying SSL/TLS on the GlareDB side as well. I
                // want to hold off on doing that until we have a shared config
                // between the proxy and GlareDB.
                ssl_conf: None,
                integration_testing: self.integration_testing,
            };
            let pg_handler = Arc::new(ProtocolHandler::new(engine.clone(), handler_conf));
            Some(PostgresProtocolConfig {
                listener,
                handler: pg_handler,
            })
        } else {
            debug!("pg_listener not provided. Skipping pg protocol.");
            None
        };

        Ok(ComputeServer {
            integration_testing: self.integration_testing,
            disable_rpc_auth: self.disable_rpc_auth,
            enable_simple_query_rpc: self.enable_simple_query_rpc,
            enable_flight_api: self.enable_flight_api,
            pg_config,
            engine,
            rpc_listener: self.rpc_listener,
        })
    }

    async fn create_engine_from_opts(
        &self,
        tracker: Tracker,
    ) -> Result<Arc<Engine>, anyhow::Error> {
        let engine = if let Some(location) = self.location.clone() {
            // TODO: try to consolidate with --data-dir and --metastore-addr options
            let engine = Engine::from_storage_options(
                &location,
                &HashMap::from_iter(self.storage_options.clone()),
            )
            .await?;
            Arc::new(engine.with_tracker(Arc::new(tracker)))
        } else {
            // TODO: There's going to need to more validation needed to ensure we're
            // using a metastore that makes sense. E.g. using a remote metastore and
            // in-memory table storage would cause inconsistency.
            //
            // We don't want to end up in a situation where a metastore thinks a
            // table exists but it really doesn't (or the other way around).
            let storage_conf = match (self.data_dir.clone(), self.service_account_path.clone()) {
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

            let metastore_storage_conf = match (
                self.metastore_bucket.clone(),
                self.metastore_service_account_path.clone(),
                self.metastore_local_file_path.clone(),
            ) {
                (Some(bucket), Some(service_account_path), None) => {
                    let service_account_key = std::fs::read_to_string(service_account_path)?;
                    StorageConfig::Gcs {
                        bucket: Some(bucket),
                        service_account_key,
                    }
                }
                (None, None, Some(p)) => {
                    ensure_dir(&p)?;
                    StorageConfig::Local { path: p }
                }
                (None, None, None) => StorageConfig::Memory,
                _ => {
                    return Err(anyhow!(
                    "Invalid arguments, 'metastore-service-account-path' and 'metastore-bucket' must both be provided."
                ))
                }
            };


            let metastore_client =
                start_inprocess(metastore_storage_conf.new_object_store()?).await?;

            Arc::new(
                Engine::new(
                    metastore_client,
                    storage_conf,
                    Arc::new(tracker),
                    self.spill_path.clone(),
                )
                .await?,
            )
        };
        Ok(engine)
    }
}


impl ComputeServer {
    pub fn builder() -> ComputeServerBuilder {
        ComputeServerBuilder::new()
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

        if self.enable_flight_api {
            info!("enabling flight sql service");
            let flight_handler = FlightSessionHandler::new(self.engine.clone());
            server = server.add_service(FlightServiceServer::new(flight_handler));
        }
        // Add in the simple interface if requested.
        if self.enable_simple_query_rpc {
            info!("enabling simple query rpc service");
            let handler = SimpleHandler::new(self.engine.clone());
            server = server.add_service(SimpleServiceServer::new(handler));
        }
        server
    }

    /// Serve using the provided config.
    pub async fn serve(self) -> Result<()> {
        let rpc_msg = if let Some(listener) = &self.rpc_listener {
            format!("Connect via RPC: grpc://{}", listener.local_addr()?)
        } else {
            "".to_string()
        };

        let pg_msg = if let Some(PostgresProtocolConfig { ref listener, .. }) = &self.pg_config {
            format!(
                "Connect via Postgres protocol: postgresql://{}",
                listener.local_addr()?,
            )
        } else {
            "".to_string()
        };

        info!(
            "Starting GlareDB {}\n{}",
            env!("CARGO_PKG_VERSION"),
            [rpc_msg, pg_msg].join("\n"),
        );

        // Shutdown handler.

        let engine = self.engine.clone();
        let mut rx = spawn_shutdown_handler(engine, self.integration_testing);

        // Start rpc service.
        if self.rpc_listener.is_some() {
            let server = self.build_rpc_service();
            tokio::spawn(async move {
                let incoming =
                    TcpIncoming::from_listener(self.rpc_listener.unwrap(), true, None).unwrap();

                if let Err(e) = server.serve_with_incoming(incoming).await {
                    // TODO: Maybe panic instead? Revisit once we have
                    // everything working.
                    error!(%e, "rpc service died");
                }
            });
        }

        if let Some(PostgresProtocolConfig { listener, handler }) = self.pg_config {
            // Postgres handler loop.
            loop {
                tokio::select! {
                    _ = &mut rx => {
                        info!("shutting down");
                        return Ok(())
                    }

                result = listener.accept() => {
                    let (conn, client_addr) = result?;

                    let pg_handler = handler.clone();
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
        } else {
            // No pg listener. Just wait for shutdown.
            rx.await.map_err(|_| anyhow!("shutdown error"))
        }
    }
}

fn spawn_shutdown_handler(
    engine: Arc<Engine>,
    is_integration_testing: bool,
) -> oneshot::Receiver<()> {
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        match signal::ctrl_c().await {
            Ok(()) => {
                info!("shutdown triggered");

                // Don't wait for active-sessions if integration testing is
                // not set. This helps when doing "CTRL-C" during testing.
                if !is_integration_testing {
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
    rx
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
        let rpc_listener = TcpListener::bind("localhost:0").await.unwrap();

        let pg_addr = pg_listener.local_addr().unwrap();

        let server = ComputeServer::builder()
            .with_authenticator(SingleUserAuthenticator {
                user: "glaredb".to_string(),
                password: "glaredb".to_string(),
            })
            .with_pg_listener(pg_listener)
            .with_rpc_listener(rpc_listener)
            .connect()
            .await
            .unwrap();

        tokio::spawn(server.serve());

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
