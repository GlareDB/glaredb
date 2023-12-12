use crate::args::server::ServerArgs;
use crate::args::{LocalArgs, MetastoreArgs, PgProxyArgs, RpcProxyArgs};
use crate::local::LocalSession;
use crate::metastore::Metastore;
use crate::pg_proxy::PgProxy;
use crate::rpc_proxy::RpcProxy;
use crate::server::ComputeServer;
use anyhow::{anyhow, Result};
use atty::Stream;
use clap::Subcommand;
use ioutil::ensure_dir;
use object_store_util::conf::StorageConfig;
use pgsrv::auth::{LocalAuthenticator, PasswordlessAuthenticator, SingleUserAuthenticator};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpListener;
use tokio::runtime::{Builder, Runtime};
use tracing::info;

#[derive(Subcommand)]
pub enum Commands {
    /// Starts a local version of GlareDB (default).
    Local(LocalArgs),
    /// Starts the sql server portion of GlareDB.
    Server(ServerArgs),
    /// Starts an instance of the pgsrv proxy.
    #[clap(hide = true)]
    PgProxy(PgProxyArgs),
    /// Starts an instance of the rpcsrv proxy.
    #[clap(hide = true)]
    RpcProxy(RpcProxyArgs),
    /// Starts an instance of the Metastore.
    #[clap(hide = true)]
    Metastore(MetastoreArgs),
}

impl Commands {
    pub fn run(self) -> Result<()> {
        match self {
            Commands::Local(local) => local.run(),
            Commands::Server(server) => server.run(),
            Commands::PgProxy(pg_proxy) => pg_proxy.run(),
            Commands::RpcProxy(rpc_proxy) => rpc_proxy.run(),
            Commands::Metastore(metastore) => metastore.run(),
        }
    }
}
const DEFAULT_PG_BIND_ADDR: &str = "0.0.0.0:6543";
const DEFAULT_RPC_BIND_ADDR: &str = "0.0.0.0:6789";

trait RunCommand {
    fn run(self) -> Result<()>;
}

impl RunCommand for LocalArgs {
    fn run(self) -> Result<()> {
        let runtime = build_runtime("local")?;
        runtime.block_on(async move {
            let query = match (self.file, self.query) {
                (Some(_), Some(_)) => {
                    return Err(anyhow!(
                        "only one of query or an SQL file can be passed at a time"
                    ))
                }
                (Some(file), None) => {
                    if file.to_ascii_lowercase() == "version" {
                        return Err(anyhow!(
                            "'version' is not a valid command, did you mean '--version'?"
                        ));
                    }
                    let path = std::path::Path::new(file.as_str());
                    if !path.exists() {
                        return Err(anyhow!("file '{}' does not exist", file));
                    }

                    Some(tokio::fs::read_to_string(path).await?)
                }
                (None, Some(query)) => Some(query),
                // If no query and it's not a tty, try to read from stdin.
                // Should work with both a query string and a file.
                // echo "select 1;" | ./glaredb
                // ./glaredb < query.sql
                (None, None) if atty::isnt(Stream::Stdin) => {
                    let mut query = String::new();
                    loop {
                        let mut line = String::new();
                        std::io::stdin().read_line(&mut line)?;
                        if line.is_empty() {
                            break;
                        }
                        let path = std::path::Path::new(line.as_str());
                        if path.exists() {
                            let contents = tokio::fs::read_to_string(path).await?;
                            query.push_str(&contents);
                            break;
                        } else {
                            query.push_str(&line);
                        }
                    }

                    Some(query)
                }
                (None, None) => None,
            };

            if query.is_none() {
                println!("GlareDB (v{})", env!("CARGO_PKG_VERSION"));
            }

            let local = LocalSession::connect(self.opts).await?;
            local.run(query).await
        })
    }
}

impl RunCommand for ServerArgs {
    fn run(self) -> Result<()> {
        let Self {
            bind,
            rpc_bind,
            metastore_addr,
            user,
            password,
            data_dir,
            service_account_path,
            storage_config,
            spill_path,
            ignore_pg_auth,
            disable_rpc_auth,
            segment_key,
            enable_simple_query_rpc,
            enable_flight_api,
            disable_postgres_api,
        } = self;

        // Map an empty string to None. Makes writing the terraform easier.
        let segment_key = segment_key.and_then(|s| if s.is_empty() { None } else { Some(s) });

        // If we don't enable the rpc service, then trying to enable the simple
        // interface doesn't make sense.
        if rpc_bind.is_none() && enable_simple_query_rpc {
            return Err(anyhow!(
                "An rpc bind address needs to be provided to enable the simple query interface"
            ));
        }
        if bind.is_some() && disable_postgres_api {
            return Err(anyhow!(
                "Cannot disable the postgres api when a bind address is provided"
            ));
        }

        let auth: Box<dyn LocalAuthenticator> = match password {
            Some(password) => Box::new(SingleUserAuthenticator { user, password }),
            None => Box::new(PasswordlessAuthenticator {
                drop_auth_messages: ignore_pg_auth,
            }),
        };

        let runtime = build_runtime("server")?;

        runtime.block_on(async move {
            let pg_listener = match bind {
                Some(bind) => Some(TcpListener::bind(bind).await?),
                None if disable_postgres_api => None,
                None => Some(TcpListener::bind(DEFAULT_PG_BIND_ADDR).await?),
            };
            let rpc_listener = match rpc_bind {
                Some(bind) => Some(TcpListener::bind(bind).await?),
                None if enable_flight_api => Some(TcpListener::bind(DEFAULT_RPC_BIND_ADDR).await?),
                None => None,
            };

            let server = ComputeServer::builder()
                .with_authenticator(auth)
                .with_pg_listener_opt(pg_listener)
                .with_rpc_listener_opt(rpc_listener)
                .with_metastore_addr_opt(metastore_addr)
                .with_segment_key_opt(segment_key)
                .with_data_dir_opt(data_dir)
                .with_service_account_path_opt(service_account_path)
                .with_location_opt(storage_config.location)
                .with_storage_options(HashMap::from_iter(storage_config.storage_options.clone()))
                .with_spill_path_opt(spill_path)
                .disable_rpc_auth(disable_rpc_auth)
                .enable_simple_query_rpc(enable_simple_query_rpc)
                .enable_flight_api(enable_flight_api)
                .connect()
                .await?;

            server.serve().await
        })
    }
}

impl RunCommand for PgProxyArgs {
    fn run(self) -> Result<()> {
        let runtime = build_runtime("pgsrv")?;
        runtime.block_on(async move {
            let pg_listener = TcpListener::bind(self.bind).await?;
            let proxy = PgProxy::new(
                self.cloud_api_addr,
                self.cloud_auth_code,
                self.ssl_server_cert,
                self.ssl_server_key,
            )
            .await?;
            proxy.serve(pg_listener).await
        })
    }
}

impl RunCommand for RpcProxyArgs {
    fn run(self) -> Result<()> {
        let Self {
            bind,
            cloud_api_addr,
            cloud_auth_code,
            tls_mode,
        } = self;

        let runtime = build_runtime("rpcsrv")?;
        runtime.block_on(async move {
            let addr = bind.parse()?;
            let proxy = RpcProxy::new(cloud_api_addr, cloud_auth_code).await?;
            proxy.serve(addr, tls_mode).await
        })
    }
}

impl RunCommand for MetastoreArgs {
    fn run(self) -> Result<()> {
        let Self {
            bind,
            bucket,
            service_account_path,
            local_file_path,
        } = self;
        let conf = match (bucket, service_account_path, local_file_path) {
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
                    "Invalid arguments, 'service-account-path' and 'bucket' must both be provided."
                ))
            }
        };
        let addr: SocketAddr = bind.parse()?;
        let runtime = build_runtime("metastore")?;

        info!(?conf, "starting Metastore with object store config");

        runtime.block_on(async move {
            let store = conf.new_object_store()?;
            let metastore = Metastore::new(store)?;
            metastore.serve(addr).await
        })
    }
}

fn build_runtime(thread_label: &'static str) -> Result<Runtime> {
    let runtime = Builder::new_multi_thread()
        .thread_name_fn(move || {
            static THREAD_ID: AtomicU64 = AtomicU64::new(0);
            let id = THREAD_ID.fetch_add(1, Ordering::Relaxed);
            format!("{}-thread-{}", thread_label, id)
        })
        .enable_all()
        .build()?;

    Ok(runtime)
}
