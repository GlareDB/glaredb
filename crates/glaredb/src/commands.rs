use crate::args::server::ServerArgs;
use crate::args::{LocalArgs, MetastoreArgs, PgProxyArgs, RpcProxyArgs};
use crate::local::LocalSession;
use crate::metastore::Metastore;
use crate::pg_proxy::PgProxy;
use crate::rpc_proxy::RpcProxy;
use crate::server::{ComputeServer, ServerConfig};
use anyhow::{anyhow, Result};
use atty::Stream;
use clap::Subcommand;
use object_store_util::conf::StorageConfig;
use pgsrv::auth::{LocalAuthenticator, PasswordlessAuthenticator, SingleUserAuthenticator};
use std::collections::HashMap;
use std::fs;
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

        let auth: Box<dyn LocalAuthenticator> = match password {
            Some(password) => Box::new(SingleUserAuthenticator { user, password }),
            None => Box::new(PasswordlessAuthenticator {
                drop_auth_messages: ignore_pg_auth,
            }),
        };

        let runtime = build_runtime("server")?;
        runtime.block_on(async move {
            let pg_listener = TcpListener::bind(bind).await?;
            let conf = ServerConfig {
                pg_listener,
                rpc_addr: rpc_bind.map(|s| s.parse()).transpose()?,
            };
            let server = ComputeServer::connect(
                metastore_addr,
                segment_key,
                auth,
                data_dir,
                service_account_path,
                storage_config.location,
                HashMap::from_iter(storage_config.storage_options.clone()),
                spill_path,
                /* integration_testing = */ false,
                disable_rpc_auth,
                enable_simple_query_rpc,
            )
            .await?;
            server.serve(conf).await
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
                // Error if the path exists and is not a directory else
                // create the directory.
                if p.exists() && !p.is_dir() {
                    return Err(anyhow!(
                        "Path '{}' is not a valid directory",
                        p.to_string_lossy()
                    ));
                } else if !p.exists() {
                    fs::create_dir_all(&p)?;
                }

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
