use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use atty::Stream;
use clap::Subcommand;
use pgsrv::auth::{LocalAuthenticator, PasswordlessAuthenticator, SingleUserAuthenticator};
use rustls::crypto::{aws_lc_rs, CryptoProvider};
use slt::discovery::SltDiscovery;
use slt::hooks::{
    AllTestsHook,
    DeltaWriteResetHook,
    IcebergFormatVersionHook,
    SqliteTestsHook,
    SshTunnelHook,
};
use slt::tests::{PgBinaryEncoding, SshKeysTest};
use tokio::net::TcpListener;
use tokio::runtime::{Builder, Runtime};

use crate::args::server::ServerArgs;
use crate::args::{LocalArgs, PgProxyArgs, RpcProxyArgs, SltArgs};
use crate::built_info;
use crate::local::LocalSession;
use crate::proxy::{PgProxy, RpcProxy};
use crate::server::ComputeServer;

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
    /// Runs SQL Logic Tests
    #[clap(hide = true, alias = "slt")]
    SqlLogicTests(SltArgs),
}

impl Commands {
    pub fn run(self) -> Result<()> {
        match self {
            Commands::Local(local) => local.run(),
            Commands::Server(server) => server.run(),
            Commands::PgProxy(pg_proxy) => pg_proxy.run(),
            Commands::RpcProxy(rpc_proxy) => rpc_proxy.run(),
            Commands::SqlLogicTests(slt) => slt.run(),
        }
    }
}
const DEFAULT_PG_BIND_ADDR: &str = "0.0.0.0:6543";
const DEFAULT_RPC_BIND_ADDR: &str = "0.0.0.0:6789";

trait RunCommand {
    fn run(self) -> Result<()>;
}

impl LocalArgs {
    #[cfg(not(release))]
    fn start_tokio_debugger(&self) {
        if self.debug_tokio {
            eprintln!("> Starting tokio debugger");
            console_subscriber::init();
        }
    }
}

impl RunCommand for LocalArgs {
    fn run(self) -> Result<()> {
        #[cfg(not(release))]
        self.start_tokio_debugger();

        let runtime = build_runtime("local")?;
        runtime.block_on(async move {
            let query = match self.query {
                Some(q) if q.to_ascii_lowercase() == "version" => {
                    return Err(anyhow!(
                        "'version' is not a valid command, did you mean '--version'?"
                    ))
                }
                Some(q) if q.ends_with(".sql") => {
                    let file = std::path::Path::new(&q);
                    if !file.exists() {
                        return Err(anyhow!("file '{q}' does not exist"));
                    } else {
                        Some(tokio::fs::read_to_string(file).await?)
                    }
                }
                Some(q) => Some(q),
                None if atty::isnt(Stream::Stdin) => {
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
                None => None,
            };

            if query.is_none() {
                // git should always be present, but just in case, we'll fall back to the version
                if let Some(git_version) = built_info::GIT_VERSION {
                    println!("GlareDB ({git_version})",);
                } else {
                    println!("GlareDB (v{})", env!("CARGO_PKG_VERSION"));
                }
            }

            let local = LocalSession::connect(self.opts).await?;
            local.run(query).await
        })
    }
}

impl RunCommand for ServerArgs {
    fn run(self) -> Result<()> {
        // Map an empty string to None. Makes writing the terraform easier.
        let segment_key = self
            .segment_key
            .and_then(|s| if s.is_empty() { None } else { Some(s) });

        // If we don't enable the rpc service, then trying to enable the simple
        // interface doesn't make sense.
        // Clap isn't intelligent enough to handle negative conditions, so we
        // have to manually check.
        if self.rpc_bind.is_none() && self.enable_simple_query_rpc {
            return Err(anyhow!(
                "An rpc bind address needs to be provided to enable the simple query interface"
            ));
        }

        let auth: Box<dyn LocalAuthenticator> = match self.password {
            Some(password) => Box::new(SingleUserAuthenticator {
                user: self.user,
                password,
            }),
            None => Box::new(PasswordlessAuthenticator {
                drop_auth_messages: self.ignore_pg_auth,
            }),
        };

        let runtime = build_runtime("server")?;

        runtime.block_on(async move {
            let pg_listener = match self.bind {
                Some(bind) => Some(TcpListener::bind(bind).await?),
                None if self.disable_postgres_api => None,
                None => Some(TcpListener::bind(DEFAULT_PG_BIND_ADDR).await?),
            };
            let rpc_listener = match self.rpc_bind {
                Some(bind) => Some(TcpListener::bind(bind).await?),
                None if self.enable_flight_api => {
                    Some(TcpListener::bind(DEFAULT_RPC_BIND_ADDR).await?)
                }
                None => None,
            };

            let server = ComputeServer::builder()
                .with_authenticator(auth)
                .with_pg_listener_opt(pg_listener)
                .with_rpc_listener_opt(rpc_listener)
                .with_segment_key_opt(segment_key)
                .with_data_dir_opt(self.data_dir)
                .with_service_account_path_opt(self.service_account_path)
                .with_location_opt(self.storage_config.location)
                .with_storage_options(HashMap::from_iter(
                    self.storage_config.storage_options.clone(),
                ))
                .with_spill_path_opt(self.spill_path)
                .with_metastore_bucket_opt(self.metastore_bucket)
                .disable_rpc_auth(self.disable_rpc_auth)
                .enable_simple_query_rpc(self.enable_simple_query_rpc)
                .enable_flight_api(self.enable_flight_api)
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
            tls_cert_path,
            tls_key_path,
        } = self;

        let runtime = build_runtime("rpcsrv")?;
        runtime.block_on(async move {
            let addr = bind.parse()?;
            let proxy =
                RpcProxy::new(cloud_api_addr, cloud_auth_code, tls_cert_path, tls_key_path).await?;
            proxy.serve(addr, tls_mode).await
        })
    }
}


impl RunCommand for SltArgs {
    fn run(self) -> Result<()> {
        CryptoProvider::install_default(aws_lc_rs::default_provider()).unwrap();

        let disco = SltDiscovery::new()
            .test_files_dir("testdata")?
            // Rust tests
            .test("sqllogictests/ssh_keys", Box::new(SshKeysTest))?
            .test("pgproto/binary_encoding", Box::new(PgBinaryEncoding))?
            // Add hooks
            .hook("*", Arc::new(AllTestsHook))?
            // Sqlite tests
            .hook("sqllogictests_sqlite/*", Arc::new(SqliteTestsHook))?
            // Iceberg format version tests
            .hook(
                "sqllogictests_object_store/local/*",
                Arc::new(DeltaWriteResetHook),
            )?
            .hook(
                "sqllogictests_iceberg/local_v1",
                Arc::new(IcebergFormatVersionHook(1)),
            )?
            .hook(
                "sqllogictests_iceberg/local_v2",
                Arc::new(IcebergFormatVersionHook(2)),
            )?
            // SSH Tunnels hook
            .hook("*/tunnels/ssh", Arc::new(SshTunnelHook))?;

        self.execute(disco.tests, disco.hooks)
    }
}

fn build_runtime(thread_label: &'static str) -> Result<Runtime> {
    CryptoProvider::install_default(aws_lc_rs::default_provider()).unwrap();

    let mut builder = Builder::new_multi_thread();

    // Bump the stack from the default 2MB.
    //
    // We reach the limit when planning a query
    // with nested views.
    //
    // Note that Sean observed the stack size only reaching ~300KB when
    // running in release mode, and so we don't need to bump this
    // everywhere. However there's definitely improvements to stack
    // usage that we can make.
    // see <https://github.com/GlareDB/glaredb/issues/2390>
    #[cfg(not(release))]
    builder.thread_stack_size(4 * 1024 * 1024);

    let runtime = builder
        .thread_name_fn(move || {
            static THREAD_ID: AtomicU64 = AtomicU64::new(0);
            let id = THREAD_ID.fetch_add(1, Ordering::Relaxed);
            format!("{}-thread-{}", thread_label, id)
        })
        .enable_all()
        .build()?;

    Ok(runtime)
}
