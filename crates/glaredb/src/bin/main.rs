use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use glaredb::local::{LocalClientOpts, LocalSession};
use glaredb::metastore::Metastore;
use glaredb::proxy::Proxy;
use glaredb::server::{Server, ServerConfig};
use object_store::local::LocalFileSystem;
use object_store::{gcp::GoogleCloudStorageBuilder, memory::InMemory, ObjectStore};
use pgsrv::auth::{LocalAuthenticator, PasswordlessAuthenticator, SingleUserAuthenticator};
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::runtime::{Builder, Runtime};
use tracing::info;

#[derive(Parser)]
#[clap(name = "GlareDB")]
#[clap(version)]
#[clap(about = "CLI for GlareDB", long_about = None)]
struct Cli {
    /// Log verbosity.
    #[clap(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    /// Output logs in json format.
    #[clap(long)]
    json_logging: bool,

    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Starts a local version of GlareDB.
    Local {
        /// Execute a query, exiting upon completion.
        ///
        /// Multiple statements may be provided, and results will be printed out
        /// one after another.
        #[clap(short, long, value_parser)]
        query: Option<String>,

        #[clap(flatten)]
        opts: LocalClientOpts,
    },

    /// Starts the sql server portion of GlareDB.
    Server {
        /// TCP address to bind to.
        #[clap(short, long, value_parser, default_value_t = String::from("0.0.0.0:6543"))]
        bind: String,

        /// Address to the Metastore.
        ///
        /// If not provided and `local` is set to a true, an in-process
        /// metastore will be started.
        #[clap(short, long, value_parser)]
        metastore_addr: Option<String>,

        /// Set the user used for authentication.
        ///
        /// Only has an affect if a password is also provided. If a password is
        /// not provided, the GlareDB server will not prompt for a password.
        #[clap(short, long, value_parser, default_value_t = String::from("glaredb"))]
        user: String,

        /// Set the password used for authentication.
        ///
        /// If unset, the GlareDB server will not prompt for a password.
        #[clap(short, long, value_parser)]
        password: Option<String>,

        /// Optional file path to store metastore data (to enable persistent
        /// data storage when in-process store is launched).
        #[clap(short = 'f', long, value_parser)]
        local_file_path: Option<PathBuf>,

        /// Path to spill temporary files to.
        #[clap(long, value_parser)]
        spill_path: Option<PathBuf>,

        /// Ignore authentication messages.
        ///
        /// (Internal)
        ///
        /// This is only relevant for internal development. The postgres
        /// protocol proxy will drop all authentication related messages.
        #[clap(long, value_parser)]
        ignore_auth: bool,

        /// API key for segment.
        ///
        /// (Internal)
        #[clap(long, value_parser)]
        segment_key: Option<String>,
    },

    /// Starts an instance of the pgsrv proxy.
    Proxy {
        /// TCP address to bind to.
        #[clap(short, long, value_parser, default_value_t = String::from("0.0.0.0:6544"))]
        bind: String,

        /// Path to SSL server cert to use.
        #[clap(long)]
        ssl_server_cert: Option<String>,

        /// Path to SSL server key to use.
        #[clap(long)]
        ssl_server_key: Option<String>,

        /// Address of the GlareDB cloud server.
        #[clap(long)]
        cloud_api_addr: String,

        /// Authorization code for communicating with Cloud.
        #[clap(long)]
        cloud_auth_code: String,
    },

    /// Starts an instance of the Metastore.
    Metastore {
        /// TCP address to bind do.
        #[clap(short, long, value_parser, default_value_t = String::from("0.0.0.0:6545"))]
        bind: String,

        /// Bucket to use for database catalogs.
        #[clap(short = 'u', long, value_parser)]
        bucket: Option<String>,

        /// Path to GCP service account to use when connecting to GCS.
        #[clap(short, long, value_parser)]
        service_account_path: Option<String>,

        /// Local file path to store database catalog (for a local persistent
        /// store).
        #[clap(short = 'f', long, value_parser)]
        local_file_path: Option<PathBuf>,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Disable logging when running locally since it'll clobber the repl
    // _unless_ the user specified a logging related option.
    match (&cli.command, cli.json_logging, cli.verbose) {
        (Commands::Local { .. }, false, 0) => (),
        _ => logutil::init(cli.verbose, cli.json_logging),
    }

    info!(version = env!("CARGO_PKG_VERSION"), "starting...");

    match cli.command {
        Commands::Local { query, opts } => {
            let runtime = build_runtime("local")?;
            runtime.block_on(async move {
                let local = LocalSession::connect(opts).await?;
                match query {
                    Some(q) => local.execute_one(&q).await,
                    None => local.run_interactive().await,
                }
            })?;
        }
        Commands::Server {
            bind,
            metastore_addr,
            user,
            password,
            local_file_path,
            mut segment_key,
            spill_path,
            ignore_auth,
        } => {
            // Map an empty string to None. Makes writing the terraform easier.
            segment_key = segment_key.and_then(|s| if s.is_empty() { None } else { Some(s) });

            let auth: Box<dyn LocalAuthenticator> = match password {
                Some(password) => Box::new(SingleUserAuthenticator { user, password }),
                None => Box::new(PasswordlessAuthenticator {
                    drop_auth_messages: ignore_auth,
                }),
            };

            begin_server(
                &bind,
                metastore_addr,
                segment_key,
                auth,
                local_file_path,
                spill_path,
            )?;
        }
        Commands::Proxy {
            bind,
            ssl_server_cert,
            ssl_server_key,
            cloud_api_addr,
            cloud_auth_code,
        } => {
            let runtime = build_runtime("pgsrv")?;
            runtime.block_on(async move {
                let pg_listener = TcpListener::bind(bind).await?;
                let proxy = Proxy::new(
                    cloud_api_addr,
                    cloud_auth_code,
                    ssl_server_cert,
                    ssl_server_key,
                )
                .await?;
                proxy.serve(pg_listener).await
            })?;
        }
        Commands::Metastore {
            bind,
            bucket,
            service_account_path,
            local_file_path,
        } => {
            let conf = match (bucket, service_account_path, local_file_path) {
                (Some(bucket), Some(service_account_path), None) => ObjectStoreConfig::Gcs {
                    bucket,
                    service_account_path,
                },
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

                    ObjectStoreConfig::Local(p)
                }
                (None, None, None) => ObjectStoreConfig::Memory,
                _ => {
                    return Err(anyhow!(
                    "Invalid arguments, 'service-account-path' and 'bucket' must both be provided."
                ))
                }
            };
            begin_metastore(&bind, conf)?
        }
    }

    Ok(())
}

fn begin_server(
    pg_bind: &str,
    metastore_addr: Option<String>,
    segment_key: Option<String>,
    authenticator: Box<dyn LocalAuthenticator>,
    local_file_path: Option<PathBuf>,
    spill_path: Option<PathBuf>,
) -> Result<()> {
    let runtime = build_runtime("server")?;
    runtime.block_on(async move {
        let pg_listener = TcpListener::bind(pg_bind).await?;
        let conf = ServerConfig { pg_listener };
        let server = Server::connect(
            metastore_addr,
            segment_key,
            authenticator,
            local_file_path,
            spill_path,
            /* integration_testing = */ false,
        )
        .await?;
        server.serve(conf).await
    })
}

#[derive(Debug)]
enum ObjectStoreConfig {
    Memory,
    Local(PathBuf),
    Gcs {
        bucket: String,
        service_account_path: String,
    },
}

impl ObjectStoreConfig {
    fn into_object_store(self) -> Result<Arc<dyn ObjectStore>> {
        Ok(match self {
            ObjectStoreConfig::Memory => Arc::new(InMemory::new()),
            ObjectStoreConfig::Local(path) => Arc::new(LocalFileSystem::new_with_prefix(path)?),
            ObjectStoreConfig::Gcs {
                bucket,
                service_account_path,
            } => Arc::new(
                GoogleCloudStorageBuilder::new()
                    .with_bucket_name(bucket)
                    .with_service_account_path(service_account_path)
                    .build()?,
            ),
        })
    }
}

fn begin_metastore(bind: &str, conf: ObjectStoreConfig) -> Result<()> {
    let addr: SocketAddr = bind.parse()?;
    let runtime = build_runtime("metastore")?;

    info!(?conf, "starting Metastore with object store config");

    runtime.block_on(async move {
        let store = conf.into_object_store()?;
        let metastore = Metastore::new(store)?;
        metastore.serve(addr).await
    })
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
