use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use cloudwal::wal::Wal;
use glaredb::server::{Server, ServerConfig};
use lemur::execute::stream::source::{DataSource, MemoryDataSource};
use object_store::gcp::GoogleCloudStorageBuilder;
use std::sync::atomic::{AtomicU64, Ordering};
use storageengine::rocks::{RocksStore, StorageConfig};
use tokio::net::TcpListener;
use tokio::runtime::{Builder, Runtime};
use tracing::{error, info};

#[derive(Parser)]
#[clap(name = "GlareDB")]
#[clap(version = "pre-release")]
#[clap(about = "CLI for GlareDB", long_about = None)]
struct Cli {
    #[clap(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Starts the sql server portion of GlareDB.
    Server {
        /// TCP address to bind to.
        #[clap(short, long, value_parser, default_value_t = String::from("0.0.0.0:6543"))]
        bind: String,

        /// Storage backend.
        #[clap(long, value_enum)]
        storage: StorageBackend,

        /// Directory for storing data.
        ///
        /// Applicable only with the RocksDB storage backend.
        #[clap(long, value_parser)]
        data_path: Option<String>,

        #[clap(long, value_parser)]
        cloud_wal_bucket: Option<String>,

        #[clap(long, value_parser)]
        cloud_wal_service_account: Option<String>,
    },

    /// Starts a client to some server.
    Client {
        /// Address of server to connect to.
        #[clap(value_parser)]
        addr: String,
    },
}

#[derive(ValueEnum, Clone)]
enum StorageBackend {
    Memory,
    Rocks,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    logutil::init(cli.verbose);

    match cli.command {
        Commands::Server {
            bind,
            storage,
            data_path,
            cloud_wal_bucket,
            cloud_wal_service_account,
        } => match storage {
            StorageBackend::Memory => {
                begin_server(
                    MemoryDataSource::new(),
                    &bind,
                    cloud_wal_bucket,
                    cloud_wal_service_account,
                )?;
            }
            StorageBackend::Rocks => match data_path {
                Some(path) => {
                    let storage_conf = StorageConfig { data_dir: path };
                    let source = RocksStore::open(storage_conf)?;
                    begin_server(source, &bind, cloud_wal_bucket, cloud_wal_service_account)?;
                }
                None => {
                    error!("`data-path` arg required with RocksDB");
                }
            },
        },
        Commands::Client { .. } => {
            // TODO: Eventually there will be some "management" client. E.g.
            // adding nodes to the cluster, graceful shutdowns, etc.
            error!("client not implemented");
        }
    }

    Ok(())
}

fn begin_server<S>(
    source: S,
    pg_bind: &str,
    wal_bucket: Option<String>,
    wal_sa: Option<String>,
) -> Result<()>
where
    S: DataSource + 'static,
{
    let runtime = build_runtime()?;
    runtime.block_on(async move {
        let pg_listener = TcpListener::bind(pg_bind).await?;
        let conf = ServerConfig { pg_listener };

        if let (Some(bucket), Some(sa)) = (wal_bucket, wal_sa) {
            info!("enabling cloud wal");

            let store = GoogleCloudStorageBuilder::new()
                .with_bucket_name(bucket)
                .with_service_account_path(sa)
                .build()?;
            let source = Wal::open(source, store, "wal").await?;

            let server = Server::connect(source).await?;
            server.serve(conf).await
        } else {
            let server = Server::connect(source).await?;
            server.serve(conf).await
        }
    })
}

fn build_runtime() -> Result<Runtime> {
    let runtime = Builder::new_multi_thread()
        .thread_name_fn(|| {
            static THREAD_ID: AtomicU64 = AtomicU64::new(0);
            let id = THREAD_ID.fetch_add(1, Ordering::Relaxed);
            format!("glaredb-thread-{}", id)
        })
        .enable_all()
        .build()?;
    Ok(runtime)
}
