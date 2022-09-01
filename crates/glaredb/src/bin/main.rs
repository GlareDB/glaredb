use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use glaredb::server::{Server, ServerConfig};
use lemur::execute::stream::source::{DataSource, MemoryDataSource};
use raft::server::start_raft_node;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use storageengine::rocks::{RocksStore, StorageConfig};
use tokio::net::TcpListener;
use tokio::runtime::{Builder, Runtime};
use tracing::error;

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
    },

    /// Starts a client to some server.
    Client {
        /// Address of server to connect to.
        #[clap(value_parser)]
        addr: String,
    },

    /// Starts the sql server portion of GlareDB, using a cluster of raft nodes.
    Cluster {
        /// TCP address to bind to.
        #[clap(long, value_parser, default_value_t = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 22001))]
        address: SocketAddr,

        /// Directory for storing data.
        data_path: String,

        /// leader node address.
        #[clap(long, value_parser)]
        leader: Option<String>,
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
        } => match storage {
            StorageBackend::Memory => {
                begin_server(MemoryDataSource::new(), &bind)?;
            }
            StorageBackend::Rocks => match data_path {
                Some(path) => {
                    let storage_conf = StorageConfig { data_dir: path };
                    let source = RocksStore::open(storage_conf)?;
                    begin_server(source, &bind)?;
                }
                None => {
                    error!("`data-path` arg required with RocksDB");
                }
            },
        },
        Commands::Cluster {
            data_path,
            leader: _,
            address,
        } => {
            // let storage_conf = StorageConfig { data_dir: data_path };
            // let source = RocksStore::open(storage_conf)?;
            let rt = tokio::runtime::Runtime::new()?;

            rt.block_on(async {
                start_raft_node(
                    0,
                    data_path,
                    format!("http://{}", address),
                    address,
                ).await.expect("raft node");
            });
        }
        Commands::Client { .. } => {
            // TODO: Eventually there will be some "management" client. E.g.
            // adding nodes to the cluster, graceful shutdowns, etc.
            error!("client not implemented");
        }
    }

    Ok(())
}

fn begin_server<S>(source: S, pg_bind: &str) -> Result<()>
where
    S: DataSource + 'static,
{
    let runtime = build_runtime()?;
    runtime.block_on(async move {
        let pg_listener = TcpListener::bind(pg_bind).await?;
        let conf = ServerConfig { pg_listener };
        let server = Server::connect(source).await?;
        server.serve(conf).await
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
