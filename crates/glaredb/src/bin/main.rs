use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use glaredb::server::Server;
use lemur::execute::stream::source::{DataSource, MemoryDataSource};
use rustyline::{error::ReadlineError, Editor};
use sqlengine::engine::Engine;
use std::sync::atomic::{AtomicU64, Ordering};
use storageengine::rocks::{RocksStore, StorageConfig};
use tokio::net::ToSocketAddrs;
use tokio::runtime::Builder;
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
        } => match storage {
            StorageBackend::Memory => begin_server(MemoryDataSource::new(), &bind)?,
            StorageBackend::Rocks => match data_path {
                Some(path) => {
                    let conf = StorageConfig { data_dir: path };
                    let source = RocksStore::open(conf)?;
                    begin_server(source, &bind)?;
                }
                None => {
                    error!("`data-path` arg required with RocksDB");
                }
            },
        },
        Commands::Client { addr } => {
            // TODO: Eventually there will be some "management" client. E.g.
            // adding nodes to the cluster, graceful shutdowns, etc.
            error!("client not implemented");
        }
    }

    Ok(())
}

fn begin_server<S>(source: S, bind: &str) -> Result<()>
where
    S: DataSource + 'static,
{
    let runtime = Builder::new_multi_thread()
        .thread_name_fn(|| {
            static THREAD_ID: AtomicU64 = AtomicU64::new(0);
            let id = THREAD_ID.fetch_add(1, Ordering::Relaxed);
            format!("glaredb-thread-{}", id)
        })
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let mut engine = Engine::new(source);
        match engine.ensure_system_tables().await {
            Ok(_) => info!("system tables ensured"),
            Err(e) => {
                error!(%e, "failed to ensure system tables, exiting...");
                return;
            }
        }
        let server = Server::new(engine);
        match server.serve(bind).await {
            Ok(_) => info!("server exiting"),
            Err(e) => error!(%e, "server exited with error"),
        }
    });
    Ok(())
}
