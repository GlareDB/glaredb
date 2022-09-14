use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use glaredb::server::{Server, ServerConfig};
use std::sync::atomic::{AtomicU64, Ordering};
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

        /// Name of the database to connect to.
        #[clap(short, long, value_parser, default_value_t = String::from("glaredb"))]
        db_name: String,
    },

    /// Starts a client to some server.
    Client {
        /// Address of server to connect to.
        #[clap(value_parser)]
        addr: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    logutil::init(cli.verbose);

    match cli.command {
        Commands::Server { bind, db_name } => {
            begin_server(db_name, &bind)?;
        }
        Commands::Client { .. } => {
            // TODO: Eventually there will be some "management" client. E.g.
            // adding nodes to the cluster, graceful shutdowns, etc.
            error!("client not implemented");
        }
    }

    Ok(())
}

fn begin_server(db_name: impl Into<String>, pg_bind: &str) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(async move {
        let pg_listener = TcpListener::bind(pg_bind).await?;
        let conf = ServerConfig { pg_listener };
        let server = Server::connect(db_name).await?;
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
