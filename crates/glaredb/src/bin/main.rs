use anyhow::Result;
use clap::{Parser, Subcommand};
use common::config::DbConfig;
use glaredb::proxy::Proxy;
use glaredb::server::{Server, ServerConfig};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpListener;
use tokio::runtime::{Builder, Runtime};
use tracing::info;

#[derive(Parser)]
#[clap(name = "GlareDB")]
#[clap(version = "pre-release")]
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
    /// Starts the sql server portion of GlareDB.
    Server {
        /// TCP address to bind to.
        #[clap(short, long, value_parser, default_value_t = String::from("0.0.0.0:6543"))]
        bind: String,

        /// Name of the database to connect to.
        #[clap(short, long, value_parser)]
        db_name: Option<String>,

        /// Path to config file
        #[clap(short, long, value_parser)]
        config: Option<String>,
    },

    Proxy {
        /// TCP address to bind to.
        #[clap(short, long, value_parser, default_value_t = String::from("0.0.0.0:6544"))]
        bind: String,

        /// Path to SSL server cert to use.
        // TODO: This and the key path should be provided by a config.
        #[clap(long)]
        ssl_server_cert: Option<String>,

        /// Path to SSL server key to use.
        #[clap(long)]
        ssl_server_key: Option<String>,

        /// Address of the GlareDB cloud server.
        api_addr: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    logutil::init(cli.verbose, cli.json_logging);

    let version = buildenv::git_tag();
    info!(%version, "starting...");

    match cli.command {
        Commands::Server {
            bind,
            db_name,
            config,
        } => {
            // Use clap values as default
            let config: DbConfig = DbConfig::base(config)
                .set_override_option("access.db_name", db_name)?
                .build()?
                .try_deserialize()?;

            begin_server(config, &bind)?;
        }
        Commands::Proxy {
            bind,
            ssl_server_cert,
            ssl_server_key,
            api_addr,
        } => {
            let runtime = build_runtime()?;
            runtime.block_on(async move {
                let pg_listener = TcpListener::bind(bind).await?;
                let proxy = Proxy::new(api_addr, ssl_server_cert, ssl_server_key).await?;
                proxy.serve(pg_listener).await
            })?;
        }
    }

    Ok(())
}

fn begin_server(config: DbConfig, pg_bind: &str) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(async move {
        let pg_listener = TcpListener::bind(pg_bind).await?;
        let conf = ServerConfig { pg_listener };
        let server = Server::connect(config).await?;
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
