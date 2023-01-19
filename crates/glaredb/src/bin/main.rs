use anyhow::Result;
use clap::{Parser, Subcommand};
use common::config::DbConfig;
use glaredb::metastore::Metastore;
use glaredb::proxy::Proxy;
use glaredb::server::{Server, ServerConfig};
use logutil::Verbosity;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpListener;
use tokio::runtime::{Builder, Runtime};
use tracing::{error, info};

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

        /// Whether or not this instance is running locally.
        ///
        /// When not set, the postgres protocol handler will expect additional
        /// parameters to found on the startup message for each connection.
        /// These additional params are set by the pgsrv proxy.
        ///
        /// When set to true, these additional params are not expected.
        #[clap(short, long, value_parser)]
        local: bool,
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
        api_addr: String,
    },

    /// Starts an instance of the Metastore.
    Metastore {
        /// TCP address to bind do.
        #[clap(short, long, value_parser, default_value_t = String::from("0.0.0.0:6545"))]
        bind: String,
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
            local,
        } => {
            // Use clap values as default
            let config: DbConfig = DbConfig::base(config)
                .set_override_option("access.db_name", db_name)?
                .build()?
                .try_deserialize()?;

            begin_server(config, &bind, local)?;
        }
        Commands::Proxy {
            bind,
            ssl_server_cert,
            ssl_server_key,
            api_addr,
        } => {
            let runtime = build_runtime("pgsrv")?;
            runtime.block_on(async move {
                let pg_listener = TcpListener::bind(bind).await?;
                let proxy = Proxy::new(api_addr, ssl_server_cert, ssl_server_key).await?;
                proxy.serve(pg_listener).await
            })?;
        }
        Commands::Metastore { bind } => begin_metastore(&bind)?,
    }

    Ok(())
}

fn begin_server(config: DbConfig, pg_bind: &str, local: bool) -> Result<()> {
    let runtime = build_runtime("server")?;
    runtime.block_on(async move {
        let pg_listener = TcpListener::bind(pg_bind).await?;
        let conf = ServerConfig { pg_listener };
        let server = Server::connect(config, local).await?;
        server.serve(conf).await
    })
}

fn begin_metastore(bind: &str) -> Result<()> {
    let addr: SocketAddr = bind.parse()?;
    let runtime = build_runtime("metastore")?;
    runtime.block_on(async move {
        let metastore = Metastore::new()?;
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
