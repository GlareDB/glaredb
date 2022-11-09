use anyhow::Result;
use clap::{Parser, Subcommand};
use glaredb::proxy::Proxy;
use glaredb::server::{Server, ServerConfig};
use raft::client::ConsensusClient;
use raft::repr::NodeId;
use raft::rpc::pb::AddLearnerRequest;
use raft::server::start_raft_node;
use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpListener;
use tokio::runtime::{Builder, Runtime};

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
        #[clap(short, long, value_parser, default_value_t = String::from("glaredb"))]
        db_name: String,

        /// Type of object storage database will access
        #[clap(short, long, value_parser, default_value_t = String::from("local"))]
        object_store: String,
    },

    /// Starts a client to some server.
    Client {
        /// Address of server to connect to.
        #[clap(value_parser)]
        addr: String,

        #[clap(subcommand)]
        command: ClientCommands,
    },

    /// Starts the sql server portion of GlareDB, using a cluster of raft nodes.
    RaftNode {
        /// TCP port to bind to.
        #[clap(long, value_parser, default_value_t = 6000)]
        port: u16,

        /// leader node address.
        #[clap(long, value_parser)]
        leader: Option<String>,

        /// node id.
        #[clap(long, value_parser)]
        node_id: u64,
    },

    Proxy {
        /// TCP address to bind to.
        #[clap(short, long, value_parser, default_value_t = String::from("0.0.0.0:6544"))]
        bind: String,

        /// Address of the GlareDB cloud server.
        api_addr: String,
    },
}

#[derive(Subcommand)]
enum ClientCommands {
    Init,
    AddLearner {
        #[clap(short, long)]
        address: String,

        #[clap(short, long)]
        node_id: NodeId,
    },
    ChangeMembership {
        // TODO: add a command to change membership
        membership: Vec<NodeId>,
    },
    Metrics,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    logutil::init(cli.verbose, cli.json_logging);

    match cli.command {
        Commands::RaftNode {
            leader: _,
            port,
            node_id,
        } => {
            let rt = tokio::runtime::Runtime::new()?;

            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port);
            let url = format!("http://127.0.0.1:{}", port);

            rt.block_on(async {
                start_raft_node(node_id, url, addr)
                    .await
                    .expect("raft node");
            });
        }
        Commands::Server {
            bind,
            db_name,
            object_store,
        } => {
            begin_server(db_name, &bind, &object_store)?;
        }
        Commands::Client { addr, command } => {
            let rt = tokio::runtime::Runtime::new()?;

            rt.block_on(async {
                let client = ConsensusClient::new(1, addr).await.expect("client");

                match command {
                    ClientCommands::Init => {
                        client.init().await.expect("failed to init cluster");
                    }
                    ClientCommands::AddLearner { address, node_id } => {
                        client
                            .add_learner(AddLearnerRequest { address, node_id })
                            .await
                            .expect("failed to add learner");
                    }
                    ClientCommands::ChangeMembership { membership } => {
                        let new_membership = BTreeSet::from_iter(membership);
                        client
                            .change_membership(&new_membership)
                            .await
                            .expect("failed to change membership");
                    }
                    ClientCommands::Metrics => {
                        let metrics = client.metrics().await.expect("failed to get metrics");
                        println!("{:?}", metrics);
                    }
                }
            });
        }
        Commands::Proxy { bind, api_addr } => {
            let runtime = build_runtime()?;
            runtime.block_on(async move {
                let pg_listener = TcpListener::bind(bind).await?;
                let proxy = Proxy::new(api_addr).await?;
                proxy.serve(pg_listener).await
            })?;
        }
    }

    Ok(())
}

fn begin_server(db_name: impl Into<String>, pg_bind: &str, object_store: &str) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(async move {
        let pg_listener = TcpListener::bind(pg_bind).await?;
        let conf = ServerConfig { pg_listener };
        let server = Server::connect(db_name, object_store).await?;
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
