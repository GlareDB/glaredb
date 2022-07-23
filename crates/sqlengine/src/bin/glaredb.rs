use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use lemur::execute::stream::source::MemoryDataSource;
use rustyline::{error::ReadlineError, Editor};
use sqlengine::engine::Engine;
use sqlengine::server::{Client, Response, Server};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::ToSocketAddrs;
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
        Commands::Server { bind } => {
            let runtime = Builder::new_multi_thread()
                .thread_name_fn(|| {
                    static THREAD_ID: AtomicU64 = AtomicU64::new(0);
                    let id = THREAD_ID.fetch_add(1, Ordering::Relaxed);
                    format!("glaredb-thread-{}", id)
                })
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let source = MemoryDataSource::new();
                let engine = Engine::new(source);
                let server = Server::new(engine);
                match server.serve(&bind).await {
                    Ok(_) => info!("server exiting"),
                    Err(e) => error!("server exited with error: {}", e),
                }
            });
        }
        Commands::Client { addr } => {
            let runtime = Builder::new_current_thread().enable_all().build()?;
            runtime.block_on(async move {
                let repl = Repl::new(addr).await?;
                repl.run().await
            })?;
        }
    }

    Ok(())
}

struct Repl {
    client: Client,
    editor: Editor<()>,
}

impl Repl {
    async fn new<A: ToSocketAddrs>(addr: A) -> Result<Repl> {
        let client = Client::connect(addr).await?;
        let editor = Editor::<()>::new()?;
        Ok(Repl { client, editor })
    }

    async fn handle_input(&mut self, input: String) -> Result<()> {
        let resp = self.client.execute(input).await?;
        for result in resp.into_iter() {
            // Lazy
            println!("{:?}", result);
        }
        Ok(())
    }

    async fn run(mut self) -> Result<()> {
        println!("connected to GlareDB");
        loop {
            let readline = self.editor.readline("glaredb> ");
            match readline {
                Ok(line) => match self.handle_input(line).await {
                    Ok(_) => (),
                    Err(e) => println!("error: {}", e),
                },
                Err(ReadlineError::Interrupted) => {
                    println!("CTRL-C");
                    break;
                }
                Err(ReadlineError::Eof) => {
                    println!("CTRL-D");
                    break;
                }
                Err(err) => return Err(err.into()),
            }
        }
        Ok(())
    }
}
