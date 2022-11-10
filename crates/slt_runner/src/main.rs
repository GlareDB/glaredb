use anyhow::{anyhow, Result};
use async_trait::async_trait;
use clap::{Parser, Subcommand};
use common::config::{DbConfig, CONFIG};
use glaredb::server::{Server, ServerConfig};
use glob::glob;
use sqllogictest::{AsyncDB, Runner};
use std::fmt::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio_postgres::{Client, Config as ClientConfig, NoTls, SimpleQueryMessage};

#[derive(Parser)]
#[clap(name = "slt_runner")]
#[clap(about = "Run sqllogictests against a GlareDB server", long_about = None)]
struct Cli {
    #[clap(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start an embedded GlareDB server to execute the tests against.
    Embedded {
        /// TCP address to bind to for the GlareDB server.
        ///
        /// Omitting this will attempt to bind to any available port.
        #[clap(long, value_parser)]
        bind: Option<String>,

        /// Whether or not to keep the GlareDB server running after a failure.
        ///
        /// This allow for an external client to connect to allow for additional
        /// debugging.
        #[clap(long, value_parser)]
        keep_running: bool,

        /// Name of the database to connect to.
        #[clap(short, long, value_parser)]
        db_name: Option<String>,

        /// Path to config file
        #[clap(short, long, value_parser)]
        config: Option<String>,

        /// Path to test files.
        files: Vec<String>,
    },

    /// Connect to a remote instance to execute tests against.
    External {
        /// Connection string to use for connecting to the database.
        #[clap(short, long, value_parser)]
        connection_str: String,

        /// Path to test files.
        files: Vec<String>,
    },
}

impl Commands {
    fn collect_globbed_files(&self) -> Result<Vec<PathBuf>> {
        let files = match self {
            Commands::Embedded { files, .. } => files.clone(),
            Commands::External { files, .. } => files.clone(),
        };
        Ok(files
            .into_iter()
            .map(|s| glob(&s))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Result<Vec<_>, _>>()?)
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    logutil::init(cli.verbose, false);

    let files = cli.command.collect_globbed_files()?;
    if files.is_empty() {
        println!("no files, exiting...");
        return Ok(());
    }

    let runtime = Builder::new_multi_thread().enable_all().build()?;
    match cli.command {
        Commands::Embedded {
            bind,
            keep_running,
            db_name,
            config,
            ..
        } => runtime.block_on(async move {
            let pg_listener =
                TcpListener::bind(bind.unwrap_or_else(|| "localhost:0".to_string())).await?;
            let pg_addr = pg_listener.local_addr()?;
            let conf = ServerConfig { pg_listener };

            // Use clap values as default
            let config: DbConfig = DbConfig::base(config)
                .set_override_option("access.db_name", db_name)?
                .build()?
                .try_deserialize()?;

            let config = CONFIG.get_or_init(|| config);

            let server = Server::connect(config).await?;
            let _ = tokio::spawn(server.serve(conf));

            let runner = TestRunner::connect_embedded(pg_addr).await?;
            match runner.exec_tests(&files).await {
                Ok(taken) => {
                    println!("tests completed in {:?}", taken);
                    Ok(())
                }
                Err(e) => {
                    if keep_running {
                        eprintln!("{}", e);
                        println!("keeping the server running, addr: {}", pg_addr);
                        println!("CTRL-C to exit");
                        tokio::signal::ctrl_c().await?;
                    }
                    Err(e)
                }
            }
        }),
        Commands::External { connection_str, .. } => runtime.block_on(async move {
            let runner = TestRunner::connect_external(&connection_str).await?;
            let taken = runner.exec_tests(&files).await?;
            println!("tests completed in {:?}", taken);
            Ok(())
        }),
    }
}

struct TestRunner {
    client: TestClient,
    conn_err: oneshot::Receiver<Result<(), tokio_postgres::Error>>,
}

impl TestRunner {
    /// Connect to an embedded database at the given addr.
    async fn connect_embedded(pg_addr: SocketAddr) -> Result<TestRunner> {
        let host = pg_addr.ip().to_string();
        let port = pg_addr.port();
        let (client, conn) = ClientConfig::new()
            .user("glaredb")
            .password("glaredb")
            .dbname("glaredb")
            .host(&host)
            .port(port)
            .connect(NoTls)
            .await?;

        let (conn_err_tx, conn_err_rx) = oneshot::channel();
        tokio::spawn(async move { conn_err_tx.send(conn.await) });

        Ok(TestRunner {
            client: TestClient { client },
            conn_err: conn_err_rx,
        })
    }

    async fn connect_external(conn: &str) -> Result<TestRunner> {
        let (client, conn) = tokio_postgres::connect(conn, NoTls).await?;
        let (conn_err_tx, conn_err_rx) = oneshot::channel();
        tokio::spawn(async move { conn_err_tx.send(conn.await) });

        Ok(TestRunner {
            client: TestClient { client },
            conn_err: conn_err_rx,
        })
    }

    /// Execute all test files, returning after the first error.
    ///
    /// All tests are ran sequentially.
    async fn exec_tests(mut self, files: &[PathBuf]) -> Result<Duration> {
        let start = Instant::now();
        let mut runner = Runner::new(self.client);
        for file in files {
            runner
                .run_file_async(file)
                .await
                .map_err(|e| anyhow!("test fail: {}", e))?;
            if let Ok(result) = self.conn_err.try_recv() {
                match result {
                    Ok(()) => return Err(anyhow!("client connection unexpectedly closed")),
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Ok(Instant::now().duration_since(start))
    }
}

struct TestClient {
    client: Client,
}

#[async_trait]
impl AsyncDB for TestClient {
    type Error = tokio_postgres::Error;

    async fn run(&mut self, sql: &str) -> Result<String, Self::Error> {
        let mut output = String::new();
        let rows = self.client.simple_query(sql).await?;
        for row in rows {
            match row {
                SimpleQueryMessage::Row(row) => {
                    for i in 0..row.len() {
                        if i != 0 {
                            write!(output, " ").unwrap();
                        }
                        match row.get(i) {
                            Some(v) => {
                                if v.is_empty() {
                                    write!(output, "(empty)").unwrap()
                                } else {
                                    write!(output, "{}", v).unwrap()
                                }
                            }
                            None => write!(output, "NULL").unwrap(),
                        }
                    }
                }
                SimpleQueryMessage::CommandComplete(_) => {}
                _ => unreachable!(),
            }
            writeln!(output).unwrap();
        }
        Ok(output)
    }

    fn engine_name(&self) -> &str {
        "glaredb"
    }
}
