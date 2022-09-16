use anyhow::{anyhow, Result};
use async_trait::async_trait;
use clap::Parser;
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

    /// Path to test files.
    files: Vec<String>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    logutil::init(cli.verbose);

    let files = cli
        .files
        .into_iter()
        .map(|s| glob(&s))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Result<Vec<_>, _>>()?;
    if files.is_empty() {
        println!("no files, exiting...");
        return Ok(());
    }

    let runtime = Builder::new_multi_thread().enable_all().build()?;
    runtime.block_on(async move {
        let pg_listener =
            TcpListener::bind(cli.bind.unwrap_or_else(|| "localhost:0".to_string())).await?;
        let pg_addr = pg_listener.local_addr()?;
        let conf = ServerConfig { pg_listener };

        let server = Server::connect("slt_test").await?;
        let _ = tokio::spawn(server.serve(conf));

        let runner = TestRunner::connect(pg_addr).await?;
        match runner.exec_tests(&files).await {
            Ok(taken) => {
                println!("tests completed in {:?}", taken);
                Ok(())
            }
            Err(e) => {
                if cli.keep_running {
                    eprintln!("{}", e);
                    println!("keeping the server running, addr: {}", pg_addr);
                    println!("CTRL-C to exit");
                    tokio::signal::ctrl_c().await?;
                }
                Err(e)
            }
        }
    })
}

struct TestRunner {
    client: TestClient,
    conn_err: oneshot::Receiver<Result<(), tokio_postgres::Error>>,
}

impl TestRunner {
    /// Connect to the database at the given addr.
    async fn connect(pg_addr: SocketAddr) -> Result<TestRunner> {
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
