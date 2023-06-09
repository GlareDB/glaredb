use anyhow::{anyhow, Result};
use async_trait::async_trait;
use clap::{Parser, Subcommand};
use glaredb::server::{Server, ServerConfig};
use glob::glob;
use pgsrv::auth::SingleUserAuthenticator;
use regex::{Captures, Regex};
use sqllogictest::{
    parse_with_name, AsyncDB, ColumnType, DBOutput, DefaultColumnType, Injected, Record, Runner,
};
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

        /// Address of metastore to use.
        ///
        /// If not provided, a Metastore will be spun up automatically.
        #[clap(long, value_parser)]
        metastore_addr: Option<String>,

        /// Whether or not to keep the GlareDB server running after a failure.
        ///
        /// This allow for an external client to connect to allow for additional
        /// debugging.
        #[clap(long, value_parser)]
        keep_running: bool,

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
            metastore_addr,
            keep_running,
            ..
        } => runtime.block_on(async move {
            let pg_listener =
                TcpListener::bind(bind.unwrap_or_else(|| "localhost:0".to_string())).await?;
            let pg_addr = pg_listener.local_addr()?;
            let server_conf = ServerConfig { pg_listener };

            let server = Server::connect(
                metastore_addr,
                None,
                Box::new(SingleUserAuthenticator {
                    user: "glaredb".to_string(),
                    password: "glaredb".to_string(),
                }),
                // Run the SLT runner with in-memory metastore.
                None,
                // Spill-path:
                None,
                // Integration testing:
                true,
            )
            .await?;
            tokio::spawn(server.serve(server_conf));

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

    const ENV_REGEX: &str = r"\$\{\s*(\w+)\s*\}";

    /// Execute all test files, returning after the first error.
    ///
    /// All tests are ran sequentially.
    async fn exec_tests(mut self, files: &[PathBuf]) -> Result<Duration> {
        let regx = Regex::new(Self::ENV_REGEX).unwrap();
        let start = Instant::now();
        let mut runner = Runner::new(self.client);
        for file in files {
            let records = parse_file(&regx, file)?;
            runner
                .run_multi_async(records)
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

fn parse_file<T: ColumnType>(regx: &Regex, path: &PathBuf) -> Result<Vec<Record<T>>> {
    let script = std::fs::read_to_string(path)
        .map_err(|e| anyhow!("error while opening '{}': {}", path.to_string_lossy(), e))?;

    // Replace all occurances of ${some_env_var} with actual values
    // from the environment.
    let script = regx.replace_all(&script, |caps: &Captures| {
        let env_var = &caps[1];
        match std::env::var(env_var) {
            Ok(v) => v,
            Err(error) => {
                tracing::error!(%error, %env_var, "error fetching env variable");
                format!("<error fetching env variable '{}': {}>", env_var, error)
            }
        }
    });

    let mut records = vec![];

    let script_name = path.to_str().unwrap();
    let parsed_records = parse_with_name(&script, script_name).map_err(|e| {
        anyhow!(
            "error while parsing '{}': {}",
            path.to_string_lossy(),
            e.kind()
        )
    })?;

    for rec in parsed_records {
        records.push(rec);

        // What we just pushed
        let rec = records.last().unwrap();

        // Includes are not actually processed by the runner. It's more of a
        // pre-processor, so we process them during the parse stage.
        //
        // This code was borrowed from `parse_file` function since the inner
        // function is private.

        if let Record::Include { filename, .. } = rec {
            let complete_filename = {
                let mut path_buf = path.to_path_buf();
                path_buf.pop();
                path_buf.push(filename.clone());
                path_buf.as_os_str().to_string_lossy().to_string()
            };

            for included_file in glob::glob(&complete_filename)
                .map_err(|e| anyhow!("Invalid include file at {}: {}", path.to_string_lossy(), e))?
                .filter_map(Result::ok)
            {
                let included_file = included_file.as_os_str().to_string_lossy().to_string();

                records.push(Record::Injected(Injected::BeginInclude(
                    included_file.clone(),
                )));
                records.extend(parse_file(regx, &PathBuf::from(&included_file))?);
                records.push(Record::Injected(Injected::EndInclude(included_file)));
            }
        }
    }
    Ok(records)
}

struct TestClient {
    client: Client,
}

#[async_trait]
impl AsyncDB for TestClient {
    type Error = tokio_postgres::Error;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let mut output = Vec::new();
        let mut num_columns = 0;
        let rows = self.client.simple_query(sql).await?;
        for row in rows {
            match row {
                SimpleQueryMessage::Row(row) => {
                    num_columns = row.len();
                    let mut row_output = Vec::with_capacity(row.len());
                    for i in 0..row.len() {
                        match row.get(i) {
                            Some(v) => {
                                if v.is_empty() {
                                    row_output.push("(empty)".to_string());
                                } else {
                                    row_output.push(v.to_string());
                                }
                            }
                            None => row_output.push("NULL".to_string()),
                        }
                    }
                    output.push(row_output);
                }
                SimpleQueryMessage::CommandComplete(_) => {}
                _ => unreachable!(),
            }
        }

        if output.is_empty() && num_columns == 0 {
            Ok(DBOutput::StatementComplete(0))
        } else {
            Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text; num_columns],
                rows: output,
            })
        }
    }

    fn engine_name(&self) -> &str {
        "glaredb"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}
