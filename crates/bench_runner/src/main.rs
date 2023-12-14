use anyhow::Result;
use clap::Parser;
use glaredb::server::ComputeServer;
use glob::glob;
use pgsrv::auth::SingleUserAuthenticator;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use tokio::net::TcpListener;
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio_postgres::{Client, Config as ClientConfig, NoTls};

#[derive(Parser)]
#[clap(name = "bench_runner")]
#[clap(about = "Run SQL benchmarks against a running GlareDB", long_about = None)]
struct Cli {
    /// TCP address to bind to for the GlareDB server.
    ///
    /// Omitting this will attempt to bind to any available port.
    #[clap(long, value_parser)]
    bind: Option<String>,

    /// File containing sql statements for loading data.
    #[clap(long)]
    load: String,

    /// Number of times to run each benchmark query.
    #[clap(long, default_value_t = 1)]
    runs: usize,

    /// Path to benchmark files.
    files: Vec<String>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

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

        let server = ComputeServer::builder()
            .with_authenticator(SingleUserAuthenticator {
                user: "glaredb".to_string(),
                password: "glaredb".to_string(),
            })
            .with_pg_listener(pg_listener)
            .connect()
            .await?;

        tokio::spawn(server.serve());

        let mut runner = BenchRunner::connect(pg_addr).await?;
        let load_path = tokio::fs::read_to_string(PathBuf::from(cli.load)).await?;
        runner.load_sql(load_path).await?;

        for file in files {
            let sql = tokio::fs::read_to_string(&file).await?;
            let name = file.file_name().unwrap().to_str().unwrap().to_string();
            runner.bench_sql(name, sql, cli.runs).await?;
        }

        runner.print_stats();

        Ok::<(), anyhow::Error>(())
    })?;

    Ok(())
}

struct BenchStat {
    name: String,
    run: usize,
    dur: Duration,
}

struct BenchRunner {
    client: Client,
    _conn_err: oneshot::Receiver<Result<(), tokio_postgres::Error>>,

    stats: Vec<BenchStat>,
}

impl BenchRunner {
    /// Connect to a database at the given addr.
    async fn connect(pg_addr: SocketAddr) -> Result<BenchRunner> {
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

        Ok(BenchRunner {
            client,
            _conn_err: conn_err_rx,
            stats: Vec::new(),
        })
    }

    async fn load_sql(&mut self, sql: String) -> Result<()> {
        self.client.simple_query(&sql).await?;
        Ok(())
    }

    async fn bench_sql(&mut self, name: String, sql: String, num_runs: usize) -> Result<()> {
        for i in 0..num_runs {
            let start = SystemTime::now();
            self.client.simple_query(&sql).await?;
            let dur = SystemTime::now().duration_since(start)?;

            self.stats.push(BenchStat {
                name: name.clone(),
                run: i + 1,
                dur,
            });
        }
        Ok(())
    }

    fn print_stats(&self) {
        println!("{0: <15}, {1: <15}, {2: <15}", "name", "run", "duration");
        for stat in &self.stats {
            println!(
                "{0: <15}, {1: <15}, {2: <15}",
                &stat.name,
                &stat.run,
                &stat.dur.as_secs_f64()
            );
        }

        let total = self
            .stats
            .iter()
            .fold(Duration::ZERO, |acc, stat| acc + stat.dur);
        eprintln!("Total: {0}s", total.as_secs_f64());
    }
}
