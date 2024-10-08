use clap::{Parser, ValueEnum};
use rayexec_csv::CsvDataSource;
use rayexec_delta::DeltaDataSource;
use rayexec_error::Result;
use rayexec_execution::datasource::{DataSourceBuilder, DataSourceRegistry, MemoryDataSource};
use rayexec_execution::engine::Engine;
use rayexec_execution::runtime::{Runtime, TokioHandlerProvider};
use rayexec_parquet::ParquetDataSource;
use rayexec_postgres::PostgresDataSource;
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use rayexec_server::serve_with_engine;

#[derive(Parser)]
#[clap(name = "rayexec_server")]
struct Arguments {
    /// Port to start the server on.
    #[clap(short, long, default_value_t = 8080)]
    port: u16,

    /// Log format.
    #[arg(value_enum, long, value_parser, default_value_t = LogFormat::Json)]
    log_format: LogFormat,
}

#[derive(Debug, Clone, Copy, Default, ValueEnum)]
enum LogFormat {
    #[default]
    Json,
    Pretty,
}

fn main() -> Result<()> {
    let args = Arguments::parse();
    logutil::configure_global_logger(
        tracing::Level::DEBUG,
        match args.log_format {
            LogFormat::Json => logutil::LogFormat::Json,
            LogFormat::Pretty => logutil::LogFormat::HumanReadable,
        },
    );

    let sched = ThreadedNativeExecutor::try_new()?;
    let runtime = NativeRuntime::with_default_tokio()?;
    let tokio_handle = runtime
        .tokio_handle()
        .handle()
        .expect("tokio to be configured");

    let registry = DataSourceRegistry::default()
        .with_datasource("memory", Box::new(MemoryDataSource))?
        .with_datasource("postgres", PostgresDataSource::initialize(runtime.clone()))?
        .with_datasource("delta", DeltaDataSource::initialize(runtime.clone()))?
        .with_datasource("parquet", ParquetDataSource::initialize(runtime.clone()))?
        .with_datasource("csv", CsvDataSource::initialize(runtime.clone()))?;
    let engine = Engine::new_with_registry(sched.clone(), runtime.clone(), registry)?;

    tokio_handle.block_on(async move { serve_with_engine(engine, args.port).await })
}
