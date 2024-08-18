mod errors;
mod handlers;

use axum::{
    routing::{get, post},
    Router,
};
use clap::{Parser, ValueEnum};
use rayexec_csv::CsvDataSource;
use rayexec_delta::DeltaDataSource;
use rayexec_error::{Result, ResultExt};
use rayexec_execution::{
    datasource::{DataSourceBuilder, DataSourceRegistry, MemoryDataSource},
    engine::Engine,
    hybrid::client::REMOTE_ENDPOINTS,
    runtime::{Runtime, TokioHandlerProvider},
};
use rayexec_parquet::ParquetDataSource;
use rayexec_postgres::PostgresDataSource;
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use std::sync::Arc;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::info;

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

fn main() {
    let args = Arguments::parse();
    logutil::configure_global_logger(
        tracing::Level::DEBUG,
        match args.log_format {
            LogFormat::Json => logutil::LogFormat::Json,
            LogFormat::Pretty => logutil::LogFormat::HumanReadable,
        },
    );

    let sched = ThreadedNativeExecutor::try_new().unwrap();
    let runtime = NativeRuntime::with_default_tokio().unwrap();
    let tokio_handle = runtime
        .tokio_handle()
        .handle()
        .expect("tokio to be configured");

    let runtime_clone = runtime.clone();
    let result = tokio_handle.block_on(async move { inner(args, sched, runtime_clone).await });

    if let Err(e) = result {
        println!("ERROR: {e}");
        std::process::exit(1);
    }
}

async fn inner(
    args: Arguments,
    sched: ThreadedNativeExecutor,
    runtime: NativeRuntime,
) -> Result<()> {
    let registry = DataSourceRegistry::default()
        .with_datasource("memory", Box::new(MemoryDataSource))?
        .with_datasource("postgres", PostgresDataSource::initialize(runtime.clone()))?
        .with_datasource("delta", DeltaDataSource::initialize(runtime.clone()))?
        .with_datasource("parquet", ParquetDataSource::initialize(runtime.clone()))?
        .with_datasource("csv", CsvDataSource::initialize(runtime.clone()))?;
    let engine = Engine::new_with_registry(sched.clone(), runtime.clone(), registry)?;
    let server_state = engine.new_server_state()?;

    let state = Arc::new(handlers::HandlerState {
        engine,
        server_state,
    });

    let app = Router::new()
        .route(REMOTE_ENDPOINTS.healthz, get(handlers::healthz))
        .route(
            REMOTE_ENDPOINTS.rpc_hybrid_plan,
            post(handlers::remote_plan_rpc),
        )
        .route(
            REMOTE_ENDPOINTS.rpc_hybrid_execute,
            post(handlers::remote_execute_rpc),
        )
        .route(
            REMOTE_ENDPOINTS.rpc_hybrid_push,
            post(handlers::push_batch_rpc),
        )
        .route(
            REMOTE_ENDPOINTS.rpc_hybrid_finalize,
            post(handlers::finalize_rpc),
        )
        .route(
            REMOTE_ENDPOINTS.rpc_hybrid_pull,
            post(handlers::pull_batch_rpc),
        )
        // TODO: Limit CORS to *.glaredb.com and localhost. And maybe make
        // localhost dev build only.
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", args.port))
        .await
        .context("failed to bind port")?;

    info!(port = %args.port, "starting server");

    axum::serve(listener, app)
        .await
        .context("failed to begin serving")?;

    Ok(())
}
