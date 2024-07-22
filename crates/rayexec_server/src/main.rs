#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

mod errors;
mod handlers;

use axum::{
    routing::{get, post},
    Router,
};
use clap::Parser;
use rayexec_csv::CsvDataSource;
use rayexec_error::{Result, ResultExt};
use rayexec_execution::{
    datasource::{DataSourceRegistry, MemoryDataSource},
    engine::Engine,
    runtime::ExecutionRuntime,
};
use rayexec_parquet::ParquetDataSource;
use rayexec_postgres::PostgresDataSource;
use rayexec_rt_native::runtime::ThreadedExecutionRuntime;
use rayexec_server_client::ENDPOINTS;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tracing::info;

#[derive(Parser)]
#[clap(name = "rayexec_server")]
struct Arguments {
    /// Port to start the server on.
    #[clap(short, long, default_value_t = 8080)]
    port: u16,
}

fn main() {
    let args = Arguments::parse();
    logutil::configure_global_logger(tracing::Level::DEBUG);

    let runtime = Arc::new(
        ThreadedExecutionRuntime::try_new()
            .unwrap()
            .with_default_tokio()
            .unwrap(),
    );
    let tokio_handle = runtime.tokio_handle().expect("tokio to be configured");

    let runtime_clone = runtime.clone();
    let result = tokio_handle.block_on(async move { inner(args, runtime_clone).await });

    if let Err(e) = result {
        println!("ERROR: {e}");
        std::process::exit(1);
    }
}

async fn inner(args: Arguments, runtime: Arc<dyn ExecutionRuntime>) -> Result<()> {
    let registry = DataSourceRegistry::default()
        .with_datasource("memory", Box::new(MemoryDataSource))?
        .with_datasource("postgres", Box::new(PostgresDataSource))?
        .with_datasource("parquet", Box::new(ParquetDataSource))?
        .with_datasource("csv", Box::new(CsvDataSource))?;
    let engine = Engine::new_with_registry(runtime.clone(), registry)?;

    let state = Arc::new(handlers::ServerState { engine });

    let app = Router::new()
        .route(ENDPOINTS.healthz, get(handlers::healthz))
        .route(ENDPOINTS.rpc_hybrid_run, post(handlers::hybrid_execute_rpc))
        .route(ENDPOINTS.rpc_hybrid_push, post(handlers::push_batch_rpc))
        .route(ENDPOINTS.rpc_hybrid_pull, post(handlers::pull_batch_rpc))
        // TODO: Limit CORS to *.glaredb.com and localhost. And maybe make
        // localhost dev build only.
        .layer(CorsLayer::permissive())
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
