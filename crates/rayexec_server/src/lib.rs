pub mod errors;
pub mod handlers;

use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;
use rayexec_error::{Result, ResultExt};
use rayexec_execution::engine::Engine;
use rayexec_execution::hybrid::client::REMOTE_ENDPOINTS;
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

/// Starts the server portion for an engine on the given port.
///
/// This is expected to be ran inside a tokio runtime.
pub async fn serve_with_engine(
    engine: Engine<ThreadedNativeExecutor, NativeRuntime>,
    port: u16,
) -> Result<()> {
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

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .context("failed to bind port")?;

    info!(%port, "starting server");

    axum::serve(listener, app)
        .await
        .context("failed to begin serving")?;

    Ok(())
}
