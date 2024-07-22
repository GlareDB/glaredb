use std::sync::Arc;

use axum::{extract::State, Json};
use rayexec_error::ResultExt;
use rayexec_execution::{
    engine::Engine,
    logical::sql::binder::{BindData, BoundStatement},
};
use rayexec_server_client::types::{
    PlanAstRpcRequest, PlanAstRpcResponse, PullBatchRpcRequest, PullBatchRpcResponse,
    PushBatchRcpRequest, PushBatchRpcResponse,
};

use crate::errors::ServerResult;

/// State that's passed to all handlers.
#[derive(Debug)]
pub struct ServerState {
    /// Engine responsible for planning and executing queries.
    pub engine: Engine,
}

pub async fn healthz(State(_): State<Arc<ServerState>>) -> &'static str {
    "OK"
}

pub async fn hybrid_execute_rpc(
    State(state): State<Arc<ServerState>>,
    Json(body): Json<PlanAstRpcRequest>,
) -> ServerResult<Json<PlanAstRpcResponse>> {
    let sess = state.engine.new_server_session()?;

    // TODO: Actual types.
    // let (stmt, data): (BoundStatement, BindData) =
    //     serde_json::from_slice(&body.ast_data).context("failed to deserialize ast")?;

    // let (stmt, data) = sess.complete_binding(stmt, data).await?;
    // let graph = sess.plan_hybrid_graph(stmt, data)?;

    // TODO: Split graph, begin executing.

    Ok(PlanAstRpcResponse {
        hello: "hello".to_string(),
    }
    .into())
}

pub async fn push_batch_rpc(
    State(state): State<Arc<ServerState>>,
    Json(body): Json<PushBatchRcpRequest>,
) -> ServerResult<Json<PushBatchRpcResponse>> {
    unimplemented!()
}

pub async fn pull_batch_rpc(
    State(state): State<Arc<ServerState>>,
    Json(body): Json<PullBatchRpcRequest>,
) -> ServerResult<Json<PullBatchRpcResponse>> {
    unimplemented!()
}
