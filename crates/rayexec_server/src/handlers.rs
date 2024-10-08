use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use rayexec_error::{RayexecError, ResultExt};
use rayexec_execution::engine::server_state::ServerState;
use rayexec_execution::engine::Engine;
use rayexec_execution::hybrid::client::{
    HybridExecuteRequest,
    HybridExecuteResponse,
    HybridFinalizeRequest,
    HybridFinalizeResponse,
    HybridPlanRequest,
    HybridPullRequest,
    HybridPullResponse,
    HybridPushRequest,
    HybridPushResponse,
    RequestEnvelope,
    ResponseEnvelope,
};
use rayexec_execution::proto::DatabaseProtoConv;
use rayexec_proto::prost::Message;
use rayexec_proto::ProtoConv;
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};

use crate::errors::ServerResult;

/// State that's passed to all handlers.
#[derive(Debug)]
pub struct HandlerState {
    /// Engine responsible for planning and executing queries.
    pub engine: Engine<ThreadedNativeExecutor, NativeRuntime>,
    /// Holds query and execution states for everything on the server.
    pub server_state: ServerState<ThreadedNativeExecutor, NativeRuntime>,
}

pub async fn healthz(State(_): State<Arc<HandlerState>>) -> &'static str {
    "OK"
}

pub async fn remote_plan_rpc(
    State(state): State<Arc<HandlerState>>,
    Json(body): Json<RequestEnvelope>,
) -> ServerResult<Json<ResponseEnvelope>> {
    // TODO: The flow here can possibly be confusing. We're create a context
    // here to allow us to properly decode the plan request (since it may
    // include functions).
    //
    // After we decode, we then extend the context based on what the query needs
    // by adding databases to the context based on what's provided in the bind
    // data.
    //
    // However, this means that if we want to properly support functions that
    // aren't included in the system catalog, there will need to be an extra
    // step to get the from the client somehow.
    let context = state.engine.new_base_database_context()?;

    let msg = HybridPlanRequest::from_proto_ctx(
        Message::decode(body.encoded_msg.as_slice()).context("failed to decode message")?,
        &context,
    )?;

    let resp = state
        .server_state
        .plan_partially_bound(context, msg.statement, msg.resolve_context)
        .await?;

    // TODO: Weird. Needed since we're encoding an intermediate plan which may
    // contain function references. As above, it we plan support functions
    // outside the system catalog, we'll need to use a real context.
    let stub_context = state.engine.new_base_database_context()?;

    let resp = ResponseEnvelope {
        encoded_msg: resp.to_proto_ctx(&stub_context)?.encode_to_vec(),
    };

    Ok(Json(resp))
}

pub async fn remote_execute_rpc(
    State(state): State<Arc<HandlerState>>,
    Json(body): Json<RequestEnvelope>,
) -> ServerResult<Json<ResponseEnvelope>> {
    let msg = HybridExecuteRequest::from_proto(
        Message::decode(body.encoded_msg.as_slice()).context("failed to decode message")?,
    )?;

    state.server_state.execute_pending(msg.query_id)?;

    Ok(Json(ResponseEnvelope {
        encoded_msg: HybridExecuteResponse {}.to_proto()?.encode_to_vec(),
    }))
}

pub async fn push_batch_rpc(
    State(state): State<Arc<HandlerState>>,
    Json(body): Json<RequestEnvelope>,
) -> ServerResult<Json<ResponseEnvelope>> {
    let msg = HybridPushRequest::from_proto(
        Message::decode(body.encoded_msg.as_slice()).context("failed to decode message")?,
    )?;
    if msg.partition != 0 {
        return Err(RayexecError::new(format!("Invalid partition: {}", msg.partition)).into());
    }

    state
        .server_state
        .push_batch_for_stream(msg.stream_id, msg.batch.0)?;

    Ok(Json(ResponseEnvelope {
        encoded_msg: HybridPushResponse {}.to_proto()?.encode_to_vec(),
    }))
}

pub async fn finalize_rpc(
    State(state): State<Arc<HandlerState>>,
    Json(body): Json<RequestEnvelope>,
) -> ServerResult<Json<ResponseEnvelope>> {
    let msg = HybridFinalizeRequest::from_proto(
        Message::decode(body.encoded_msg.as_slice()).context("failed to decode message")?,
    )?;
    if msg.partition != 0 {
        return Err(RayexecError::new(format!("Invalid partition: {}", msg.partition)).into());
    }

    state.server_state.finalize_stream(msg.stream_id)?;

    Ok(Json(ResponseEnvelope {
        encoded_msg: HybridFinalizeResponse {}.to_proto()?.encode_to_vec(),
    }))
}

pub async fn pull_batch_rpc(
    State(state): State<Arc<HandlerState>>,
    Json(body): Json<RequestEnvelope>,
) -> ServerResult<Json<ResponseEnvelope>> {
    let msg = HybridPullRequest::from_proto(
        Message::decode(body.encoded_msg.as_slice()).context("failed to decode message")?,
    )?;
    if msg.partition != 0 {
        return Err(RayexecError::new(format!("Invalid partition: {}", msg.partition)).into());
    }

    let status = state.server_state.pull_batch_for_stream(msg.stream_id)?;

    Ok(Json(ResponseEnvelope {
        encoded_msg: HybridPullResponse { status }.to_proto()?.encode_to_vec(),
    }))
}
