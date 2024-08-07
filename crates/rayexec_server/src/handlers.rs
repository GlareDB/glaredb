use std::sync::Arc;

use axum::{extract::State, Json};
use rayexec_error::{RayexecError, ResultExt};
use rayexec_execution::{
    engine::{server_session::ServerSession, Engine},
    hybrid::client::{
        HybridExecuteRequest, HybridExecuteResponse, HybridFinalizeRequest, HybridFinalizeResponse,
        HybridPlanRequest, HybridPullRequest, HybridPullResponse, HybridPushRequest,
        HybridPushResponse, RequestEnvelope, ResponseEnvelope,
    },
    proto::DatabaseProtoConv,
};
use rayexec_proto::{prost::Message, ProtoConv};
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};

use crate::errors::ServerResult;

/// State that's passed to all handlers.
#[derive(Debug)]
pub struct ServerState {
    /// Engine responsible for planning and executing queries.
    pub _engine: Engine<ThreadedNativeExecutor, NativeRuntime>,

    // TODO: Don't have just one.
    // TODO: Why not?
    pub session: ServerSession<ThreadedNativeExecutor, NativeRuntime>,
}

pub async fn healthz(State(_): State<Arc<ServerState>>) -> &'static str {
    "OK"
}

pub async fn remote_plan_rpc(
    State(state): State<Arc<ServerState>>,
    Json(body): Json<RequestEnvelope>,
) -> ServerResult<Json<ResponseEnvelope>> {
    let context = state.session.context();
    let msg = HybridPlanRequest::from_proto_ctx(
        Message::decode(body.encoded_msg.as_slice()).context("failed to decode message")?,
        context,
    )?;

    let resp = state
        .session
        .plan_partially_bound(msg.statement, msg.bind_data)
        .await?;

    let resp = ResponseEnvelope {
        encoded_msg: resp.to_proto_ctx(context)?.encode_to_vec(),
    };

    Ok(Json(resp))
}

pub async fn remote_execute_rpc(
    State(state): State<Arc<ServerState>>,
    Json(body): Json<RequestEnvelope>,
) -> ServerResult<Json<ResponseEnvelope>> {
    let msg = HybridExecuteRequest::from_proto(
        Message::decode(body.encoded_msg.as_slice()).context("failed to decode message")?,
    )?;

    state.session.execute_pending(msg.query_id)?;

    Ok(Json(ResponseEnvelope {
        encoded_msg: HybridExecuteResponse {}.to_proto()?.encode_to_vec(),
    }))
}

pub async fn push_batch_rpc(
    State(state): State<Arc<ServerState>>,
    Json(body): Json<RequestEnvelope>,
) -> ServerResult<Json<ResponseEnvelope>> {
    let msg = HybridPushRequest::from_proto(
        Message::decode(body.encoded_msg.as_slice()).context("failed to decode message")?,
    )?;
    if msg.partition != 0 {
        return Err(RayexecError::new(format!("Invalid partition: {}", msg.partition)).into());
    }

    state
        .session
        .push_batch_for_stream(msg.stream_id, msg.batch.0)?;

    Ok(Json(ResponseEnvelope {
        encoded_msg: HybridPushResponse {}.to_proto()?.encode_to_vec(),
    }))
}

pub async fn finalize_rpc(
    State(state): State<Arc<ServerState>>,
    Json(body): Json<RequestEnvelope>,
) -> ServerResult<Json<ResponseEnvelope>> {
    let msg = HybridFinalizeRequest::from_proto(
        Message::decode(body.encoded_msg.as_slice()).context("failed to decode message")?,
    )?;
    if msg.partition != 0 {
        return Err(RayexecError::new(format!("Invalid partition: {}", msg.partition)).into());
    }

    state.session.finalize_stream(msg.stream_id)?;

    Ok(Json(ResponseEnvelope {
        encoded_msg: HybridFinalizeResponse {}.to_proto()?.encode_to_vec(),
    }))
}

pub async fn pull_batch_rpc(
    State(state): State<Arc<ServerState>>,
    Json(body): Json<RequestEnvelope>,
) -> ServerResult<Json<ResponseEnvelope>> {
    let msg = HybridPullRequest::from_proto(
        Message::decode(body.encoded_msg.as_slice()).context("failed to decode message")?,
    )?;
    if msg.partition != 0 {
        return Err(RayexecError::new(format!("Invalid partition: {}", msg.partition)).into());
    }

    let status = state.session.pull_batch_for_stream(msg.stream_id)?;

    Ok(Json(ResponseEnvelope {
        encoded_msg: HybridPullResponse { status }.to_proto()?.encode_to_vec(),
    }))
}
