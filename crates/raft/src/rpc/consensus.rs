use std::sync::Arc;

use crate::openraft_types::prelude::*;
use crate::server::app::ApplicationState;

use super::{
    pb::{
        raft_network_server::RaftNetwork, AppendEntriesRequest, AppendEntriesResponse,
        InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    TonicResult,
};

#[derive(Clone)]
pub struct RaftRpcHandler {
    app: Arc<ApplicationState>,
}

impl RaftRpcHandler {
    pub fn new(app: Arc<ApplicationState>) -> Self {
        Self { app }
    }
}

#[tonic::async_trait]
impl RaftNetwork for RaftRpcHandler {
    #[tracing::instrument(skip(self, req))]
    async fn append_entries(
        &self,
        req: tonic::Request<AppendEntriesRequest>,
    ) -> TonicResult<AppendEntriesResponse> {
        let req: OAppendEntriesRequest = req.into_inner().try_into().map_err(|e| {
            tracing::error!(?e, "failed to convert AppendEntriesRequest");
            tonic::Status::invalid_argument("failed to convert AppendEntriesRequest")
        })?;

        match self.app.raft.append_entries(req).await {
            Ok(resp) => Ok(tonic::Response::new(
                resp.try_into().expect("invalid response"),
            )),
            Err(e) => Err(tonic::Status::new(tonic::Code::Internal, e.to_string())),
        }
    }

    #[tracing::instrument(skip(self, req))]
    async fn vote(&self, req: tonic::Request<VoteRequest>) -> TonicResult<VoteResponse> {
        let req: OVoteRequest = req.into_inner().try_into().map_err(|e| {
            tonic::Status::new(tonic::Code::InvalidArgument, format!("invalid request: {}", e))
        })?;

        self.app
            .raft
            .vote(req)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))
            .map(|r| tonic::Response::new(r.try_into().expect("invalid response")))
    }

    #[tracing::instrument(skip(self, req))]
    async fn snapshot(
        &self,
        req: tonic::Request<InstallSnapshotRequest>,
    ) -> TonicResult<InstallSnapshotResponse> {
        let req: OInstallSnapshotRequest = req.into_inner().try_into().map_err(|e| {
            tonic::Status::new(tonic::Code::InvalidArgument, format!("invalid request: {}", e))
        })?;

        self.app
            .raft
            .install_snapshot(req)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))
            .map(|r| tonic::Response::new(r.try_into().expect("invalid response")))
    }
}
