use std::sync::Arc;

use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use toy_rpc::macros::export_impl;

use super::{app::ApplicationState, GlareTypeConfig};

pub struct Raft {
    app: Arc<ApplicationState>,
}

#[export_impl]
impl Raft {
    pub fn new(app: Arc<ApplicationState>) -> Self {
        Self { app }
    }

    #[export_method]
    pub async fn append(
        &self,
        req: AppendEntriesRequest<GlareTypeConfig>,
    ) -> Result<AppendEntriesResponse<u64>, toy_rpc::Error> {
        self.app
            .raft
            .append_entries(req)
            .await
            .map_err(|e| toy_rpc::Error::Internal(Box::new(e)))
    }

    #[export_method]
    pub async fn vote(&self, vote: VoteRequest<u64>) -> Result<VoteResponse<u64>, toy_rpc::Error> {
        self.app
            .raft
            .vote(vote)
            .await
            .map_err(|e| toy_rpc::Error::Internal(Box::new(e)))
    }

    #[export_method]
    pub async fn snapshot(
        &self,
        req: InstallSnapshotRequest<GlareTypeConfig>,
    ) -> Result<InstallSnapshotResponse<u64>, toy_rpc::Error> {
        self.app
            .raft
            .install_snapshot(req)
            .await
            .map_err(|e| toy_rpc::Error::Internal(Box::new(e)))
    }
}
