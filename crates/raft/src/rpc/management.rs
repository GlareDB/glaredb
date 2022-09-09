use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use super::{
    pb::{
        raft_node_server::RaftNode, AddLearnerRequest, AddLearnerResponse, ChangeMembershipRequest,
        ChangeMembershipResponse, MetricsResponse,
    },
    TonicResult,
};

use crate::repr::NodeId;
use crate::{repr::Node, server::app::ApplicationState};

#[derive(Clone)]
pub struct ManagementRpcHandler {
    app: Arc<ApplicationState>,
}

impl ManagementRpcHandler {
    pub fn new(app: Arc<ApplicationState>) -> Self {
        Self { app }
    }
}

#[tonic::async_trait]
impl RaftNode for ManagementRpcHandler {
    async fn init(&self, _request: tonic::Request<()>) -> TonicResult<()> {
        let mut nodes = BTreeMap::new();
        let node = Node {
            address: self.app.address.clone(),
        };

        nodes.insert(self.app.id, node);
        match self.app.raft.initialize(nodes).await {
            Ok(resp) => Ok(tonic::Response::new(resp)),
            Err(e) => Err(tonic::Status::new(tonic::Code::Internal, e.to_string())),
        }
    }

    async fn add_learner(
        &self,
        request: tonic::Request<AddLearnerRequest>,
    ) -> TonicResult<AddLearnerResponse> {
        let req = request.into_inner();
        let node = Node {
            address: req.address,
        };

        let resp = self.app.raft.add_learner(req.node_id, node, true).await;

        let data = bincode::serialize(&resp).unwrap();
        Ok(tonic::Response::new(AddLearnerResponse { payload: data }))
    }

    async fn change_membership(
        &self,
        request: tonic::Request<ChangeMembershipRequest>,
    ) -> TonicResult<ChangeMembershipResponse> {
        let req: BTreeSet<NodeId> = bincode::deserialize(&request.into_inner().payload).unwrap();
        let resp = self.app.raft.change_membership(req, true, false).await;

        let data = bincode::serialize(&resp).unwrap();
        Ok(tonic::Response::new(ChangeMembershipResponse {
            payload: data,
        }))
    }

    async fn metrics(&self, _request: tonic::Request<()>) -> TonicResult<MetricsResponse> {
        let metrics = self.app.raft.metrics().borrow().clone();

        match metrics.try_into() {
            Ok(resp) => Ok(tonic::Response::new(resp)),
            Err(e) => Err(tonic::Status::new(tonic::Code::Internal, e.to_string())),
        }
    }
}
