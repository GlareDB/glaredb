use crate::clustercom::client::{ClusterClient, ClusterClientError};
use crate::clustercom::proto::clustercom::MembershipChangeRequest;
use crate::clustercom::proto::clustercom::MembershipChangeResponse;
use crate::{
    clustercom::proto::clustercom::cluster_com_service_server::ClusterComService,
    clustercom::proto::clustercom::{
        emit_database_event_request::Event, EmitDatabaseEventRequest, EmitDatabaseEventResponse,
    },
    engine::Engine,
};
use async_trait::async_trait;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("Invalid database id: {0:?}")]
    InvalidDatabaseId(Vec<u8>),

    #[error("Missing emit event message")]
    MissingEmitEventMessage,

    #[error(transparent)]
    ExecError(#[from] crate::errors::ExecError),

    #[error(transparent)]
    ClusterClient(#[from] ClusterClientError),
}

impl From<ServiceError> for tonic::Status {
    fn from(value: ServiceError) -> Self {
        let status = tonic::Status::from_error(Box::new(value));
        status
    }
}

/// Handle cross-node messages.
pub struct ClusterService {
    engine: Arc<Engine>,
    client: ClusterClient,
}

impl ClusterService {
    /// Create a new cluster service to handle incoming requests from other
    /// nodes.
    ///
    /// Accepts a cluster client to allow for reconfuration of membership based
    /// on requests from other nodes.
    pub fn new(engine: Arc<Engine>, client: ClusterClient) -> Self {
        ClusterService { engine, client }
    }
}

#[async_trait]
impl ClusterComService for ClusterService {
    async fn emit_database_event(
        &self,
        request: Request<EmitDatabaseEventRequest>,
    ) -> Result<Response<EmitDatabaseEventResponse>, Status> {
        let req = request.into_inner();
        let id =
            Uuid::from_slice(&req.db_id).map_err(|_| ServiceError::InvalidDatabaseId(req.db_id))?;

        match req.event {
            Some(Event::CatalogMutated(_)) => self
                .engine
                .maybe_refresh_catalog(id)
                .await
                .map_err(|e| ServiceError::ExecError(e))?,
            None => return Err(ServiceError::MissingEmitEventMessage.into()),
        }

        Ok(Response::new(EmitDatabaseEventResponse {}))
    }

    async fn membership_change(
        &self,
        request: Request<MembershipChangeRequest>,
    ) -> Result<Response<MembershipChangeResponse>, Status> {
        let req = request.into_inner();

        self.client
            .membership_change(req.to_add, req.to_remove)
            .await
            .map_err(|e| ServiceError::ClusterClient(e))?;

        Ok(Response::new(MembershipChangeResponse {}))
    }
}
