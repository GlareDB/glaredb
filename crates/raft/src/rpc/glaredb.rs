use std::sync::Arc;

use super::{
    pb::{
        remote_data_source_server::RemoteDataSource, BinaryReadRequest, BinaryReadResponse,
        BinaryWriteRequest, BinaryWriteResponse,
    },
    TonicResult,
};

use crate::{
    message::{ReadTxRequest, Request},
    server::app::ApplicationState,
};

#[derive(Clone)]
pub struct GlaredbRpcHandler {
    app: Arc<ApplicationState>,
}

impl GlaredbRpcHandler {
    pub fn new(app: Arc<ApplicationState>) -> Self {
        Self { app }
    }
}

#[tonic::async_trait]
impl RemoteDataSource for GlaredbRpcHandler {
    async fn write(
        &self,
        req: tonic::Request<BinaryWriteRequest>,
    ) -> TonicResult<BinaryWriteResponse> {
        let req: Request = bincode::deserialize(&req.into_inner().payload).map_err(|e| {
            tonic::Status::new(
                tonic::Code::InvalidArgument,
                format!("invalid request: {}", e),
            )
        })?;

        match self.app.raft.client_write(req).await {
            Ok(resp) => Ok(tonic::Response::new(BinaryWriteResponse {
                payload: bincode::serialize(&resp).unwrap(),
            })),
            Err(e) => Err(tonic::Status::new(tonic::Code::Internal, e.to_string())),
        }
    }

    async fn read(
        &self,
        req: tonic::Request<BinaryReadRequest>,
    ) -> TonicResult<BinaryReadResponse> {
        let _req: ReadTxRequest = bincode::deserialize(&req.into_inner().payload).map_err(|e| {
            tonic::Status::new(
                tonic::Code::InvalidArgument,
                format!("invalid request: {}", e),
            )
        })?;

        let _state_machine = self.app.store.state_machine.read().await;

        todo!();
    }
}
