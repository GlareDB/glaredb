use std::sync::Arc;

use super::{pb::{
    remote_data_source_server::{RemoteDataSource}, BinaryWriteRequest, BinaryWriteResponse,
}, TonicResult};

use crate::{server::app::ApplicationState, message::RequestData};

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
        request: tonic::Request<BinaryWriteRequest>,
    ) -> TonicResult<BinaryWriteResponse> {
        // Decode the request
        let req: RequestData = bincode::deserialize(&request.into_inner().payload).unwrap();
        
        todo!();

        /*
        match self.app.raft.client_write(req).await {
            Ok(resp) => Ok(tonic::Response::new(BinaryWriteResponse {
                payload: bincode::serialize(&resp).unwrap(),
            })),
            Err(e) => Err(tonic::Status::new(tonic::Code::Internal, e.to_string())),
        }
        */
    }
}

