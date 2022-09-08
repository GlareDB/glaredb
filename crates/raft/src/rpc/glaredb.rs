use std::sync::Arc;

use super::{pb::{
    remote_data_source_server::{RemoteDataSource}, BinaryWriteRequest, BinaryWriteResponse, GetSchemaRequest, BinaryReadResponse, BinaryReadRequest,
}, TonicResult};

use crate::{server::app::ApplicationState, message::{Request, Response, ReadTxRequest, ReadTxResponse, ScanRequest}};

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
        let req: Request = bincode::deserialize(&req.into_inner().payload).unwrap();

        match self.app.raft.client_write(req).await {
            Ok(resp) => Ok(tonic::Response::new(BinaryWriteResponse { payload: bincode::serialize(&resp).unwrap() })),
            Err(e) => Err(tonic::Status::new(tonic::Code::Internal, e.to_string())),
        }
    }

    async fn read(
        &self,
        req: tonic::Request<BinaryReadRequest>,
    ) -> TonicResult<BinaryReadResponse> {
        let req: ReadTxRequest = bincode::deserialize(&req.into_inner().payload).unwrap();

        let state_machine = self.app.store.state_machine.read().await;

        let resp = match req {
            ReadTxRequest::GetSchema(GetSchemaRequest { table }) => {
                let schema = state_machine.get_schema(&table).await.unwrap();

                ReadTxResponse::TableSchema(schema)
            }
            ReadTxRequest::Scan(ScanRequest { table, filter }) => {
                let stream = state_machine.scan(&table, filter).await.unwrap();

                todo!();
            }
            _ => return Err(tonic::Status::new(tonic::Code::Internal, "not implemented".to_string())),
        };

        Ok(tonic::Response::new(BinaryReadResponse { payload: bincode::serialize(&resp).unwrap() }))
    }
}

