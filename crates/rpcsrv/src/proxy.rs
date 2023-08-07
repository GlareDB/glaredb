use crate::{
    errors::{Result, RpcsrvError},
    session::RemoteSession,
};
use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::arrow::ipc::writer::FileWriter as IpcFileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_ext::vars::{SessionVars, VarSetter};
use futures::{Stream, StreamExt};
use protogen::gen::{
    metastore::catalog::CatalogState,
    rpcsrv::service::{
        execution_service_server::ExecutionService, ExecuteRequest, ExecuteResponse,
        InitializeSessionRequest, InitializeSessionResponse,
    },
};
use sqlexec::engine::{Engine, SessionStorageConfig};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tonic::{Request, Response, Status};
use tracing::info;
use uuid::Uuid;

/// Proxies rpc requests to compute nodes.
pub struct RpcProxy {}

#[async_trait]
impl ExecutionService for RpcProxy {
    type ExecuteStream = Pin<Box<dyn Stream<Item = Result<ExecuteResponse, Status>> + Send>>;

    async fn initialize_session(
        &self,
        request: Request<InitializeSessionRequest>,
    ) -> Result<Response<InitializeSessionResponse>, Status> {
        unimplemented!()
    }

    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        unimplemented!()
    }
}
