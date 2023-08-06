use crate::{
    errors::{Result, RpcsrvError},
    session::RemoteSession,
};
use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::arrow::ipc::writer::FileWriter as IpcFileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use datafusion_ext::vars::{SessionVars, VarSetter};
use datafusion_proto::logical_plan::{AsLogicalPlan, DefaultLogicalExtensionCodec};
use datafusion_proto::protobuf::LogicalPlanNode;
use futures::{Stream, StreamExt};
use protogen::gen::{
    metastore::catalog::CatalogState,
    rpcsrv::service::{
        execute_request::Plan, execution_service_server::ExecutionService, ExecuteRequest,
        ExecuteResponse, InitializeSessionRequest, InitializeSessionResponse,
    },
};
use sqlexec::{
    engine::{Engine, SessionStorageConfig, TrackedSession},
    parser::{self, StatementWithExtensions},
    session::{ExecutionResult, Session},
};
use std::io::Cursor;
use std::task::{Context, Poll};
use std::{pin::Pin, sync::Arc};
use tonic::{Request, Response, Status};
use tracing::{debug, info};
use uuid::Uuid;

pub struct RpcHandler {
    /// Core db engine for creating sessions.
    engine: Arc<Engine>,

    /// Open sessions.
    sessions: DashMap<Uuid, RemoteSession>,
}

impl RpcHandler {
    pub fn new(engine: Arc<Engine>) -> Self {
        RpcHandler {
            engine,
            sessions: DashMap::new(),
        }
    }

    async fn initialize_session_inner(
        &self,
        req: InitializeSessionRequest,
    ) -> Result<InitializeSessionResponse> {
        let db_id =
            Uuid::from_slice(&req.db_id).map_err(|e| RpcsrvError::InvalidId("database", e))?;

        let conn_id = Uuid::new_v4();

        let mut vars = SessionVars::default();
        // TODO: handle error instead
        vars.database_id.set_and_log(db_id, VarSetter::System);
        vars.connection_id.set_and_log(conn_id, VarSetter::System);

        let sess = self
            .engine
            .new_session(vars, SessionStorageConfig::default())
            .await?;

        let sess = RemoteSession::new(sess);
        let initial_state: CatalogState = sess.get_catalog_state().await.try_into()?;

        self.sessions.insert(conn_id, sess);

        Ok(InitializeSessionResponse {
            session_id: conn_id.into_bytes().to_vec(),
            catalog: Some(initial_state),
        })
    }

    async fn execute_inner(&self, req: ExecuteRequest) -> Result<ExecutionResponseBatchStream> {
        let session_id =
            Uuid::from_slice(&req.session_id).map_err(|e| RpcsrvError::InvalidId("session", e))?;

        // TODO(perf): This actually ends being two/three locks that we need to acquire.
        // 1. The hashmap
        // 2. The session itself
        // 3. (soon) Datafusion's context once we start using that

        let sess = self
            .sessions
            .get(&session_id)
            .ok_or_else(|| RpcsrvError::MissingSession(session_id))?;

        let batches = sess.execute_serialized_plan(req).await?;
        Ok(ExecutionResponseBatchStream {
            batches,
            buf: Vec::new(),
        })
    }
}

#[async_trait]
impl ExecutionService for RpcHandler {
    type ExecuteStream = Pin<Box<dyn Stream<Item = Result<ExecuteResponse, Status>> + Send>>;

    async fn initialize_session(
        &self,
        request: Request<InitializeSessionRequest>,
    ) -> Result<Response<InitializeSessionResponse>, Status> {
        info!("initializing session");
        let resp = self.initialize_session_inner(request.into_inner()).await?;
        Ok(Response::new(resp))
    }

    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        info!("executing");
        let stream = self.execute_inner(request.into_inner()).await?;
        Ok(Response::new(Box::pin(stream)))
    }
}

/// Convert a record batch stream into a stream of execution responses
/// containing ipc serialized batches.
// TODO: StreamWriter
// TODO: Possibly buffer record batches.
struct ExecutionResponseBatchStream {
    batches: SendableRecordBatchStream,
    buf: Vec<u8>,
}

impl ExecutionResponseBatchStream {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<ExecuteResponse> {
        self.buf.clear();

        let schema = batch.schema();
        let mut writer = IpcFileWriter::try_new(&mut self.buf, &schema)?;
        writer.write(&batch)?;
        writer.finish()?;

        let _ = writer.into_inner()?;

        Ok(ExecuteResponse {
            arrow_ipc: self.buf.clone(),
        })
    }
}

impl Stream for ExecutionResponseBatchStream {
    type Item = Result<ExecuteResponse, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.batches.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => match self.write_batch(&batch) {
                Ok(resp) => Poll::Ready(Some(Ok(resp))),
                Err(e) => Poll::Ready(Some(Err(e.into()))),
            },
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(RpcsrvError::from(e).into()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
