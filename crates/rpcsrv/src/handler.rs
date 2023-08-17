use crate::{
    errors::{Result, RpcsrvError},
    session::RemoteSession,
};
use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::{arrow::ipc::writer::FileWriter as IpcFileWriter, variable::VarType};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_ext::vars::SessionVarsInner;
use futures::{Stream, StreamExt};
use protogen::{
    gen::rpcsrv::service,
    metastore::types::catalog::CatalogState,
    rpcsrv::types::service::{
        CloseSessionRequest, CloseSessionResponse, CreatePhysicalPlanRequest,
        DispatchAccessRequest, InitializeSessionRequest, InitializeSessionResponse,
        PhysicalPlanExecuteRequest, PhysicalPlanResponse, TableProviderInsertIntoRequest,
        TableProviderResponse, TableProviderScanRequest,
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

pub struct RpcHandler {
    /// Core db engine for creating sessions.
    engine: Arc<Engine>,

    /// Open sessions.
    sessions: DashMap<Uuid, RemoteSession>,

    /// Allow initialize session messages from client.
    ///
    /// By default only messages from proxy are accepted.
    allow_client_init: bool,
}

impl RpcHandler {
    pub fn new(engine: Arc<Engine>, allow_client_init: bool) -> Self {
        RpcHandler {
            engine,
            sessions: DashMap::new(),
            allow_client_init,
        }
    }

    async fn initialize_session_inner(
        &self,
        req: InitializeSessionRequest,
    ) -> Result<InitializeSessionResponse> {
        // Get db id and storage config from the request.
        //
        // This will check that we actually received a proxy request, and not a
        // request from the client.
        let (db_id, storage_conf) = match req {
            InitializeSessionRequest::Proxy(req) => {
                let storage_conf = SessionStorageConfig {
                    gcs_bucket: req.storage_conf.gcs_bucket,
                };
                (req.db_id, storage_conf)
            }
            InitializeSessionRequest::Client(_req) if self.allow_client_init => {
                (Uuid::nil(), SessionStorageConfig::default())
            }
            _ => {
                return Err(RpcsrvError::SessionInitalizeError(
                    "unexpectedly received client request, expected a request from the proxy"
                        .to_string(),
                ))
            }
        };

        let conn_id = Uuid::new_v4();
        info!(session_id=%conn_id, "initializing remote session");

        let mut vars = SessionVarsInner::default();
        // TODO: handle error instead
        vars.database_id.set_and_log(db_id, VarType::System);
        vars.connection_id.set_and_log(conn_id, VarType::System);

        let sess = self
            .engine
            .new_session(vars, storage_conf, /* remote_ctx = */ true)
            .await?;

        let sess = RemoteSession::new(sess);
        let initial_state: CatalogState = sess.get_catalog_state().await;

        self.sessions.insert(conn_id, sess);

        Ok(InitializeSessionResponse {
            session_id: conn_id,
            catalog: initial_state,
        })
    }

    async fn create_physical_plan_inner(
        &self,
        req: CreatePhysicalPlanRequest,
    ) -> Result<PhysicalPlanResponse> {
        // TODO(perf): This actually ends being two/three locks that we need to acquire.
        // 1. The hashmap
        // 2. The session itself
        // 3. (soon) Datafusion's context once we start using that

        let session = self.get_session(req.session_id)?;
        info!(session_id=%req.session_id, "creating physical plan");
        let (id, schema) = session.create_physical_plan(req.logical_plan).await?;
        Ok(PhysicalPlanResponse { id, schema })
    }

    async fn dispatch_access_inner(
        &self,
        req: DispatchAccessRequest,
    ) -> Result<TableProviderResponse> {
        let session = self.get_session(req.session_id)?;
        info!(session_id=%req.session_id, table_ref=%req.table_ref, "dispatching table access");
        let (id, schema) = session.dispatch_access(req.table_ref).await?;
        Ok(TableProviderResponse { id, schema })
    }

    async fn table_provider_scan_inner(
        &self,
        req: TableProviderScanRequest,
    ) -> Result<PhysicalPlanResponse> {
        let session = self.get_session(req.session_id)?;
        info!(session_id=%req.session_id, provider_id=%req.provider_id, "scanning table provider");
        let (id, schema) = session
            .table_provider_scan(
                req.provider_id,
                req.projection.as_ref(),
                &req.filters,
                req.limit,
            )
            .await?;
        Ok(PhysicalPlanResponse { id, schema })
    }

    async fn table_provider_insert_into_inner(
        &self,
        req: TableProviderInsertIntoRequest,
    ) -> Result<PhysicalPlanResponse> {
        let session = self.get_session(req.session_id)?;
        info!(session_id=%req.session_id, provider_id=%req.provider_id, "insert into table provider");
        let (id, schema) = session
            .table_provider_insert_into(req.provider_id, req.input_exec_id)
            .await?;
        Ok(PhysicalPlanResponse { id, schema })
    }

    async fn physical_plan_execute_inner(
        &self,
        req: PhysicalPlanExecuteRequest,
    ) -> Result<ExecutionResponseBatchStream> {
        let session = self.get_session(req.session_id)?;
        info!(session_id=%req.session_id, exec_id=%req.exec_id, "executing physical plan");
        let batches = session.physical_plan_execute(req.exec_id).await?;
        Ok(ExecutionResponseBatchStream {
            batches,
            buf: Vec::new(),
        })
    }

    fn close_session_inner(&self, req: CloseSessionRequest) -> Result<CloseSessionResponse> {
        info!(session_id=%req.session_id, "closing session");
        self.sessions.remove(&req.session_id);
        Ok(CloseSessionResponse {})
    }

    fn get_session(&self, session_id: Uuid) -> Result<RemoteSession> {
        self.sessions
            .get(&session_id)
            .ok_or_else(|| RpcsrvError::MissingSession(session_id))
            .map(|s| s.value().clone())
    }
}

#[async_trait]
impl service::execution_service_server::ExecutionService for RpcHandler {
    type PhysicalPlanExecuteStream =
        Pin<Box<dyn Stream<Item = Result<service::RecordBatchResponse, Status>> + Send>>;

    async fn initialize_session(
        &self,
        request: Request<service::InitializeSessionRequest>,
    ) -> Result<Response<service::InitializeSessionResponse>, Status> {
        let resp = self
            .initialize_session_inner(request.into_inner().try_into()?)
            .await?;
        Ok(Response::new(resp.try_into()?))
    }

    async fn create_physical_plan(
        &self,
        request: Request<service::CreatePhysicalPlanRequest>,
    ) -> Result<Response<service::PhysicalPlanResponse>, Status> {
        let resp = self
            .create_physical_plan_inner(request.into_inner().try_into()?)
            .await?;
        Ok(Response::new(resp.try_into()?))
    }

    async fn dispatch_access(
        &self,
        request: Request<service::DispatchAccessRequest>,
    ) -> Result<Response<service::TableProviderResponse>, Status> {
        let resp = self
            .dispatch_access_inner(request.into_inner().try_into()?)
            .await?;
        Ok(Response::new(resp.try_into()?))
    }

    async fn table_provider_scan(
        &self,
        request: Request<service::TableProviderScanRequest>,
    ) -> Result<Response<service::PhysicalPlanResponse>, Status> {
        let resp = self
            .table_provider_scan_inner(request.into_inner().try_into()?)
            .await?;
        Ok(Response::new(resp.try_into()?))
    }

    async fn table_provider_insert_into(
        &self,
        request: Request<service::TableProviderInsertIntoRequest>,
    ) -> Result<Response<service::PhysicalPlanResponse>, Status> {
        let resp = self
            .table_provider_insert_into_inner(request.into_inner().try_into()?)
            .await?;
        Ok(Response::new(resp.try_into()?))
    }

    async fn physical_plan_execute(
        &self,
        request: Request<service::PhysicalPlanExecuteRequest>,
    ) -> Result<Response<Self::PhysicalPlanExecuteStream>, Status> {
        let resp = self
            .physical_plan_execute_inner(request.into_inner().try_into()?)
            .await?;
        Ok(Response::new(Box::pin(resp)))
    }

    async fn close_session(
        &self,
        request: Request<service::CloseSessionRequest>,
    ) -> Result<Response<service::CloseSessionResponse>, Status> {
        let resp = self.close_session_inner(request.into_inner().try_into()?)?;
        Ok(Response::new(resp.into()))
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
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<service::RecordBatchResponse> {
        self.buf.clear();

        let schema = batch.schema();
        let mut writer = IpcFileWriter::try_new(&mut self.buf, &schema)?;
        writer.write(batch)?;
        writer.finish()?;

        let _ = writer.into_inner()?;

        Ok(service::RecordBatchResponse {
            arrow_ipc: self.buf.clone(),
        })
    }
}

impl Stream for ExecutionResponseBatchStream {
    type Item = Result<service::RecordBatchResponse, Status>;

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
