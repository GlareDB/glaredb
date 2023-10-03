use crate::{
    errors::{Result, RpcsrvError},
    session::RemoteSession,
};
use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::arrow::ipc::writer::FileWriter as IpcFileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{Stream, StreamExt};
use protogen::{
    gen::rpcsrv::service::{self, BroadcastExchangeResponse},
    rpcsrv::types::service::{
        CloseSessionRequest, CloseSessionResponse, DispatchAccessRequest, FetchCatalogRequest,
        FetchCatalogResponse, InitializeSessionRequest, InitializeSessionResponse,
        PhysicalPlanExecuteRequest, TableProviderResponse,
    },
};
use sqlexec::{
    engine::{Engine, SessionStorageConfig},
    remote::exchange_stream::ClientExchangeRecvStream,
};
use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tonic::{Request, Response, Status, Streaming};
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

    /// Whether we're running in itegration testing mode.
    integration_testing: bool,
}

impl RpcHandler {
    pub fn new(engine: Arc<Engine>, allow_client_init: bool, integration_testing: bool) -> Self {
        RpcHandler {
            engine,
            sessions: DashMap::new(),
            allow_client_init,
            integration_testing,
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
            InitializeSessionRequest::Client(req) if self.allow_client_init => {
                let mut db_id = Uuid::nil();
                if let Some(test_db_id) = req.test_db_id {
                    if self.integration_testing {
                        db_id = test_db_id;
                    }
                }
                (db_id, SessionStorageConfig::default())
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

        let context = self
            .engine
            .new_remote_session_context(conn_id, db_id, storage_conf)
            .await?;

        let sess = RemoteSession::new(context);
        let initial_state = sess.get_refreshed_catalog_state().await?;

        self.sessions.insert(conn_id, sess);

        Ok(InitializeSessionResponse {
            session_id: conn_id,
            catalog: initial_state,
        })
    }

    async fn fetch_catalog_inner(&self, req: FetchCatalogRequest) -> Result<FetchCatalogResponse> {
        let session = self.get_session(req.session_id)?;
        let catalog = session.get_refreshed_catalog_state().await?;

        info!(session_id=%req.session_id, version = %catalog.version, "fetching catalog");

        Ok(FetchCatalogResponse { catalog })
    }

    async fn dispatch_access_inner(
        &self,
        req: DispatchAccessRequest,
    ) -> Result<TableProviderResponse> {
        info!(session_id=%req.session_id, table_ref=%req.table_ref, "dispatching table access");
        let args = req
            .args
            .map(|args| {
                args.into_iter()
                    .map(|arg| Ok(arg.try_into()?))
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?;

        let opts = req
            .opts
            .map(|opts| {
                opts.into_iter()
                    .map(|(k, v)| Ok((k, v.try_into()?)))
                    .collect::<Result<HashMap<_, _>>>()
            })
            .transpose()?;

        let session = self.get_session(req.session_id)?;
        let (id, schema) = session.dispatch_access(req.table_ref, args, opts).await?;
        Ok(TableProviderResponse { id, schema })
    }

    async fn physical_plan_execute_inner(
        &self,
        req: PhysicalPlanExecuteRequest,
    ) -> Result<ExecutionResponseBatchStream> {
        info!(session_id=%req.session_id, "executing physical plan");

        let session = self.get_session(req.session_id)?;
        let batches = session.physical_plan_execute(req.physical_plan).await?;
        Ok(ExecutionResponseBatchStream {
            batches,
            buf: Vec::new(),
        })
    }

    async fn broadcast_exchange_inner(
        &self,
        req: Streaming<service::BroadcastExchangeRequest>,
    ) -> Result<BroadcastExchangeResponse> {
        let stream = ClientExchangeRecvStream::try_new(req).await?;
        let session_id = stream.session_id();

        info!(session_id=%session_id, broadcast_id=%stream.broadcast_id(), "beginning client exchange stream");

        let session = self.get_session(session_id)?;

        session.register_broadcast_stream(stream).await?;

        // TODO: We might need to await here for stream completion.

        Ok(BroadcastExchangeResponse {})
    }

    fn close_session_inner(&self, req: CloseSessionRequest) -> Result<CloseSessionResponse> {
        info!(session_id=%req.session_id, "Closing Session");
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

    async fn fetch_catalog(
        &self,
        request: Request<service::FetchCatalogRequest>,
    ) -> Result<Response<service::FetchCatalogResponse>, Status> {
        let resp = self
            .fetch_catalog_inner(request.into_inner().try_into()?)
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

    async fn physical_plan_execute(
        &self,
        request: Request<service::PhysicalPlanExecuteRequest>,
    ) -> Result<Response<Self::PhysicalPlanExecuteStream>, Status> {
        let resp = self
            .physical_plan_execute_inner(request.into_inner().try_into()?)
            .await?;
        Ok(Response::new(Box::pin(resp)))
    }

    async fn broadcast_exchange(
        &self,
        request: Request<Streaming<service::BroadcastExchangeRequest>>,
    ) -> Result<Response<service::BroadcastExchangeResponse>, Status> {
        let resp = self.broadcast_exchange_inner(request.into_inner()).await?;
        Ok(Response::new(resp))
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
