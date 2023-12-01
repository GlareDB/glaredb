use crate::{
    errors::{Result, RpcsrvError},
    session::RemoteSession,
};
use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::arrow::ipc::writer::FileWriter as IpcFileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion_ext::session_metrics::{
    BatchStreamWithMetricSender, QueryMetrics, SessionMetricsHandler,
};
use futures::{Stream, StreamExt};
use protogen::{
    gen::rpcsrv::common,
    gen::rpcsrv::service,
    rpcsrv::types::service::{
        DispatchAccessRequest, FetchCatalogRequest, FetchCatalogResponse, InitializeSessionRequest,
        InitializeSessionResponse, PhysicalPlanExecuteRequest, TableProviderResponse,
    },
};
use sqlexec::{
    engine::{Engine, SessionStorageConfig},
    remote::batch_stream::ExecutionBatchStream,
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
        let (db_id, user_id, storage_conf) = match req {
            InitializeSessionRequest::Proxy(req) => {
                let storage_conf = SessionStorageConfig {
                    gcs_bucket: req.storage_conf.gcs_bucket,
                };
                (req.db_id, Some(req.user_id), storage_conf)
            }
            InitializeSessionRequest::Client(req) if self.allow_client_init => {
                let mut db_id = Uuid::nil();
                if let Some(test_db_id) = req.test_db_id {
                    if self.integration_testing {
                        db_id = test_db_id;
                    }
                }
                (db_id, None, SessionStorageConfig::default())
            }
            _ => {
                return Err(RpcsrvError::SessionInitalizeError(
                    "unexpectedly received client request, expected a request from the proxy"
                        .to_string(),
                ))
            }
        };

        let sess = match self.get_session(db_id) {
            Ok(session) => session,
            Err(_) => {
                info!(session_id=%db_id, "initializing remote session");

                let context = self
                    .engine
                    .new_remote_session_context(db_id, storage_conf)
                    .await?;

                RemoteSession::new(context)
            }
        };

        let initial_state = sess.get_refreshed_catalog_state().await?;

        self.sessions.insert(db_id, sess);

        Ok(InitializeSessionResponse {
            database_id: db_id,
            catalog: initial_state,
            user_id,
        })
    }

    async fn fetch_catalog_inner(&self, req: FetchCatalogRequest) -> Result<FetchCatalogResponse> {
        let session = self.get_session(req.database_id)?;
        let catalog = session.get_refreshed_catalog_state().await?;

        info!(database_id=%req.database_id, version = %catalog.version, "fetching catalog");

        Ok(FetchCatalogResponse { catalog })
    }

    async fn dispatch_access_inner(
        &self,
        req: DispatchAccessRequest,
    ) -> Result<TableProviderResponse> {
        info!(database_id=%req.database_id, table_ref=%req.table_ref, "dispatching table access");
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

        let session = self.get_session(req.database_id)?;
        let (id, schema) = session.dispatch_access(req.table_ref, args, opts).await?;
        Ok(TableProviderResponse { id, schema })
    }

    async fn physical_plan_execute_inner(
        &self,
        req: PhysicalPlanExecuteRequest,
    ) -> Result<ExecutionResponseBatchStream> {
        info!(database_id=%req.database_id, "executing physical plan");

        let session = self.get_session(req.database_id)?;
        let (plan, batches) = session.physical_plan_execute(req.physical_plan).await?;

        let session_metrics_handler = SessionMetricsHandler::new(
            req.user_id.unwrap_or_default(),
            req.database_id,
            Uuid::nil(), // TODO: Connection ID?
            self.engine.get_tracker(),
        );

        let query_metrics = QueryMetrics {
            query_text: req.query_text,
            ..Default::default()
        };

        let batches =
            BatchStreamWithMetricSender::new(batches, plan, query_metrics, session_metrics_handler);

        Ok(ExecutionResponseBatchStream {
            batches,
            buf: Vec::new(),
        })
    }

    async fn broadcast_exchange_inner(
        &self,
        req: Streaming<common::ExecutionResultBatch>,
    ) -> Result<service::BroadcastExchangeResponse> {
        let stream = ExecutionBatchStream::try_new(req).await?;
        let database_id = stream.database_id();

        info!(database_id=%database_id, work_id=%stream.work_id(), "beginning client exchange stream");

        let session = self.get_session(database_id)?;

        session.register_broadcast_stream(stream).await?;

        // TODO: We might need to await here for stream completion.

        Ok(service::BroadcastExchangeResponse {})
    }

    fn get_session(&self, db_id: Uuid) -> Result<RemoteSession> {
        self.sessions
            .get(&db_id)
            .ok_or_else(|| RpcsrvError::MissingSession(db_id))
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
        request: Request<Streaming<common::ExecutionResultBatch>>,
    ) -> Result<Response<service::BroadcastExchangeResponse>, Status> {
        let resp = self.broadcast_exchange_inner(request.into_inner()).await?;
        Ok(Response::new(resp))
    }
}

/// Convert a record batch stream into a stream of execution responses
/// containing ipc serialized batches.
// TODO: StreamWriter
// TODO: Possibly buffer record batches.
struct ExecutionResponseBatchStream {
    batches: BatchStreamWithMetricSender,
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
