use crate::errors::{Result, RpcsrvError};

use dashmap::DashMap;
use datafusion::{arrow::ipc::writer::IpcWriteOptions, physical_plan::ExecutionPlan};
use datafusion_ext::vars::SessionVars;
use datafusion_proto::protobuf::PhysicalPlanNode;
use once_cell::sync::Lazy;
use sqlexec::{
    context::remote::RemoteSessionContext,
    engine::{Engine, SessionStorageConfig},
    extension_codec::GlareDBExtensionCodec,
    session::Session,
    OperationInfo,
};
use std::{pin::Pin, sync::Arc};
use uuid::Uuid;

pub use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::{
    encode::FlightDataEncoderBuilder, flight_service_server::FlightService, sql::*, Action,
    FlightDescriptor, FlightEndpoint, FlightInfo, IpcMessage, SchemaAsIpc, Ticket,
};
use arrow_flight::{
    sql::{
        metadata::{SqlInfoData, SqlInfoDataBuilder},
        server::FlightSqlService,
    },
    HandshakeRequest, HandshakeResponse,
};
use datafusion_proto::physical_plan::AsExecutionPlan;
use futures::TryStreamExt;
use futures::{lock::Mutex, Stream};
use prost::Message;
use tonic::{Request, Response, Status, Streaming};

static INSTANCE_SQL_DATA: Lazy<SqlInfoData> = Lazy::new(|| {
    let mut builder = SqlInfoDataBuilder::new();
    // Server information
    builder.append(SqlInfo::FlightSqlServerName, "GlareDB Flight Server");
    builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
    builder.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");
    builder.build().unwrap()
});

/// Custom header clients can use to specify the database they want to connect to.
/// the ADBC driver requires it to be passed in as `adbc.flight.sql.rpc.call_header.x-database`
const DATABASE_HEADER: &str = "x-database";

pub struct FlightSessionHandler {
    engine: Arc<Engine>,
    /// TODO: currently, we aren't removing these sessions, so this will grow forever.
    /// there's no close/shutdown hook, so the sessions can at most only be tied to a single transaction, not a connection.
    /// We'll want to implement a time based eviction policy, or a max size.
    remote_sessions: DashMap<String, Arc<RemoteSessionContext>>,
    // We use [`Session`] instead of [`TrackedSession`] because tracked sessions need to be
    // explicitly closed, and we don't have a way to do that yet.
    sessions: DashMap<String, Arc<Mutex<Session>>>,
}

impl FlightSessionHandler {
    pub fn new(engine: &Arc<Engine>) -> Self {
        Self {
            engine: engine.clone(),
            remote_sessions: DashMap::new(),
            sessions: DashMap::new(),
        }
    }

    async fn get_or_create_ctx<T>(
        &self,
        request: &Request<T>,
    ) -> Result<(String, Arc<Mutex<Session>>), Status> {
        let db_handle = request
            .metadata()
            .get(DATABASE_HEADER)
            .map(|s| s.to_str().unwrap().to_string())
            .unwrap_or_else(|| Uuid::default().to_string());
        if self.sessions.contains_key(&db_handle) {
            let sess = self.sessions.get(&db_handle).unwrap().clone();
            return Ok((db_handle, sess));
        }

        let db_id = Uuid::parse_str(&db_handle).map_err(|e| {
            Status::internal(format!(
                "Unable to parse database handle: {}",
                e.to_string()
            ))
        })?;

        let session_vars =
            SessionVars::default().with_database_id(db_id, datafusion::variable::VarType::System);
        let sess = self
            .engine
            .new_untracked_session(session_vars, SessionStorageConfig::default())
            .await
            .map_err(RpcsrvError::from)?;

        let remote_sess = self
            .engine
            .new_remote_session_context(db_id, SessionStorageConfig::default())
            .await
            .map_err(RpcsrvError::from)?;
        let sess = Arc::new(Mutex::new(sess));
        self.sessions.insert(db_handle.clone(), sess.clone());
        self.remote_sessions
            .insert(db_handle.clone(), Arc::new(remote_sess));

        Ok((db_handle, sess))
    }

    async fn get_exec_ctx<T>(
        &self,
        request: &Request<T>,
    ) -> Result<Arc<RemoteSessionContext>, Status> {
        let db_handle = request
            .metadata()
            .get(DATABASE_HEADER)
            .map(|s| s.to_str().unwrap().to_string())
            .unwrap_or_else(|| Uuid::default().to_string());

        if let Some(sess) = self.remote_sessions.get(&db_handle) {
            Ok(sess.clone())
        } else {
            Err(Status::internal(format!(
                "Unable to find session with handle {}",
                db_handle
            )))
        }
    }

    async fn get_ctx<T>(&self, request: &Request<T>) -> Result<Arc<Mutex<Session>>, Status> {
        let db_handle = request
            .metadata()
            .get(DATABASE_HEADER)
            .map(|s| s.to_str().unwrap().to_string())
            .unwrap_or_else(|| Uuid::default().to_string());

        self.sessions
            .get(&db_handle)
            .map(|s| s.clone())
            .ok_or_else(|| {
                Status::internal(format!("Unable to find session with handle {}", db_handle))
            })
    }

    async fn do_action_execute_physical_plan(
        &self,
        req: &Request<Ticket>,
        query: ActionExecutePhysicalPlan,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let ctx = self.get_exec_ctx(&req).await?;

        let plan = PhysicalPlanNode::try_decode(&query.plan).map_err(RpcsrvError::from)?;
        let codec = ctx.extension_codec();

        let plan = plan
            .try_into_physical_plan(
                ctx.get_datafusion_context(),
                ctx.get_datafusion_context().runtime_env().as_ref(),
                &codec,
            )
            .map_err(RpcsrvError::from)?;

        self.execute_physical_plan(req, plan).await
    }

    async fn execute_physical_plan<T>(
        &self,
        request: &Request<T>,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let ctx = self.get_exec_ctx(&request).await?;
        let stream = ctx
            .execute_physical(physical_plan)
            .map_err(RpcsrvError::from)?;

        let schema = stream.schema();
        let stream =
            stream.map_err(|e| arrow_flight::error::FlightError::ExternalError(Box::new(e)));

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream)
            .map_err(Status::from);

        Ok(Response::new(Box::pin(stream)))
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSessionHandler {
    type FlightService = FlightSessionHandler;

    async fn do_handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        todo!("support TLS")
    }

    async fn do_get_fallback(
        &self,
        req: Request<Ticket>,
        message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        match message.type_url.as_str() {
            ActionExecutePhysicalPlan::TYPE_URL => {
                let action: ActionExecutePhysicalPlan = message
                    .unpack()
                    .map_err(RpcsrvError::from)?
                    .ok_or_else(|| {
                        Status::internal("Expected ActionExecutePhysicalPlan but got None!")
                    })?;

                return self.do_action_execute_physical_plan(&req, action).await;
            }
            // All non specified types should be handled as a sql query
            other => {
                let mut ctx = self
                    .engine
                    .new_local_session_context(
                        SessionVars::default(),
                        SessionStorageConfig::default(),
                    )
                    .await
                    .map_err(RpcsrvError::from)?;
                match sqlexec::parser::parse_sql(&other) {
                    Ok(statements) => {
                        let lp = ctx
                            .parsed_to_lp(statements)
                            .await
                            .map_err(RpcsrvError::from)?;
                        let physical = ctx
                            .create_physical_plan(
                                lp.try_into_datafusion_plan().map_err(RpcsrvError::from)?,
                                &OperationInfo::default(),
                            )
                            .await
                            .map_err(RpcsrvError::from)?;
                        self.execute_physical_plan(&req, physical).await
                    }
                    Err(e) => Err(Status::internal(format!(
                        "Expected a SQL query, instead received: {}",
                        e.to_string()
                    ))),
                }
            }
        }
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket::new(query.encode_to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(tonic::Response::new(flight_info))
    }

    async fn get_flight_info_substrait_plan(
        &self,
        _query: CommandStatementSubstraitPlan,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_substrait_plan not implemented",
        ))
    }

    async fn get_flight_info_prepared_statement(
        &self,
        _: CommandPreparedStatementQuery,
        req: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let (handle, ctx) = self.get_or_create_ctx(&req).await?;
        let ctx = ctx.lock().await;
        let portal = ctx.get_portal(&handle).map_err(RpcsrvError::from)?;

        let plan = portal.logical_plan().unwrap();

        let plan = plan
            .clone()
            .try_into_datafusion_plan()
            .map_err(RpcsrvError::from)?;

        let physical = ctx
            .create_physical_plan(plan, &OperationInfo::default())
            .await
            .map_err(RpcsrvError::from)?;

        // Encode the physical plan into a protobuf message.
        let physical_plan = {
            let node = PhysicalPlanNode::try_from_physical_plan(
                physical,
                &GlareDBExtensionCodec::new_encoder(),
            )
            .map_err(RpcsrvError::from)?;

            let mut buf = Vec::new();
            node.try_encode(&mut buf).map_err(RpcsrvError::from)?;
            buf
        };

        let action = ActionExecutePhysicalPlan {
            plan: physical_plan,
            handle: handle.to_string(),
        };

        let ticket = Ticket::new(action.as_any().encode_to_vec());

        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        // Ideally, we'd start the execution here, but instead we defer it all to the "do_get" call.
        // Eventually, we should asynchronously start the execution here,
        // and return a `Ticket` that contains information on how to retrieve the results.
        let flight_info = FlightInfo::new()
            .with_descriptor(FlightDescriptor::new_cmd(vec![]))
            .with_endpoint(endpoint);

        Ok(tonic::Response::new(flight_info))
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();

        let flight_info = FlightInfo::new()
            .try_with_schema(query.into_builder(&INSTANCE_SQL_DATA).schema().as_ref())
            .map_err(RpcsrvError::from)?
            .with_descriptor(flight_descriptor);

        Ok(tonic::Response::new(flight_info))
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        req: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let (handle, ctx) = self.get_or_create_ctx(&req).await?;
        let mut ctx = ctx.lock().await;

        ctx.prepare_portal(&handle, &query.query)
            .await
            .map_err(RpcsrvError::from)?;

        let lp = ctx
            .query_to_lp(&query.query)
            .await
            .map_err(RpcsrvError::from)?;

        let output_schema = lp.output_schema().expect("no output schema");

        let message = SchemaAsIpc::new(&output_schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(RpcsrvError::from)?;

        let IpcMessage(schema_bytes) = message;
        let res = ActionCreatePreparedStatementResult {
            prepared_statement_handle: handle.into(),
            dataset_schema: schema_bytes,
            parameter_schema: Default::default(), // TODO: parameters
        };

        Ok(res)
    }

    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        req: Request<Action>,
    ) -> Result<(), Status> {
        let handle = std::str::from_utf8(&query.prepared_statement_handle)
            .map_err(|e| RpcsrvError::ParseError(e.to_string()))?;
        let ctx = self.get_ctx(&req).await?;
        ctx.lock().await.remove_portal(handle);

        Ok(())
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActionExecutePhysicalPlan {
    #[prost(bytes, tag = "1")]
    pub plan: Vec<u8>,
    #[prost(string, tag = "2")]
    pub handle: String,
}

impl ActionExecutePhysicalPlan {
    pub const TYPE_URL: &'static str =
        "type.googleapis.com/glaredb.rpcsrv.ActionExecutePhysicalPlan";
}

impl ProstMessageExt for ActionExecutePhysicalPlan {
    fn type_url() -> &'static str {
        Self::TYPE_URL
    }

    fn as_any(&self) -> Any {
        Any {
            type_url: ActionExecutePhysicalPlan::type_url().to_string(),
            value: ::prost::Message::encode_to_vec(self).into(),
        }
    }
}
