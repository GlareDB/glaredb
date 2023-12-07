use crate::errors::{Result, RpcsrvError};

use dashmap::DashMap;
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion_ext::vars::SessionVars;
use datafusion_proto::protobuf::PhysicalPlanNode;
use once_cell::sync::Lazy;
use sqlexec::{
    context::remote::RemoteSessionContext,
    engine::{Engine, SessionStorageConfig, TrackedSession},
    extension_codec::GlareDBExtensionCodec,
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

macro_rules! status {
    ($desc:expr, $err:expr) => {
        Status::internal(format!("{}: {} at {}:{}", $desc, $err, file!(), line!()))
    };
}

pub struct FlightSessionHandler {
    engine: Arc<Engine>,
    /// The remote context is used to execute the queries in a stateless manner.
    remote_ctx: Arc<RemoteSessionContext>,
    sessions: DashMap<String, Arc<Mutex<TrackedSession>>>,
}

impl FlightSessionHandler {
    async fn create_ctx(&self, handle: &str) -> Result<Arc<Mutex<TrackedSession>>, Status> {
        if self.sessions.contains_key(handle) {
            let sess = self.sessions.get(handle).unwrap().clone();
            return Ok(sess);
        }
        let uuid = Uuid::parse_str(handle)
            .map_err(|e| RpcsrvError::ParseError(format!("Error parsing uuid: {e}")))?;
        let _ = self
            .engine
            .new_remote_session_context(uuid, SessionStorageConfig::default())
            .await
            .map_err(RpcsrvError::from)?;

        let sess = self
            .engine
            .new_local_session_context(SessionVars::default(), SessionStorageConfig::default())
            .await
            .map_err(RpcsrvError::from)?;

        let sess = Arc::new(Mutex::new(sess));
        self.sessions.insert(handle.to_string(), sess.clone());
        Ok(sess)
    }

    pub async fn try_new(engine: &Arc<Engine>) -> Result<Self> {
        let exec_ctx = engine
            .new_remote_session_context(Uuid::new_v4(), SessionStorageConfig::default())
            .await?;
        Ok(Self {
            engine: engine.clone(),
            remote_ctx: Arc::new(exec_ctx),
            sessions: DashMap::new(),
        })
    }

    async fn get_ctx(&self, handle: &str) -> Result<Arc<Mutex<TrackedSession>>, Status> {
        self.sessions.get(handle).map(|s| s.clone()).ok_or_else(|| {
            Status::internal(format!("Unable to find session with handle {}", handle))
        })
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
        todo!()
    }

    async fn do_get_fallback(
        &self,
        _: Request<Ticket>,
        message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        if !message.is::<ActionExecutePhysicalPlan>() {
            Err(Status::unimplemented(format!(
                "do_get: The defined request is invalid: {}",
                message.type_url
            )))?
        }
        let plan: ActionExecutePhysicalPlan = message
            .unpack()
            .map_err(RpcsrvError::from)?
            .ok_or_else(|| Status::internal("Expected FetchResults but got None!"))?;
        let ActionExecutePhysicalPlan { plan, handle } = plan;

        let ctx = self.get_ctx(&handle).await?;

        let ctx = ctx.lock().await;

        let plan = PhysicalPlanNode::try_decode(&plan).map_err(RpcsrvError::from)?;
        let codec = self.remote_ctx.extension_codec();

        let plan = plan
            .try_into_physical_plan(ctx.df_ctx(), ctx.df_ctx().runtime_env().as_ref(), &codec)
            .map_err(RpcsrvError::from)?;

        let stream = self
            .remote_ctx
            .execute_physical(plan)
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
        cmd: CommandPreparedStatementQuery,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let handle = std::str::from_utf8(&cmd.prepared_statement_handle)
            .map_err(|e| status!("Unable to parse handle", e))?;

        let ctx = self.get_ctx(handle).await?;
        let ctx = ctx.lock().await;
        let portal = ctx.get_portal(handle).map_err(RpcsrvError::from)?;

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

    // I think it's safe to create a session for the duration of the prepared statement?
    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        _: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let handle = uuid::Uuid::new_v4().to_string();
        let ctx = self.create_ctx(&handle).await?;
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
        _: Request<Action>,
    ) -> Result<(), Status> {
        let handle = std::str::from_utf8(&query.prepared_statement_handle)
            .map_err(|e| RpcsrvError::ParseError(e.to_string()))?;

        self.sessions.remove(handle);

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

impl ProstMessageExt for ActionExecutePhysicalPlan {
    fn type_url() -> &'static str {
        "type.googleapis.com/glaredb.rpcsrv.ActionExecutePhysicalPlan"
    }

    fn as_any(&self) -> Any {
        Any {
            type_url: ActionExecutePhysicalPlan::type_url().to_string(),
            value: ::prost::Message::encode_to_vec(self).into(),
        }
    }
}
