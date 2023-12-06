use crate::errors::{Result, RpcsrvError};

use dashmap::DashMap;
use datafusion::{
    arrow::{ipc::writer::IpcWriteOptions, record_batch::RecordBatch},
    logical_expr::LogicalPlan,
    physical_plan::SendableRecordBatchStream,
};
use datafusion_ext::vars::SessionVars;
use datafusion_proto::protobuf::PhysicalPlanNode;
use once_cell::sync::Lazy;
use sqlexec::{
    context::remote::RemoteSessionContext,
    engine::{Engine, SessionStorageConfig},
    extension_codec::GlareDBExtensionCodec,
    remote::provider_cache::ProviderCache,
    session::Session,
    OperationInfo,
};
use std::{
    fmt::Debug,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
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
use futures::{lock::Mutex, Stream};
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use tonic::{Request, Response, Status, Streaming};
static INSTANCE_SQL_DATA: Lazy<SqlInfoData> = Lazy::new(|| {
    let mut builder = SqlInfoDataBuilder::new();
    // Server information
    builder.append(SqlInfo::FlightSqlServerName, "Example Flight SQL Server");
    builder.append(SqlInfo::FlightSqlServerVersion, "1");
    // 1.3 comes from https://github.com/apache/arrow/blob/f9324b79bf4fc1ec7e97b32e3cce16e75ef0f5e3/format/Schema.fbs#L24
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
    sess: Arc<Mutex<Session>>,
    remote_session_contexts: Arc<DashMap<Uuid, Arc<RemoteSessionContext>>>,
    statements: Arc<DashMap<String, LogicalPlan>>,
    results: Arc<DashMap<String, Vec<RecordBatch>>>,
}
impl FlightSessionHandler {
    // todo: figure out how to close inactive sessions
    async fn create_ctx(&self) -> Result<String, Status> {
        let uuid = Uuid::new_v4();
        let session = self
            .engine
            .new_remote_session_context(uuid, SessionStorageConfig::default())
            .await
            .map_err(|e| Status::internal(format!("Error creating session: {e}")))?;
        self.remote_session_contexts
            .insert(uuid.clone(), Arc::new(session));
        Ok(uuid.to_string())
    }
    pub async fn try_new(engine: &Arc<Engine>) -> Result<Self> {
        let session = engine
            .new_untracked_session_context(SessionVars::default(), SessionStorageConfig::default())
            .await
            .map_err(|e| Status::internal(format!("Error creating session: {e}")))?;

        Ok(Self {
            engine: engine.clone(),
            statements: Arc::new(DashMap::new()),
            results: Arc::new(DashMap::new()),
            remote_session_contexts: Arc::new(DashMap::new()),
            sess: Arc::new(Mutex::new(session)),
        })
    }
    async fn get_ctx<T: Debug>(&self, req: &Request<T>) -> Result<Arc<Mutex<Session>>, Status> {
        Ok(self.sess.clone())
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSessionHandler {
    type FlightService = FlightSessionHandler;

    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        todo!()
    }

    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        if !message.is::<ActionExecutePhysicalPlan>() {
            panic!("Expected ActionExecutePhysicalPlan but got {:?}", message)
        }
        let ctx = self.get_ctx(&request).await?;

        let ctx = ctx.lock().await;

        let plan: ActionExecutePhysicalPlan = message
            .unpack()
            .map_err(|e| Status::internal(format!("{e:?}")))?
            .ok_or_else(|| Status::internal("Expected FetchResults but got None!"))?;
        let plan = plan.plan;
        let cache = ProviderCache::default();
        let codec = ctx.extension_codec(&cache);

        let plan =
            PhysicalPlanNode::try_decode(&plan).map_err(|e| Status::internal(format!("{e:?}")))?;

        let plan = plan
            .try_into_physical_plan(ctx.df_ctx(), ctx.df_ctx().runtime_env().as_ref(), &codec)
            .unwrap();

        let stream = ctx.execute_physical(plan).await.unwrap();
        let schema = stream.schema();
        let stream =
            stream.map_err(|e| arrow_flight::error::FlightError::ExternalError(Box::new(e)));
        // while let Some(batch) = stream.next().await {
        //     batches.push(batch.unwrap());
        // }

        // let batch_stream = futures::stream::iter(batches).map(Ok);

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
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let handle = std::str::from_utf8(&cmd.prepared_statement_handle)
            .map_err(|e| status!("Unable to parse handle", e))?;
        let ctx = self.get_ctx(&request).await?;
        let mut ctx = ctx.lock().await;

        let portal = ctx
            .get_portal(handle)
            .map_err(|e| status!("Unable to get portal", e))?;
        let plan = portal.logical_plan().unwrap();
        let plan = plan
            .clone()
            .try_into_datafusion_plan()
            .map_err(RpcsrvError::from)?;
        let physical = ctx
            .create_physical_plan(plan, &OperationInfo::default())
            .await
            .map_err(|e| status!("Unable to execute portal", e))?;
        // Encode the physical plan into a protobuf message.
        let physical_plan = {
            let node = PhysicalPlanNode::try_from_physical_plan(
                physical,
                &GlareDBExtensionCodec::new_encoder(),
            )
            .unwrap();
            let mut buf = Vec::new();
            node.try_encode(&mut buf).unwrap();
            buf
        };
        let action = ActionExecutePhysicalPlan {
            plan: physical_plan,
        };

        let ticket = Ticket::new(action.as_any().encode_to_vec());

        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        // Ideally, we'd start the execution here, but instead we defer it all to the "do_get" call.
        let flight_info = FlightInfo::new()
            .with_descriptor(FlightDescriptor::new_cmd(vec![]))
            .with_endpoint(endpoint);

        Ok(tonic::Response::new(flight_info))
    }

    // async fn get_flight_info_catalogs(
    //     &self,
    //     query: CommandGetCatalogs,
    //     request: Request<FlightDescriptor>,
    // ) -> Result<Response<FlightInfo>, Status> {
    //     todo!()
    // }

    // async fn get_flight_info_schemas(
    //     &self,
    //     query: CommandGetDbSchemas,
    //     request: Request<FlightDescriptor>,
    // ) -> Result<Response<FlightInfo>, Status> {
    //     todo!()
    // }

    // async fn get_flight_info_tables(
    //     &self,
    //     query: CommandGetTables,
    //     request: Request<FlightDescriptor>,
    // ) -> Result<Response<FlightInfo>, Status> {
    //     todo!()
    // }

    // async fn get_flight_info_table_types(
    //     &self,
    //     _query: CommandGetTableTypes,
    //     _request: Request<FlightDescriptor>,
    // ) -> Result<Response<FlightInfo>, Status> {
    //     Err(Status::unimplemented(
    //         "get_flight_info_table_types not implemented",
    //     ))
    // }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket::new(query.encode_to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(query.into_builder(&INSTANCE_SQL_DATA).schema().as_ref())
            .map_err(|e| status!("Unable to encode schema", e))?
            .with_descriptor(flight_descriptor);

        Ok(tonic::Response::new(flight_info))
    }

    // async fn get_flight_info_primary_keys(
    //     &self,
    //     _query: CommandGetPrimaryKeys,
    //     _request: Request<FlightDescriptor>,
    // ) -> Result<Response<FlightInfo>, Status> {

    //     Err(Status::unimplemented(
    //         "get_flight_info_primary_keys not implemented",
    //     ))
    // }

    // async fn get_flight_info_exported_keys(
    //     &self,
    //     _query: CommandGetExportedKeys,
    //     _request: Request<FlightDescriptor>,
    // ) -> Result<Response<FlightInfo>, Status> {

    //     Err(Status::unimplemented(
    //         "get_flight_info_exported_keys not implemented",
    //     ))
    // }

    // async fn get_flight_info_imported_keys(
    //     &self,
    //     _query: CommandGetImportedKeys,
    //     _request: Request<FlightDescriptor>,
    // ) -> Result<Response<FlightInfo>, Status> {

    //     Err(Status::unimplemented(
    //         "get_flight_info_imported_keys not implemented",
    //     ))
    // }

    // async fn get_flight_info_cross_reference(
    //     &self,
    //     _query: CommandGetCrossReference,
    //     _request: Request<FlightDescriptor>,
    // ) -> Result<Response<FlightInfo>, Status> {

    //     Err(Status::unimplemented(
    //         "get_flight_info_imported_keys not implemented",
    //     ))
    // }

    // async fn get_flight_info_xdbc_type_info(
    //     &self,
    //     query: CommandGetXdbcTypeInfo,
    //     request: Request<FlightDescriptor>,
    // ) -> Result<Response<FlightInfo>, Status> {

    //     todo!()
    // }

    // // do_get
    // async fn do_get_statement(
    //     &self,
    //     _ticket: TicketStatementQuery,
    //     _request: Request<Ticket>,
    // ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {

    //     Err(Status::unimplemented("do_get_statement not implemented"))
    // }

    // async fn do_get_prepared_statement(
    //     &self,
    //     _query: CommandPreparedStatementQuery,
    //     _request: Request<Ticket>,
    // ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {

    //     Err(Status::unimplemented(
    //         "do_get_prepared_statement not implemented",
    //     ))
    // }

    // async fn do_get_catalogs(
    //     &self,
    //     query: CommandGetCatalogs,
    //     _request: Request<Ticket>,
    // ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {

    //     todo!()
    // }

    // async fn do_get_schemas(
    //     &self,
    //     query: CommandGetDbSchemas,
    //     _request: Request<Ticket>,
    // ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {

    //     todo!()
    // }

    // async fn do_get_tables(
    //     &self,
    //     query: CommandGetTables,
    //     _request: Request<Ticket>,
    // ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {

    //     todo!()
    // }

    // async fn do_get_table_types(
    //     &self,
    //     _query: CommandGetTableTypes,
    //     _request: Request<Ticket>,
    // ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {

    //     Err(Status::unimplemented("do_get_table_types not implemented"))
    // }

    // async fn do_get_sql_info(
    //     &self,
    //     query: CommandGetSqlInfo,
    //     _request: Request<Ticket>,
    // ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {

    //     todo!()
    // }
    // async fn do_get_primary_keys(
    //     &self,
    //     _query: CommandGetPrimaryKeys,
    //     _request: Request<Ticket>,
    // ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {

    //     Err(Status::unimplemented("do_get_primary_keys not implemented"))
    // }

    // async fn do_get_exported_keys(
    //     &self,
    //     _query: CommandGetExportedKeys,
    //     _request: Request<Ticket>,
    // ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {

    //     Err(Status::unimplemented(
    //         "do_get_exported_keys not implemented",
    //     ))
    // }

    // async fn do_get_imported_keys(
    //     &self,
    //     _query: CommandGetImportedKeys,
    //     _request: Request<Ticket>,
    // ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
    //     Err(Status::unimplemented(
    //         "do_get_imported_keys not implemented",
    //     ))
    // }

    // async fn do_get_cross_reference(
    //     &self,
    //     _query: CommandGetCrossReference,
    //     _request: Request<Ticket>,
    // ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
    //     Err(Status::unimplemented(
    //         "do_get_cross_reference not implemented",
    //     ))
    // }

    // async fn do_get_xdbc_type_info(
    //     &self,
    //     query: CommandGetXdbcTypeInfo,
    //     _request: Request<Ticket>,
    // ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {

    //     todo!()
    // }

    // // do_put
    // async fn do_put_statement_update(
    //     &self,
    //     _ticket: CommandStatementUpdate,
    //     _request: Request<PeekableFlightDataStream>,
    // ) -> Result<i64, Status> {

    //     todo!()
    // }

    // async fn do_put_substrait_plan(
    //     &self,
    //     _ticket: CommandStatementSubstraitPlan,
    //     _request: Request<PeekableFlightDataStream>,
    // ) -> Result<i64, Status> {

    //     Err(Status::unimplemented(
    //         "do_put_substrait_plan not implemented",
    //     ))
    // }

    // async fn do_put_prepared_statement_query(
    //     &self,
    //     _query: CommandPreparedStatementQuery,
    //     _request: Request<PeekableFlightDataStream>,
    // ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {

    //     Err(Status::unimplemented(
    //         "do_put_prepared_statement_query not implemented",
    //     ))
    // }

    // async fn do_put_prepared_statement_update(
    //     &self,
    //     _query: CommandPreparedStatementUpdate,
    //     _request: Request<PeekableFlightDataStream>,
    // ) -> Result<i64, Status> {
    //     Err(Status::unimplemented(
    //         "do_put_prepared_statement_update not implemented",
    //     ))
    // }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let ctx = self.get_ctx(&request).await?;
        let mut ctx = ctx.lock().await;
        let handle = uuid::Uuid::new_v4().to_string();
        ctx.prepare_portal(&handle, &query.query)
            .await
            .map_err(|e| status!("Unable to prepare statement", e))?;

        let lp = ctx
            .query_to_lp(&query.query)
            .await
            .map_err(|e| status!("Unable to parse query", e))?;

        let output_schema = lp.output_schema().unwrap();
        let message = SchemaAsIpc::new(&output_schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e| status!("Unable to serialize schema", e))?;
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
        request: Request<Action>,
    ) -> Result<(), Status> {
        let ctx = self.get_ctx(&request).await?;
        let mut ctx = ctx.lock().await;
        let handle = std::str::from_utf8(&query.prepared_statement_handle)
            .map_err(|e| status!("Unable to parse handle", e))?;
        ctx.remove_portal(handle);

        Ok(())
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActionExecutePhysicalPlan {
    #[prost(bytes, tag = "1")]
    pub plan: Vec<u8>,
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
