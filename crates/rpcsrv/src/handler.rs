use crate::{
    errors::{Result, RpcsrvError},
    session::RemoteSession,
};
use async_trait::async_trait;
use dashmap::DashMap;
use datafusion_ext::vars::{SessionVars, VarSetter};
use futures::Stream;
use protogen::gen::rpcsrv::service::{
    execution_service_server::ExecutionService, ExecuteRequest, ExecuteResponse,
    InitializeSessionRequest, InitializeSessionResponse,
};
use sqlexec::{
    engine::{Engine, SessionStorageConfig, TrackedSession},
    parser::{self, StatementWithExtensions},
    session::{ExecutionResult, Session},
};
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
}

#[async_trait]
impl ExecutionService for RpcHandler {
    type ExecuteStream = Pin<Box<dyn Stream<Item = Result<ExecuteResponse, Status>> + Send>>;

    async fn initialize_session(
        &self,
        request: Request<InitializeSessionRequest>,
    ) -> Result<Response<InitializeSessionResponse>, Status> {
        let req = request.into_inner();
        info!(?req, "initializing session");

        let db_id =
            Uuid::from_slice(&req.db_id).map_err(|_| RpcsrvError::InvalidDatabaseId(req.db_id))?;

        let conn_id = Uuid::new_v4();

        let mut vars = SessionVars::default();
        // TODO: handle error instead
        vars.database_id.set_and_log(db_id, VarSetter::System);
        vars.connection_id.set_and_log(conn_id, VarSetter::System);

        let sess = self
            .engine
            .new_session(vars, SessionStorageConfig::default())
            .await
            .map_err(RpcsrvError::from)?;

        let sess = RemoteSession::new(sess);
        self.sessions.insert(conn_id, sess);

        Ok(Response::new(InitializeSessionResponse {
            session_id: conn_id.into_bytes().to_vec(),
        }))
    }

    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        info!("executing");
        unimplemented!()
    }
}
