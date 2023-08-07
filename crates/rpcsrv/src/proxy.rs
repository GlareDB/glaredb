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
        execution_service_client::ExecutionServiceClient,
        execution_service_server::ExecutionService, ExecuteRequest, ExecuteResponse,
        InitializeSessionRequest, InitializeSessionResponse,
    },
};
use proxyutil::cloudauth::{CloudAuthenticator, ProxyAuthenticator};
use sqlexec::engine::{Engine, SessionStorageConfig};
use std::{hash::Hash, time::Duration};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tonic::{
    metadata::MetadataMap,
    transport::{Channel, Endpoint},
    Request, Response, Status, Streaming,
};
use tracing::info;
use uuid::Uuid;

/// Key used for the connections map.
// TODO: Possibly per user connections?
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ConnKey {
    ip: String,
    port: String,
}

/// Proxies rpc requests to compute nodes.
pub struct RpcProxy<A> {
    authenticator: A,
    /// Connections to compute nodes.
    conns: DashMap<ConnKey, ExecutionServiceClient<Channel>>,
}

impl<A: ProxyAuthenticator> RpcProxy<A> {
    pub fn new(authenticator: A) -> Self {
        RpcProxy {
            authenticator,
            conns: DashMap::new(),
        }
    }

    /// Connect to a compute node.
    ///
    /// This will read authentication params from the metadata map, get
    /// deployment info from Cloud, then return a connection to the requested
    /// deployment+compute engine.
    async fn connect(&self, meta: &MetadataMap) -> Result<ExecutionServiceClient<Channel>> {
        let params = AuthParams::from_metadata(meta)?;

        let details = self
            .authenticator
            .authenticate(
                params.user,
                params.password,
                params.db_name,
                params.org,
                params.compute_engine.unwrap_or(""),
            )
            .await?;

        let key = ConnKey {
            ip: details.ip,
            port: details.port,
        };

        // Already have a grpc connection,
        if let Some(conn) = self.conns.get(&key) {
            let conn = conn.clone();
            return Ok(conn);
        }

        // Otherwise need to create it.
        //
        // TODO: Assumes http, do we want https internally?
        let url = format!("http://{}:{}", key.ip, key.port);
        let channel = Endpoint::new(url)?
            .tcp_keepalive(Some(Duration::from_secs(600)))
            .tcp_nodelay(true)
            .keep_alive_while_idle(true)
            .connect()
            .await?;
        let client = ExecutionServiceClient::new(channel);

        // May have raced, but that's not a concern here.
        self.conns.insert(key, client.clone());

        Ok(client)
    }
}

#[async_trait]
impl<A: ProxyAuthenticator + 'static> ExecutionService for RpcProxy<A> {
    type ExecuteStream = Streaming<ExecuteResponse>;

    async fn initialize_session(
        &self,
        request: Request<InitializeSessionRequest>,
    ) -> Result<Response<InitializeSessionResponse>, Status> {
        let mut client = self.connect(request.metadata()).await?;
        client.initialize_session(request).await
    }

    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        let mut client = self.connect(request.metadata()).await?;
        client.execute(request).await
    }
}

/// Params used for cloud authentication.
struct AuthParams<'a> {
    user: &'a str,
    password: &'a str,
    db_name: &'a str,
    org: &'a str,
    compute_engine: Option<&'a str>,
}

impl<'a> AuthParams<'a> {
    fn from_metadata<'b: 'a>(meta: &'b MetadataMap) -> Result<Self> {
        let user = Self::get_val("user", meta)?;
        let password = Self::get_val("password", meta)?;
        let db_name = Self::get_val("db_name", meta)?;
        let org = Self::get_val("org", meta)?;

        let compute_engine = meta.get("compute_engine").map(|s| s.to_str()).transpose()?;

        Ok(AuthParams {
            user,
            password,
            db_name,
            org,
            compute_engine,
        })
    }

    fn get_val<'b>(key: &'static str, meta: &'b MetadataMap) -> Result<&'b str> {
        let val = meta
            .get(key)
            .ok_or(RpcsrvError::MissingAuthKey(key))?
            .to_str()?;
        Ok(val)
    }
}
