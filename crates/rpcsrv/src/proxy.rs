use crate::errors::{Result, RpcsrvError};
use async_trait::async_trait;
use dashmap::DashMap;
use protogen::gen::rpcsrv::service::{
    execution_service_client::ExecutionServiceClient, execution_service_server::ExecutionService,
    CloseSessionRequest, CloseSessionResponse, ExecuteRequest, ExecuteResponse,
    InitializeSessionRequest, InitializeSessionResponse,
};
use proxyutil::cloudauth::{AuthParams, ProxyAuthenticator, ServiceProtocol};
use proxyutil::metada_constants::{
    COMPUTE_ENGINE_KEY, DB_NAME_KEY, ORG_KEY, PASSWORD_KEY, USER_KEY,
};
use std::{hash::Hash, time::Duration};
use tonic::{
    metadata::MetadataMap,
    transport::{Channel, Endpoint},
    Request, Response, Status, Streaming,
};
use tracing::info;

/// Key used for the connections map.
// TODO: Possibly per user connections?
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ConnKey {
    ip: String,
    port: String,
}

/// Proxies rpc requests to compute nodes.
pub struct RpcProxyHandler<A> {
    authenticator: A,
    /// Connections to compute nodes.
    conns: DashMap<ConnKey, ExecutionServiceClient<Channel>>,
}

impl<A: ProxyAuthenticator> RpcProxyHandler<A> {
    pub fn new(authenticator: A) -> Self {
        RpcProxyHandler {
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
        let params = Self::auth_params_from_metadata(meta)?;

        // TODO: We'll want to figure out long-lived auth sessions to avoid
        // needing to hit Cloud for every request (e.g. JWT). This isn't a
        // problem for pgsrv since a connections map one-to-one with sessions,
        // and we only need to authenticate at the beginning of the connection.
        let details = self.authenticator.authenticate(params).await?;

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

    fn auth_params_from_metadata(meta: &MetadataMap) -> Result<AuthParams> {
        fn get_val<'b>(key: &'static str, meta: &'b MetadataMap) -> Result<&'b str> {
            let val = meta
                .get(key)
                .ok_or(RpcsrvError::MissingAuthKey(key))?
                .to_str()?;
            Ok(val)
        }

        let user = get_val(USER_KEY, meta)?;
        let password = get_val(PASSWORD_KEY, meta)?;
        let db_name = get_val(DB_NAME_KEY, meta)?;
        let org = get_val(ORG_KEY, meta)?;

        let compute_engine = meta
            .get(COMPUTE_ENGINE_KEY)
            .map(|s| s.to_str())
            .transpose()?;

        Ok(AuthParams {
            user,
            password,
            db_name,
            org,
            compute_engine,
            service: ServiceProtocol::RpcSrv,
        })
    }
}

#[async_trait]
impl<A: ProxyAuthenticator + 'static> ExecutionService for RpcProxyHandler<A> {
    type ExecuteStream = Streaming<ExecuteResponse>;

    async fn initialize_session(
        &self,
        request: Request<InitializeSessionRequest>,
    ) -> Result<Response<InitializeSessionResponse>, Status> {
        info!("initialize session (proxy)");
        let mut client = self.connect(request.metadata()).await?;
        client.initialize_session(request).await
    }

    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        info!("execute (proxy)");
        let mut client = self.connect(request.metadata()).await?;
        client.execute(request).await
    }

    async fn close_session(
        &self,
        request: Request<CloseSessionRequest>,
    ) -> Result<Response<CloseSessionResponse>, Status> {
        info!("close session (proxy)");
        let mut client = self.connect(request.metadata()).await?;
        client.close_session(request).await
    }
}
