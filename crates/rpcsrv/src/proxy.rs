use crate::errors::{Result, RpcsrvError};
use async_trait::async_trait;
use dashmap::DashMap;
use protogen::gen::rpcsrv::service;
use protogen::gen::rpcsrv::service::execution_service_client::ExecutionServiceClient;
use protogen::rpcsrv::types::service::{
    InitializeSessionRequest, InitializeSessionRequestFromProxy, InitializeSessionResponse,
    SessionStorageConfig,
};
use proxyutil::cloudauth::{AuthParams, DatabaseDetails, ProxyAuthenticator, ServiceProtocol};
use proxyutil::metadata_constants::{
    COMPUTE_ENGINE_KEY, DB_NAME_KEY, ORG_KEY, PASSWORD_KEY, USER_KEY,
};
use std::{hash::Hash, time::Duration};
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
    ///
    /// Database details will be returned alongside the client.
    async fn connect(
        &self,
        meta: &MetadataMap,
    ) -> Result<(DatabaseDetails, ExecutionServiceClient<Channel>)> {
        let params = Self::auth_params_from_metadata(meta)?;

        // TODO: We'll want to figure out long-lived auth sessions to avoid
        // needing to hit Cloud for every request (e.g. JWT). This isn't a
        // problem for pgsrv since a connections map one-to-one with sessions,
        // and we only need to authenticate at the beginning of the connection.
        let details = self.authenticator.authenticate(params).await?;

        let key = ConnKey {
            ip: details.ip.clone(),
            port: details.port.clone(),
        };

        // Already have a grpc connection,
        if let Some(conn) = self.conns.get(&key) {
            let conn = conn.clone();
            return Ok((details, conn));
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

        Ok((details, client))
    }

    /// Inner implementation for initialize session.
    ///
    /// This takes care of translating the "client" request into the "proxy"
    /// request based on details from Cloud.
    async fn initialize_session_inner(
        &self,
        request: InitializeSessionRequest,
        details: DatabaseDetails,
        mut client: ExecutionServiceClient<Channel>,
    ) -> Result<InitializeSessionResponse> {
        match request {
            InitializeSessionRequest::Client(_req) => {
                // Create our "proxy" request based off the database details we
                // got back from Cloud.
                let db_id = Uuid::parse_str(&details.database_id)
                    .map_err(|e| RpcsrvError::InvalidId("database", e))?;
                let new_req = InitializeSessionRequest::Proxy(InitializeSessionRequestFromProxy {
                    storage_conf: SessionStorageConfig {
                        gcs_bucket: Some(details.gcs_storage_bucket),
                    },
                    db_id,
                });

                // And proxy it forward.
                Ok(client
                    .initialize_session(Request::new(new_req.into()))
                    .await?
                    .into_inner()
                    .try_into()?)
            }
            InitializeSessionRequest::Proxy(_) => Err(RpcsrvError::SessionInitalizeError(
                "unexpectedly got proxy request from client".to_string(),
            )),
        }
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
impl<A: ProxyAuthenticator + 'static> service::execution_service_server::ExecutionService
    for RpcProxyHandler<A>
{
    type PhysicalPlanExecuteStream = Streaming<service::RecordBatchResponse>;

    async fn initialize_session(
        &self,
        request: Request<service::InitializeSessionRequest>,
    ) -> Result<Response<service::InitializeSessionResponse>, Status> {
        info!("initialize session (proxy)");
        let (details, client) = self.connect(request.metadata()).await?;
        let resp = self
            .initialize_session_inner(request.into_inner().try_into()?, details, client)
            .await?;
        Ok(Response::new(resp.try_into()?))
    }

    async fn create_physical_plan(
        &self,
        request: Request<service::CreatePhysicalPlanRequest>,
    ) -> Result<Response<service::PhysicalPlanResponse>, Status> {
        info!("create physical plan (proxy)");
        let (_, mut client) = self.connect(request.metadata()).await?;
        client.create_physical_plan(request).await
    }

    async fn dispatch_access(
        &self,
        request: Request<service::DispatchAccessRequest>,
    ) -> Result<Response<service::TableProviderResponse>, Status> {
        info!("dispatch access (proxy)");
        let (_, mut client) = self.connect(request.metadata()).await?;
        client.dispatch_access(request).await
    }

    async fn table_provider_scan(
        &self,
        request: Request<service::TableProviderScanRequest>,
    ) -> Result<Response<service::PhysicalPlanResponse>, Status> {
        info!("table provider scan (proxy)");
        let (_, mut client) = self.connect(request.metadata()).await?;
        client.table_provider_scan(request).await
    }

    async fn table_provider_insert_into(
        &self,
        request: Request<service::TableProviderInsertIntoRequest>,
    ) -> Result<Response<service::PhysicalPlanResponse>, Status> {
        info!("table provider insert into (proxy)");
        let (_, mut client) = self.connect(request.metadata()).await?;
        client.table_provider_insert_into(request).await
    }

    async fn physical_plan_execute(
        &self,
        request: Request<service::PhysicalPlanExecuteRequest>,
    ) -> Result<Response<Self::PhysicalPlanExecuteStream>, Status> {
        info!("physical plan execute (proxy)");
        let (_, mut client) = self.connect(request.metadata()).await?;
        client.physical_plan_execute(request).await
    }

    async fn broadcast_exchange(
        &self,
        request: Request<Streaming<service::BroadcastExchangeRequest>>,
    ) -> Result<Response<service::BroadcastExchangeResponse>, Status> {
        unimplemented!()
        // let resp = self
        //     .physical_plan_execute_inner(request.into_inner().try_into()?)
        //     .await?;
        // Ok(Response::new(Box::pin(resp)))
    }

    async fn close_session(
        &self,
        request: Request<service::CloseSessionRequest>,
    ) -> Result<Response<service::CloseSessionResponse>, Status> {
        info!("close session (proxy)");
        let (_, mut client) = self.connect(request.metadata()).await?;
        client.close_session(request).await
    }
}
