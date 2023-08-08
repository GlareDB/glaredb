use crate::errors::Result;
use protogen::gen::rpcsrv::service::{
    execution_service_client::ExecutionServiceClient, ExecuteRequest, ExecuteResponse,
    InitializeSessionRequest, InitializeSessionResponse,
};
use proxyutil::metada_constants::{
    COMPUTE_ENGINE_KEY, DB_NAME_KEY, ORG_KEY, PASSWORD_KEY, USER_KEY,
};
use std::sync::Arc;
use tonic::{
    metadata::MetadataMap,
    transport::{Channel, Endpoint},
    IntoRequest, Response, Status, Streaming,
};

/// Params that need to be set on grpc connections when going through the proxy.
pub struct ProxyAuthParams<'a> {
    /// User (generated Cloud credentials)
    pub user: &'a str,
    /// Password (generated Cloud credentials)
    pub password: &'a str,
    /// DB name
    pub db_name: &'a str,
    /// Org name.
    pub org: &'a str,
    /// Compute engine name.
    pub compute_engine: Option<&'a str>,
}

/// An execution service client that has additonal metadata attached to each
/// request for authentication through the proxy.
#[derive(Debug, Clone)]
pub struct AuthenticatedExecutionServiceClient {
    /// The inner client.
    client: ExecutionServiceClient<Channel>,

    /// The auth metadata that gets placed on all requests.
    auth_metadata: Arc<MetadataMap>,
}

impl AuthenticatedExecutionServiceClient {
    /// Connect to destination without any additional authentication metadata.
    pub async fn connect(
        dst: impl TryInto<Endpoint, Error = tonic::transport::Error>,
    ) -> Result<Self> {
        let client = ExecutionServiceClient::connect(dst).await?;
        Ok(AuthenticatedExecutionServiceClient {
            client,
            auth_metadata: Arc::new(MetadataMap::new()),
        })
    }

    /// Connect to a destination with the provided authentication params.
    pub async fn connect_with_proxy_auth_params<'a>(
        dst: impl TryInto<Endpoint, Error = tonic::transport::Error>,
        params: ProxyAuthParams<'a>,
    ) -> Result<Self> {
        let mut metadata = MetadataMap::new();
        metadata.insert(USER_KEY, params.user.parse()?);
        metadata.insert(PASSWORD_KEY, params.password.parse()?);
        metadata.insert(DB_NAME_KEY, params.db_name.parse()?);
        metadata.insert(ORG_KEY, params.org.parse()?);
        if let Some(compute_engine) = params.compute_engine {
            metadata.insert(COMPUTE_ENGINE_KEY, compute_engine.parse()?);
        }

        let client = ExecutionServiceClient::connect(dst).await?;

        Ok(AuthenticatedExecutionServiceClient {
            client,
            auth_metadata: Arc::new(metadata),
        })
    }

    pub async fn initialize_session(
        &mut self,
        request: impl IntoRequest<InitializeSessionRequest>,
    ) -> Result<Response<InitializeSessionResponse>, Status> {
        let mut request = request.into_request();
        self.append_auth_metadata(request.metadata_mut());
        self.client.initialize_session(request).await
    }

    pub async fn execute(
        &mut self,
        request: impl IntoRequest<ExecuteRequest>,
    ) -> Result<Response<Streaming<ExecuteResponse>>, Status> {
        let mut request = request.into_request();
        self.append_auth_metadata(request.metadata_mut());
        self.client.execute(request).await
    }

    fn append_auth_metadata(&self, metadata: &mut MetadataMap) {
        for kv in self.auth_metadata.iter() {
            match kv {
                tonic::metadata::KeyAndValueRef::Ascii(key, val) => {
                    metadata.append(key, val.clone())
                }
                tonic::metadata::KeyAndValueRef::Binary(key, val) => {
                    metadata.append_bin(key, val.clone())
                }
            };
        }
    }
}
