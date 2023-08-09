use crate::errors::{ExecError, Result};
use protogen::gen::rpcsrv::service::{
    execution_service_client::ExecutionServiceClient, CloseSessionRequest, CloseSessionResponse,
    ExecuteRequest, ExecuteResponse, InitializeSessionRequest, InitializeSessionResponse,
};
use proxyutil::metadata_constants::{
    COMPUTE_ENGINE_KEY, DB_NAME_KEY, ORG_KEY, PASSWORD_KEY, USER_KEY,
};
use std::sync::Arc;
use tonic::{
    metadata::MetadataMap,
    transport::{Channel, Endpoint},
    IntoRequest, Response, Status, Streaming,
};
use url::Url;

const DEFAULT_RPC_PROXY_PORT: u16 = 6443;

/// Params that need to be set on grpc connections when going through the proxy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyAuthParams {
    /// User (generated Cloud credentials)
    pub user: String,
    /// Password (generated Cloud credentials)
    pub password: String,
    /// DB name
    pub db_name: String,
    /// Org name.
    pub org: String,
    /// Compute engine name.
    pub compute_engine: Option<String>,
}

/// Auth params and destination to use when connecting the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyAuthParamsAndDst {
    pub params: ProxyAuthParams,
    pub dst: Url,
}

impl ProxyAuthParamsAndDst {
    /// Try to parse authentication parameters and destinaton from a url.
    pub fn try_from_url(url: Url) -> Result<Self> {
        if url.scheme() != "glaredb" {
            return Err(ExecError::InvalidRemoteExecUrl(
                "URL must start with 'glaredb://'".to_string(),
            ));
        }

        let user = url.username();
        let password = url
            .password()
            .ok_or_else(|| ExecError::InvalidRemoteExecUrl("Missing password".to_string()))?;

        let host = url
            .host_str()
            .ok_or_else(|| ExecError::InvalidRemoteExecUrl("URL is missing a host".to_string()))?;

        // Host should be in the form "orgname.remote.glaredb.com"
        let (org, host) = host
            .split_once('.')
            .ok_or_else(|| ExecError::InvalidRemoteExecUrl("Invalid host".to_string()))?;

        // Remove leading slash from path, use that as database name.
        let db_name = url.path().trim_start_matches('/');
        if db_name.is_empty() {
            return Err(ExecError::InvalidRemoteExecUrl(
                "Missing db name".to_string(),
            ));
        }

        // Database name could be just the name itself, or may be in the form of
        // "engine.dbname".
        let (compute_engine, db_name) =
            if let Some((compute_engine, db_name)) = db_name.split_once('.') {
                (Some(compute_engine), db_name)
            } else {
                (None, db_name)
            };

        // Rebuild url that we should actually connect to.
        let dst = Url::parse(&format!(
            "http://{host}:{}",
            url.port().unwrap_or(DEFAULT_RPC_PROXY_PORT)
        ))
        .map_err(|e| {
            ExecError::Internal(format!("fail to parse reconstructed host and port: {e}"))
        })?;

        let params = ProxyAuthParams {
            user: user.to_string(),
            password: password.to_string(),
            db_name: db_name.to_string(),
            org: org.to_string(),
            compute_engine: compute_engine.map(String::from),
        };

        println!("{params:+?}");

        Ok(ProxyAuthParamsAndDst { params, dst })
    }
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
    ///
    /// This can be used for testing (and possibly for inter-node
    /// communication?).
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
        params: ProxyAuthParams,
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

    pub async fn close_session(
        &mut self,
        request: impl IntoRequest<CloseSessionRequest>,
    ) -> Result<Response<CloseSessionResponse>, Status> {
        let mut request = request.into_request();
        self.append_auth_metadata(request.metadata_mut());
        self.client.close_session(request).await
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn params_from_url_valid_default_port() {
        let out = ProxyAuthParamsAndDst::try_from_url(
            Url::parse("glaredb://user:password@org.remote.glaredb.com/db").unwrap(),
        )
        .unwrap();

        let expected = ProxyAuthParamsAndDst {
            params: ProxyAuthParams {
                user: "user".to_string(),
                password: "password".to_string(),
                db_name: "db".to_string(),
                org: "org".to_string(),
                compute_engine: None,
            },
            dst: Url::parse("http://remote.glaredb.com:6443").unwrap(),
        };

        assert_eq!(expected, out);
    }

    #[test]
    fn params_from_url_valid_port_and_engine() {
        let out = ProxyAuthParamsAndDst::try_from_url(
            Url::parse("glaredb://user:password@org.remote.glaredb.com:4444/engine.db").unwrap(),
        )
        .unwrap();

        let expected = ProxyAuthParamsAndDst {
            params: ProxyAuthParams {
                user: "user".to_string(),
                password: "password".to_string(),
                db_name: "db".to_string(),
                org: "org".to_string(),
                compute_engine: Some("engine".to_string()),
            },
            dst: Url::parse("http://remote.glaredb.com:4444").unwrap(),
        };

        assert_eq!(expected, out);
    }

    #[test]
    fn params_from_url_invalid() {
        // Invalid scheme
        ProxyAuthParamsAndDst::try_from_url(
            Url::parse("http://user:password@org.remote.glaredb.com:4444/engine.db").unwrap(),
        )
        .unwrap_err();

        // Missing password
        ProxyAuthParamsAndDst::try_from_url(
            Url::parse("glaredb://user@org.remote.glaredb.com:4444/engine.db").unwrap(),
        )
        .unwrap_err();

        // Missing db name
        ProxyAuthParamsAndDst::try_from_url(
            Url::parse("glaredb://user:password@org.remote.glaredb.com:4444/").unwrap(),
        )
        .unwrap_err();
    }
}
