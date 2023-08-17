use crate::{
    errors::{ExecError, Result},
    extension_codec::GlareDBExtensionCodec,
    metastore::catalog::SessionCatalog,
};
use datafusion::{common::OwnedTableReference, logical_expr::LogicalPlan, prelude::Expr};
use datafusion_proto::{logical_plan::AsLogicalPlan, protobuf::LogicalPlanNode};
use protogen::{
    gen::rpcsrv::service::{self, execution_service_client::ExecutionServiceClient},
    rpcsrv::types::service::{
        CloseSessionRequest, CreatePhysicalPlanRequest, DispatchAccessRequest,
        InitializeSessionRequest, InitializeSessionResponse, PhysicalPlanExecuteRequest,
        PhysicalPlanResponse, TableProviderInsertIntoRequest, TableProviderResponse,
        TableProviderScanRequest,
    },
};
use proxyutil::metadata_constants::{
    COMPUTE_ENGINE_KEY, DB_NAME_KEY, ORG_KEY, PASSWORD_KEY, USER_KEY,
};
use std::sync::Arc;
use tonic::{
    metadata::MetadataMap,
    transport::{Channel, Endpoint},
    IntoRequest, Streaming,
};
use url::Url;
use uuid::Uuid;

use super::{exec::RemoteExecutionPlan, table::RemoteTableProvider};

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
pub struct ProxyDestination {
    pub params: ProxyAuthParams,
    pub dst: Url,
}

impl TryFrom<Url> for ProxyDestination {
    type Error = ExecError;

    /// Try to parse authentication parameters and destinaton from a url.
    fn try_from(value: Url) -> Result<Self, Self::Error> {
        if value.scheme() != "glaredb" {
            return Err(ExecError::InvalidRemoteExecUrl(
                "URL must start with 'glaredb://'".to_string(),
            ));
        }

        let user = value.username();
        let password = value
            .password()
            .ok_or_else(|| ExecError::InvalidRemoteExecUrl("Missing password".to_string()))?;

        let host = value
            .host_str()
            .ok_or_else(|| ExecError::InvalidRemoteExecUrl("URL is missing a host".to_string()))?;

        // Host should be in the form "orgname.remote.glaredb.com"
        let (org, host) = host
            .split_once('.')
            .ok_or_else(|| ExecError::InvalidRemoteExecUrl("Invalid host".to_string()))?;

        // Remove leading slash from path, use that as database name.
        let db_name = value.path().trim_start_matches('/');
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
            value.port().unwrap_or(DEFAULT_RPC_PROXY_PORT)
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

        Ok(ProxyDestination { params, dst })
    }
}

/// An execution service client that has additonal metadata attached to each
/// request for authentication through the proxy.
#[derive(Debug, Clone)]
pub struct RemoteClient {
    /// The inner client.
    client: ExecutionServiceClient<Channel>,

    /// The auth metadata that gets placed on all requests.
    auth_metadata: Arc<MetadataMap>,
}

impl RemoteClient {
    /// Connect to destination without any additional authentication metadata.
    ///
    /// This can be used for testing (and possibly for inter-node
    /// communication?).
    pub async fn connect(dst: Url) -> Result<Self> {
        let client = ExecutionServiceClient::connect(dst.to_string()).await?;
        Ok(RemoteClient {
            client,
            auth_metadata: Arc::new(MetadataMap::new()),
        })
    }

    /// Connect to a proxy destination.
    pub async fn connect_with_proxy_destination(dst: ProxyDestination) -> Result<Self> {
        Self::connect_with_proxy_auth_params(dst.dst.to_string(), dst.params).await
    }

    /// Connect to a destination with the provided authentication params.
    async fn connect_with_proxy_auth_params<'a>(
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

        Ok(RemoteClient {
            client,
            auth_metadata: Arc::new(metadata),
        })
    }

    pub async fn initialize_session(
        &mut self,
        request: InitializeSessionRequest,
    ) -> Result<(RemoteSessionClient, SessionCatalog)> {
        let mut request = service::InitializeSessionRequest::from(request).into_request();
        self.append_auth_metadata(request.metadata_mut());

        let resp = self.client.initialize_session(request).await.map_err(|e| {
            ExecError::RemoteSession(format!("failed to initialize remote session: {e}"))
        })?;
        let resp: InitializeSessionResponse = resp.into_inner().try_into()?;

        let remote_sess_client = RemoteSessionClient {
            session_id: resp.session_id,
            inner: self.clone(),
        };

        Ok((
            remote_sess_client,
            SessionCatalog::new(Arc::new(resp.catalog)),
        ))
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

/// A client to interact with the current active remote session.
#[derive(Debug, Clone)]
pub struct RemoteSessionClient {
    inner: RemoteClient,
    session_id: Uuid,
}

impl RemoteSessionClient {
    /// Returns the current session ID.
    pub fn session_id(&self) -> Uuid {
        self.session_id
    }

    pub async fn create_physical_plan(
        &mut self,
        logical_plan: &LogicalPlan,
    ) -> Result<RemoteExecutionPlan> {
        // Encode the logical plan into a protobuf message.
        let logical_plan = {
            let node = LogicalPlanNode::try_from_logical_plan(
                logical_plan,
                &GlareDBExtensionCodec::new_encoder(),
            )?;
            let mut buf = Vec::new();
            node.try_encode(&mut buf)?;
            buf
        };

        let mut request = service::CreatePhysicalPlanRequest::from(CreatePhysicalPlanRequest {
            session_id: self.session_id(),
            logical_plan,
        })
        .into_request();
        self.inner.append_auth_metadata(request.metadata_mut());
        // send the request
        let resp: PhysicalPlanResponse = self
            .inner
            .client
            .create_physical_plan(request)
            .await
            .map_err(|e| {
                ExecError::RemoteSession(format!(
                    "cannot create physical plan from logical plan: {e}"
                ))
            })?
            .into_inner()
            .try_into()?;

        Ok(RemoteExecutionPlan::new(
            self.clone(),
            resp.id,
            Arc::new(resp.schema),
        ))
    }

    pub async fn dispatch_access(
        &mut self,
        table_ref: OwnedTableReference,
    ) -> Result<RemoteTableProvider> {
        let mut request = service::DispatchAccessRequest::from(DispatchAccessRequest {
            session_id: self.session_id(),
            table_ref,
        })
        .into_request();
        self.inner.append_auth_metadata(request.metadata_mut());

        let resp: TableProviderResponse = self
            .inner
            .client
            .dispatch_access(request)
            .await
            .map_err(|e| ExecError::RemoteSession(format!("unable to dispatch table access: {e}")))?
            .into_inner()
            .try_into()?;

        Ok(RemoteTableProvider::new(
            self.clone(),
            resp.id,
            Arc::new(resp.schema),
        ))
    }

    pub async fn table_provider_scan(
        &mut self,
        provider_id: Uuid,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<RemoteExecutionPlan> {
        let mut request = service::TableProviderScanRequest::try_from(TableProviderScanRequest {
            session_id: self.session_id(),
            provider_id,
            projection: projection.cloned(),
            filters: filters.to_vec(),
            limit,
        })?
        .into_request();
        self.inner.append_auth_metadata(request.metadata_mut());

        let resp: PhysicalPlanResponse = self
            .inner
            .client
            .table_provider_scan(request)
            .await
            .map_err(|e| ExecError::RemoteSession(format!("unable to scan table provider: {e}")))?
            .into_inner()
            .try_into()?;

        Ok(RemoteExecutionPlan::new(
            self.clone(),
            resp.id,
            Arc::new(resp.schema),
        ))
    }

    pub async fn table_provider_insert_into(
        &mut self,
        provider_id: Uuid,
        input_exec_id: Uuid,
    ) -> Result<RemoteExecutionPlan> {
        let mut request =
            service::TableProviderInsertIntoRequest::from(TableProviderInsertIntoRequest {
                session_id: self.session_id(),
                provider_id,
                input_exec_id,
            })
            .into_request();
        self.inner.append_auth_metadata(request.metadata_mut());

        let resp: PhysicalPlanResponse = self
            .inner
            .client
            .table_provider_insert_into(request)
            .await
            .map_err(|e| {
                ExecError::RemoteSession(format!("unable to insert into table provider: {e}"))
            })?
            .into_inner()
            .try_into()?;

        Ok(RemoteExecutionPlan::new(
            self.clone(),
            resp.id,
            Arc::new(resp.schema),
        ))
    }

    pub async fn physical_plan_execute(
        &mut self,
        exec_id: Uuid,
    ) -> Result<Streaming<service::RecordBatchResponse>> {
        let mut request = service::PhysicalPlanExecuteRequest::from(PhysicalPlanExecuteRequest {
            session_id: self.session_id(),
            exec_id,
        })
        .into_request();
        self.inner.append_auth_metadata(request.metadata_mut());

        let resp = self
            .inner
            .client
            .physical_plan_execute(request)
            .await
            .map_err(|e| {
                ExecError::RemoteSession(format!("error while executing physical plan: {e}"))
            })?
            .into_inner();
        Ok(resp)
    }

    pub async fn broadcast_exchange(
        &mut self,
        stream: impl tonic::IntoStreamingRequest<Message = service::BroadcastExchangeRequest>,
    ) -> Result<()> {
        let _req = self.inner.client.broadcast_exchange(stream).await?;
        Ok(())
    }

    pub async fn close_session(&mut self) -> Result<()> {
        let mut request = service::CloseSessionRequest::from(CloseSessionRequest {
            session_id: self.session_id(),
        })
        .into_request();
        self.inner.append_auth_metadata(request.metadata_mut());

        let _resp = self
            .inner
            .client
            .close_session(request)
            .await
            .map_err(|e| {
                ExecError::RemoteSession(format!(
                    "unable to close session {}: {}",
                    self.session_id(),
                    e
                ))
            })?
            .into_inner();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn params_from_url_valid_default_port() {
        let out = ProxyDestination::try_from(
            Url::parse("glaredb://user:password@org.remote.glaredb.com/db").unwrap(),
        )
        .unwrap();

        let expected = ProxyDestination {
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
        let out = ProxyDestination::try_from(
            Url::parse("glaredb://user:password@org.remote.glaredb.com:4444/engine.db").unwrap(),
        )
        .unwrap();

        let expected = ProxyDestination {
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
        ProxyDestination::try_from(
            Url::parse("http://user:password@org.remote.glaredb.com:4444/engine.db").unwrap(),
        )
        .unwrap_err();

        // Missing password
        ProxyDestination::try_from(
            Url::parse("glaredb://user@org.remote.glaredb.com:4444/engine.db").unwrap(),
        )
        .unwrap_err();

        // Missing db name
        ProxyDestination::try_from(
            Url::parse("glaredb://user:password@org.remote.glaredb.com:4444/").unwrap(),
        )
        .unwrap_err();
    }
}
