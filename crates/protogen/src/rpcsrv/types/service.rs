use datafusion::arrow::datatypes::Schema;
use prost::Message;
use uuid::Uuid;

use crate::{
    errors::ProtoConvError,
    gen::rpcsrv::service::{self, ExternalTableReference, InternalTableReference},
    metastore::types::{catalog::CatalogState, FromOptionalField},
};

pub struct SessionStorageConfig {
    pub gcs_bucket: Option<String>,
}

impl TryFrom<service::SessionStorageConfig> for SessionStorageConfig {
    type Error = ProtoConvError;
    fn try_from(value: service::SessionStorageConfig) -> Result<Self, Self::Error> {
        Ok(SessionStorageConfig {
            gcs_bucket: value.gcs_bucket,
        })
    }
}

impl From<SessionStorageConfig> for service::SessionStorageConfig {
    fn from(value: SessionStorageConfig) -> Self {
        service::SessionStorageConfig {
            gcs_bucket: value.gcs_bucket,
        }
    }
}

pub struct InitializeSessionRequestFromClient {
    pub test_db_id: Option<Uuid>,
}

impl TryFrom<service::InitializeSessionRequestFromClient> for InitializeSessionRequestFromClient {
    type Error = ProtoConvError;
    fn try_from(value: service::InitializeSessionRequestFromClient) -> Result<Self, Self::Error> {
        Ok(Self {
            test_db_id: value
                .test_db_id
                .map(|id| Uuid::from_slice(&id))
                .transpose()?,
        })
    }
}

impl From<InitializeSessionRequestFromClient> for service::InitializeSessionRequestFromClient {
    fn from(value: InitializeSessionRequestFromClient) -> Self {
        Self {
            test_db_id: value.test_db_id.map(|id| id.into_bytes().into()),
        }
    }
}

pub struct InitializeSessionRequestFromProxy {
    pub storage_conf: SessionStorageConfig,
    pub db_id: Uuid,
}

impl TryFrom<service::InitializeSessionRequestFromProxy> for InitializeSessionRequestFromProxy {
    type Error = ProtoConvError;
    fn try_from(value: service::InitializeSessionRequestFromProxy) -> Result<Self, Self::Error> {
        Ok(Self {
            storage_conf: value.storage_conf.required("storage configuration")?,
            db_id: Uuid::from_slice(&value.db_id)?,
        })
    }
}

impl From<InitializeSessionRequestFromProxy> for service::InitializeSessionRequestFromProxy {
    fn from(value: InitializeSessionRequestFromProxy) -> Self {
        Self {
            storage_conf: Some(value.storage_conf.into()),
            db_id: value.db_id.into_bytes().into(),
        }
    }
}

pub enum InitializeSessionRequest {
    Client(InitializeSessionRequestFromClient),
    Proxy(InitializeSessionRequestFromProxy),
}

impl TryFrom<service::initialize_session_request::Request> for InitializeSessionRequest {
    type Error = ProtoConvError;
    fn try_from(value: service::initialize_session_request::Request) -> Result<Self, Self::Error> {
        Ok(match value {
            service::initialize_session_request::Request::Client(c) => {
                InitializeSessionRequest::Client(c.try_into()?)
            }
            service::initialize_session_request::Request::Proxy(c) => {
                InitializeSessionRequest::Proxy(c.try_into()?)
            }
        })
    }
}

impl From<InitializeSessionRequest> for service::initialize_session_request::Request {
    fn from(value: InitializeSessionRequest) -> Self {
        match value {
            InitializeSessionRequest::Client(c) => Self::Client(c.into()),
            InitializeSessionRequest::Proxy(c) => Self::Proxy(c.into()),
        }
    }
}

impl TryFrom<service::InitializeSessionRequest> for InitializeSessionRequest {
    type Error = ProtoConvError;
    fn try_from(value: service::InitializeSessionRequest) -> Result<Self, Self::Error> {
        value.request.required("initialize session request")
    }
}

impl From<InitializeSessionRequest> for service::InitializeSessionRequest {
    fn from(value: InitializeSessionRequest) -> Self {
        Self {
            request: Some(value.into()),
        }
    }
}

pub struct InitializeSessionResponse {
    pub session_id: Uuid,
    pub catalog: CatalogState,
}

impl TryFrom<service::InitializeSessionResponse> for InitializeSessionResponse {
    type Error = ProtoConvError;
    fn try_from(value: service::InitializeSessionResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            session_id: Uuid::from_slice(&value.session_id)?,
            catalog: value.catalog.required("catalog state")?,
        })
    }
}

impl TryFrom<InitializeSessionResponse> for service::InitializeSessionResponse {
    type Error = ProtoConvError;
    fn try_from(value: InitializeSessionResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            session_id: value.session_id.into_bytes().into(),
            catalog: Some(value.catalog.try_into()?),
        })
    }
}

pub struct FetchCatalogRequest {
    pub session_id: Uuid,
}

impl TryFrom<service::FetchCatalogRequest> for FetchCatalogRequest {
    type Error = ProtoConvError;
    fn try_from(value: service::FetchCatalogRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            session_id: Uuid::from_slice(&value.session_id)?,
        })
    }
}

impl From<FetchCatalogRequest> for service::FetchCatalogRequest {
    fn from(value: FetchCatalogRequest) -> Self {
        Self {
            session_id: value.session_id.into_bytes().into(),
        }
    }
}

pub struct FetchCatalogResponse {
    pub catalog: CatalogState,
}

impl TryFrom<service::FetchCatalogResponse> for FetchCatalogResponse {
    type Error = ProtoConvError;
    fn try_from(value: service::FetchCatalogResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            catalog: value.catalog.required("catalog state")?,
        })
    }
}

impl TryFrom<FetchCatalogResponse> for service::FetchCatalogResponse {
    type Error = ProtoConvError;
    fn try_from(value: FetchCatalogResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            catalog: Some(value.catalog.try_into()?),
        })
    }
}

pub struct DispatchAccessRequest {
    pub session_id: Uuid,
    pub table_ref: ResolvedTableReference,
}

impl TryFrom<service::DispatchAccessRequest> for DispatchAccessRequest {
    type Error = ProtoConvError;
    fn try_from(value: service::DispatchAccessRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            session_id: Uuid::from_slice(&value.session_id)?,
            table_ref: value.table_ref.required("table reference")?,
        })
    }
}

impl From<DispatchAccessRequest> for service::DispatchAccessRequest {
    fn from(value: DispatchAccessRequest) -> Self {
        Self {
            session_id: value.session_id.into_bytes().into(),
            table_ref: Some(value.table_ref.into()),
        }
    }
}

pub struct PhysicalPlanExecuteRequest {
    pub session_id: Uuid,
    pub physical_plan: Vec<u8>,
}

impl TryFrom<service::PhysicalPlanExecuteRequest> for PhysicalPlanExecuteRequest {
    type Error = ProtoConvError;
    fn try_from(value: service::PhysicalPlanExecuteRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            session_id: Uuid::from_slice(&value.session_id)?,
            physical_plan: value.physical_plan,
        })
    }
}

impl From<PhysicalPlanExecuteRequest> for service::PhysicalPlanExecuteRequest {
    fn from(value: PhysicalPlanExecuteRequest) -> Self {
        Self {
            session_id: value.session_id.into_bytes().into(),
            physical_plan: value.physical_plan,
        }
    }
}

pub struct TableProviderResponse {
    pub id: Uuid,
    pub schema: Schema,
}

impl TryFrom<service::TableProviderResponse> for TableProviderResponse {
    type Error = ProtoConvError;
    fn try_from(value: service::TableProviderResponse) -> Result<Self, Self::Error> {
        let schema = datafusion_proto::protobuf::Schema::decode(value.schema.as_slice())?;
        let schema = (&schema).try_into()?;
        Ok(Self {
            id: Uuid::from_slice(&value.id)?,
            schema,
        })
    }
}

impl TryFrom<TableProviderResponse> for service::TableProviderResponse {
    type Error = ProtoConvError;
    fn try_from(value: TableProviderResponse) -> Result<Self, Self::Error> {
        let schema = datafusion_proto::protobuf::Schema::try_from(&value.schema)?;
        Ok(Self {
            id: value.id.into_bytes().into(),
            schema: schema.encode_to_vec(),
        })
    }
}

// Table Reference

#[derive(Debug, Clone)]
pub enum ResolvedTableReference {
    Internal {
        table_oid: u32,
    },
    External {
        database: String,
        schema: String,
        name: String,
    },
}

impl TryFrom<service::ResolvedTableReference> for ResolvedTableReference {
    type Error = ProtoConvError;
    fn try_from(value: service::ResolvedTableReference) -> Result<Self, Self::Error> {
        let reference = value
            .reference
            .ok_or_else(|| ProtoConvError::RequiredField("reference".to_string()))?;

        Ok(match reference {
            service::resolved_table_reference::Reference::Internal(InternalTableReference {
                table_oid,
            }) => ResolvedTableReference::Internal { table_oid },
            service::resolved_table_reference::Reference::External(ExternalTableReference {
                database,
                schema,
                name,
            }) => ResolvedTableReference::External {
                database,
                schema,
                name,
            },
        })
    }
}

impl From<ResolvedTableReference> for service::ResolvedTableReference {
    fn from(value: ResolvedTableReference) -> Self {
        match value {
            ResolvedTableReference::Internal { table_oid } => service::ResolvedTableReference {
                reference: Some(service::resolved_table_reference::Reference::Internal(
                    InternalTableReference { table_oid },
                )),
            },
            ResolvedTableReference::External {
                database,
                schema,
                name,
            } => service::ResolvedTableReference {
                reference: Some(service::resolved_table_reference::Reference::External(
                    ExternalTableReference {
                        database,
                        schema,
                        name,
                    },
                )),
            },
        }
    }
}

pub struct CloseSessionRequest {
    pub session_id: Uuid,
}

impl TryFrom<service::CloseSessionRequest> for CloseSessionRequest {
    type Error = ProtoConvError;
    fn try_from(value: service::CloseSessionRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            session_id: Uuid::from_slice(&value.session_id)?,
        })
    }
}

impl From<CloseSessionRequest> for service::CloseSessionRequest {
    fn from(value: CloseSessionRequest) -> Self {
        Self {
            session_id: value.session_id.into_bytes().into(),
        }
    }
}

pub struct CloseSessionResponse {}

impl From<service::CloseSessionResponse> for CloseSessionResponse {
    fn from(_value: service::CloseSessionResponse) -> Self {
        Self {}
    }
}

impl From<CloseSessionResponse> for service::CloseSessionResponse {
    fn from(_value: CloseSessionResponse) -> Self {
        Self {}
    }
}
