use std::{collections::HashMap, fmt::Display};

use datafusion::arrow::datatypes::Schema;
use prost::Message;
use uuid::Uuid;

use crate::{
    errors::ProtoConvError,
    gen::rpcsrv::service::{self, ExternalTableReference, InternalTableReference},
    metastore::types::{catalog::CatalogState, FromOptionalField},
};

use super::func_param_value::FuncParamValue;

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
    pub database_id: Uuid,
    pub catalog: CatalogState,
}

impl TryFrom<service::InitializeSessionResponse> for InitializeSessionResponse {
    type Error = ProtoConvError;
    fn try_from(value: service::InitializeSessionResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            database_id: Uuid::from_slice(&value.database_id)?,
            catalog: value.catalog.required("catalog state")?,
        })
    }
}

impl TryFrom<InitializeSessionResponse> for service::InitializeSessionResponse {
    type Error = ProtoConvError;
    fn try_from(value: InitializeSessionResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            database_id: value.database_id.into_bytes().into(),
            catalog: Some(value.catalog.try_into()?),
        })
    }
}

pub struct FetchCatalogRequest {
    pub database_id: Uuid,
}

impl TryFrom<service::FetchCatalogRequest> for FetchCatalogRequest {
    type Error = ProtoConvError;
    fn try_from(value: service::FetchCatalogRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            database_id: Uuid::from_slice(&value.database_id)?,
        })
    }
}

impl From<FetchCatalogRequest> for service::FetchCatalogRequest {
    fn from(value: FetchCatalogRequest) -> Self {
        Self {
            database_id: value.database_id.into_bytes().into(),
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

#[derive(Debug)]
pub struct DispatchAccessRequest {
    pub database_id: Uuid,
    pub table_ref: ResolvedTableReference,
    pub args: Option<Vec<FuncParamValue>>,

    pub opts: Option<HashMap<String, FuncParamValue>>,
}

impl TryFrom<service::DispatchAccessRequest> for DispatchAccessRequest {
    type Error = ProtoConvError;
    fn try_from(value: service::DispatchAccessRequest) -> Result<Self, Self::Error> {
        let args = value
            .args
            .iter()
            .map(|v| FuncParamValue::decode(v.as_slice()).map_err(ProtoConvError::from))
            .collect::<Result<Vec<_>, Self::Error>>()?;
        let opts = value
            .options
            .into_iter()
            .map(|(k, v)| {
                Ok((
                    k,
                    FuncParamValue::decode(v.as_slice()).map_err(ProtoConvError::from)?,
                ))
            })
            .collect::<Result<HashMap<_, _>, Self::Error>>()?;

        let args = if args.is_empty() { None } else { Some(args) };
        let opts = if opts.is_empty() { None } else { Some(opts) };

        Ok(Self {
            database_id: Uuid::from_slice(&value.database_id)?,
            table_ref: value.table_ref.required("table reference")?,
            args,
            opts,
        })
    }
}

impl From<DispatchAccessRequest> for service::DispatchAccessRequest {
    fn from(value: DispatchAccessRequest) -> Self {
        let args = value
            .args
            .map(|v| v.into_iter().map(|v| v.encode_to_vec()).collect::<Vec<_>>())
            .unwrap_or_default();

        let options = value
            .opts
            .map(|v| {
                v.into_iter()
                    .map(|(k, v)| (k, v.encode_to_vec()))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();

        Self {
            database_id: value.database_id.into_bytes().into(),
            table_ref: Some(value.table_ref.into()),
            args,
            options,
        }
    }
}

pub struct PhysicalPlanExecuteRequest {
    pub database_id: Uuid,
    pub physical_plan: Vec<u8>,
}

impl TryFrom<service::PhysicalPlanExecuteRequest> for PhysicalPlanExecuteRequest {
    type Error = ProtoConvError;
    fn try_from(value: service::PhysicalPlanExecuteRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            database_id: Uuid::from_slice(&value.database_id)?,
            physical_plan: value.physical_plan,
        })
    }
}

impl From<PhysicalPlanExecuteRequest> for service::PhysicalPlanExecuteRequest {
    fn from(value: PhysicalPlanExecuteRequest) -> Self {
        Self {
            database_id: value.database_id.into_bytes().into(),
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

impl Display for ResolvedTableReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolvedTableReference::Internal { table_oid } => {
                write!(f, "InternalTableReference({})", table_oid)
            }
            ResolvedTableReference::External {
                database,
                schema,
                name,
            } => write!(f, "{}.{}.{}", database, schema, name),
        }
    }
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
