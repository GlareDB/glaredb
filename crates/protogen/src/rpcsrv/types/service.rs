use datafusion::{
    arrow::datatypes::Schema, common::OwnedTableReference, error::DataFusionError, prelude::Expr,
    sql::TableReference,
};
use datafusion_proto::bytes::Serializeable;
use prost::Message;
use uuid::Uuid;

use crate::{
    errors::ProtoConvError,
    gen::rpcsrv::service,
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

pub struct CreatePhysicalPlanRequest {
    pub session_id: Uuid,
    pub logical_plan: Vec<u8>,
}

impl TryFrom<service::CreatePhysicalPlanRequest> for CreatePhysicalPlanRequest {
    type Error = ProtoConvError;
    fn try_from(value: service::CreatePhysicalPlanRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            session_id: Uuid::from_slice(&value.session_id)?,
            logical_plan: value.logical_plan,
        })
    }
}

impl From<CreatePhysicalPlanRequest> for service::CreatePhysicalPlanRequest {
    fn from(value: CreatePhysicalPlanRequest) -> Self {
        Self {
            session_id: value.session_id.into_bytes().into(),
            logical_plan: value.logical_plan,
        }
    }
}

pub struct DispatchAccessRequest {
    pub session_id: Uuid,
    pub table_ref: OwnedTableReference,
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

pub struct TableProviderScanRequest {
    pub session_id: Uuid,
    pub provider_id: Uuid,
    pub projection: Option<Vec<usize>>,
    pub filters: Vec<Expr>,
    pub limit: Option<usize>,
}

impl TryFrom<service::TableProviderScanRequest> for TableProviderScanRequest {
    type Error = ProtoConvError;
    fn try_from(value: service::TableProviderScanRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            session_id: Uuid::from_slice(&value.session_id)?,
            provider_id: Uuid::from_slice(&value.provider_id)?,
            projection: if value.projection.is_empty() {
                None
            } else {
                Some(value.projection.into_iter().map(|p| p as usize).collect())
            },
            filters: {
                value
                    .filters
                    .into_iter()
                    .map(|bytes| Expr::from_bytes(&bytes))
                    .collect::<Result<Vec<_>, DataFusionError>>()?
            },
            limit: value.limit.map(|l| l as usize),
        })
    }
}

impl TryFrom<TableProviderScanRequest> for service::TableProviderScanRequest {
    type Error = ProtoConvError;
    fn try_from(value: TableProviderScanRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            session_id: value.session_id.into_bytes().into(),
            provider_id: value.provider_id.into_bytes().into(),
            projection: value
                .projection
                .map(|projection| projection.into_iter().map(|p| p as u64).collect())
                .unwrap_or_default(),
            filters: {
                value
                    .filters
                    .into_iter()
                    .map(|expr| expr.to_bytes().map(|bytes| bytes.to_vec()))
                    .collect::<Result<Vec<_>, DataFusionError>>()?
            },
            limit: value.limit.map(|l| l as u64),
        })
    }
}

pub struct TableProviderInsertIntoRequest {
    pub session_id: Uuid,
    pub provider_id: Uuid,
    pub input_exec_id: Uuid,
}

impl TryFrom<service::TableProviderInsertIntoRequest> for TableProviderInsertIntoRequest {
    type Error = ProtoConvError;
    fn try_from(value: service::TableProviderInsertIntoRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            session_id: Uuid::from_slice(&value.session_id)?,
            provider_id: Uuid::from_slice(&value.provider_id)?,
            input_exec_id: Uuid::from_slice(&value.input_exec_id)?,
        })
    }
}

impl From<TableProviderInsertIntoRequest> for service::TableProviderInsertIntoRequest {
    fn from(value: TableProviderInsertIntoRequest) -> Self {
        Self {
            session_id: value.session_id.into_bytes().into(),
            provider_id: value.provider_id.into_bytes().into(),
            input_exec_id: value.input_exec_id.into_bytes().into(),
        }
    }
}

pub struct PhysicalPlanExecuteRequest {
    pub session_id: Uuid,
    pub exec_id: Uuid,
}

impl TryFrom<service::PhysicalPlanExecuteRequest> for PhysicalPlanExecuteRequest {
    type Error = ProtoConvError;
    fn try_from(value: service::PhysicalPlanExecuteRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            session_id: Uuid::from_slice(&value.session_id)?,
            exec_id: Uuid::from_slice(&value.exec_id)?,
        })
    }
}

impl From<PhysicalPlanExecuteRequest> for service::PhysicalPlanExecuteRequest {
    fn from(value: PhysicalPlanExecuteRequest) -> Self {
        Self {
            session_id: value.session_id.into_bytes().into(),
            exec_id: value.exec_id.into_bytes().into(),
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

pub struct PhysicalPlanResponse {
    pub id: Uuid,
    pub schema: Schema,
}

impl TryFrom<service::PhysicalPlanResponse> for PhysicalPlanResponse {
    type Error = ProtoConvError;
    fn try_from(value: service::PhysicalPlanResponse) -> Result<Self, Self::Error> {
        let schema = datafusion_proto::protobuf::Schema::decode(value.schema.as_slice())?;
        let schema = (&schema).try_into()?;
        Ok(Self {
            id: Uuid::from_slice(&value.id)?,
            schema,
        })
    }
}

impl TryFrom<PhysicalPlanResponse> for service::PhysicalPlanResponse {
    type Error = ProtoConvError;
    fn try_from(value: PhysicalPlanResponse) -> Result<Self, Self::Error> {
        let schema = datafusion_proto::protobuf::Schema::try_from(&value.schema)?;
        Ok(Self {
            id: value.id.into_bytes().into(),
            schema: schema.encode_to_vec(),
        })
    }
}

// Table Reference

impl TryFrom<service::TableReference> for OwnedTableReference {
    type Error = ProtoConvError;
    fn try_from(value: service::TableReference) -> Result<Self, Self::Error> {
        let service::TableReference {
            catalog,
            schema,
            table,
        } = value;
        let table_ref = match (catalog, schema, table) {
            (None, None, table) => OwnedTableReference::Bare {
                table: table.into(),
            },
            (None, Some(schema), table) => OwnedTableReference::Partial {
                table: table.into(),
                schema: schema.into(),
            },
            (Some(catalog), Some(schema), table) => OwnedTableReference::Full {
                table: table.into(),
                schema: schema.into(),
                catalog: catalog.into(),
            },
            (catalog, schema, table) => {
                return Err(ProtoConvError::InvalidTableReference(
                    catalog.unwrap_or_default(),
                    schema.unwrap_or_default(),
                    table,
                ))
            }
        };
        Ok(table_ref)
    }
}

impl<'a> From<TableReference<'a>> for service::TableReference {
    fn from(value: TableReference<'a>) -> Self {
        match value {
            TableReference::Bare { table } => service::TableReference {
                table: table.into_owned(),
                schema: None,
                catalog: None,
            },
            TableReference::Partial { schema, table } => service::TableReference {
                table: table.into_owned(),
                schema: Some(schema.into_owned()),
                catalog: None,
            },
            TableReference::Full {
                catalog,
                schema,
                table,
            } => service::TableReference {
                table: table.into_owned(),
                schema: Some(schema.into_owned()),
                catalog: Some(catalog.into_owned()),
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
