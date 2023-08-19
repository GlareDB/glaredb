#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    #[error("SQL statement currently unsupported: {0}")]
    UnsupportedSQLStatement(String),

    #[error("Unsupported feature: '{0}'. Check back soon!")]
    UnsupportedFeature(&'static str),

    #[error("Invalid remote {0} id: {1}")]
    InvalidRemoteId(&'static str, uuid::Error),

    #[error("Cannot convert proto to {0}: {1}")]
    ProtoConvCustom(&'static str, Box<dyn std::error::Error + Send + Sync>),

    #[error("Invalid value for session variable: Variable name: {name}, Value: {val}")]
    InvalidSessionVarValue { name: String, val: String },

    #[error("Variable is readonly: {0}")]
    VariableReadonly(String),

    #[error("Unknown variable: {0}")]
    UnknownVariable(String),

    #[error("Unknown prepared statement with name: {0}")]
    UnknownPreparedStatement(String),

    #[error("Unknown portal with name: {0}")]
    UnknownPortal(String),

    #[error("Empty search path, unable to resolve schema")]
    EmptySearchPath,

    #[error("Unexpected entry type; got: {got}, want: {want}")]
    UnexpectedEntryType {
        got: protogen::metastore::types::catalog::EntryType,
        want: protogen::metastore::types::catalog::EntryType,
    },

    #[error("Missing connection by name; schema: {schema}, name: {name}")]
    MissingConnectionByName { schema: String, name: String },

    #[error("Missing connection by oid: {oid}")]
    MissingConnectionByOid { oid: u32 },

    #[error("Invalid {0} id: {1} not found")]
    MissingRemoteId(&'static str, uuid::Uuid),

    #[error("Invalid connection type; expected: {expected}, got: {got}")]
    InvalidConnectionType {
        expected: &'static str,
        got: &'static str,
    },

    #[error("An ssh connection is not supported datasource for CREATE EXTERNAL TABLE. An ssh connection must be provided as an optional ssh_tunnel with another connection type")]
    ExternalTableWithSsh,

    #[error("Duplicate object name: '{0}' already exists")]
    DuplicateObjectName(String),

    #[error("Missing {typ}: '{name}' not found")]
    MissingObject { typ: &'static str, name: String },

    #[error(transparent)]
    DataFusion(#[from] datafusion::common::DataFusionError),

    #[error(transparent)]
    ParseError(#[from] datafusion::sql::sqlparser::parser::ParserError),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    PgRepr(#[from] pgrepr::error::PgReprError),

    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    #[error(transparent)]
    VarError(#[from] std::env::VarError),

    #[error(transparent)]
    ProtoConvError(#[from] protogen::errors::ProtoConvError),

    #[error("Unable to retrieve ssh tunnel connection: {0}")]
    MissingSshTunnel(Box<crate::errors::ExecError>),

    #[error("All connection methods as part of ssh_tunnel should be ssh connections")]
    NonSshConnection,

    #[error("Cannot create additional {typ}. Max: {max}, Current: {current}")]
    MaxObjectCount {
        typ: &'static str,
        max: usize,
        current: usize,
    },

    #[error("Invalid storage configuration: {0}")]
    InvalidStorageConfig(&'static str),

    #[error("Failed to read table from environment: {0}")]
    EnvironmentTableRead(Box<dyn std::error::Error + Send + Sync>),

    #[error("Unable to send message over channel: {0}")]
    ChannelSendError(Box<dyn std::error::Error + Send + Sync>),

    #[error("Invalid temporary table: {reason}")]
    InvalidTempTable { reason: String },

    #[error("internal error: {0}")]
    Internal(String),

    #[error("Remote session error: {0}")]
    RemoteSession(String),

    #[error("Invalid URL for remote execution: {0}")]
    InvalidRemoteExecUrl(String),

    #[error(transparent)]
    DatasourceDebug(#[from] datasources::debug::errors::DebugError),

    #[error(transparent)]
    DatasourceNative(#[from] datasources::native::errors::NativeError),

    #[error(transparent)]
    DatasourceCommon(#[from] datasources::common::errors::DatasourceCommonError),

    #[error(transparent)]
    DatasourceObjectStore(#[from] datasources::object_store::errors::ObjectStoreSourceError),

    #[error(transparent)]
    PlanError(#[from] crate::planner::errors::PlanError),

    #[error(transparent)]
    DispatchError(#[from] crate::dispatch::DispatchError),

    #[error(transparent)]
    MetastoreWorker(#[from] crate::metastore::client::MetastoreClientError),

    #[error(transparent)]
    SessionCatalog(#[from] crate::metastore::catalog::SessionCatalogError),

    #[error("{0:?}")]
    TonicTransport(#[from] tonic::transport::Error),

    #[error("{0:?}")]
    TonicStatus(#[from] tonic::Status),

    #[error(transparent)]
    InvalidMetadataValue(#[from] tonic::metadata::errors::InvalidMetadataValue),
}

pub type Result<T, E = ExecError> = std::result::Result<T, E>;

#[allow(unused_macros)]
macro_rules! internal {
    ($($arg:tt)*) => {
        crate::errors::ExecError::Internal(std::format!($($arg)*))
    };
}
pub(crate) use internal;
