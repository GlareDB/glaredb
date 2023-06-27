#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    #[error("SQL statement currently unsupported: {0}")]
    UnsupportedSQLStatement(String),

    #[error("Unsupported feature: '{0}'. Check back soon!")]
    UnsupportedFeature(&'static str),

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
        got: metastoreproto::types::catalog::EntryType,
        want: metastoreproto::types::catalog::EntryType,
    },

    #[error("Missing connection by name; schema: {schema}, name: {name}")]
    MissingConnectionByName { schema: String, name: String },

    #[error("Missing connection by oid: {oid}")]
    MissingConnectionByOid { oid: u32 },

    #[error("Invalid connection type; expected: {expected}, got: {got}")]
    InvalidConnectionType {
        expected: &'static str,
        got: &'static str,
    },

    #[error("An ssh connection is not supported datasource for CREATE EXTERNAL TABLE. An ssh connection must be provided as an optional ssh_tunnel with another connection type")]
    ExternalTableWithSsh,

    // TODO: Need to be more granular about errors from Metastore.
    #[error("Failed Metastore request: {message}")]
    MetastoreTonic {
        strategy: metastore::errors::ResolveErrorStrategy,
        message: String,
    },

    #[error("Metastore database worker overloaded; request type: {request_type_tag}, conn_id: {conn_id}")]
    MetastoreDatabaseWorkerOverload {
        request_type_tag: &'static str,
        conn_id: uuid::Uuid,
    },

    #[error(
        "Metastore request channel closed; request type: {request_type_tag}, conn_id: {conn_id}"
    )]
    MetastoreRequestChannelClosed {
        request_type_tag: &'static str,
        conn_id: uuid::Uuid,
    },

    #[error(
        "Metastore response channel closed; request type: {request_type_tag}, conn_id: {conn_id}"
    )]
    MetastoreResponseChannelClosed {
        request_type_tag: &'static str,
        conn_id: uuid::Uuid,
    },

    #[error("Duplicate object name: '{0}' already exists")]
    DuplicateObjectName(String),

    #[error("Missing {typ}: '{name}' not found")]
    MissingObject { typ: &'static str, name: String },

    #[error(transparent)]
    Metastore(#[from] metastore::errors::MetastoreError),

    #[error(transparent)]
    ProtoConvError(#[from] metastoreproto::types::ProtoConvError),

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

    #[error("internal error: {0}")]
    Internal(String),

    #[error(transparent)]
    DatasourceDebug(#[from] datasources::debug::errors::DebugError),

    #[error(transparent)]
    DatasourceNative(#[from] datasources::native::errors::NativeError),

    #[error(transparent)]
    DatasourceCommon(#[from] datasources::common::errors::DatasourceCommonError),

    #[error(transparent)]
    PlanError(#[from] crate::planner::errors::PlanError),
}

impl From<tonic::Status> for ExecError {
    fn from(value: tonic::Status) -> Self {
        let strat = value
            .metadata()
            .get(metastore::errors::RESOLVE_ERROR_STRATEGY_META)
            .map(|val| ResolveErrorStrategy::from_bytes(val.as_ref()))
            .unwrap_or(ResolveErrorStrategy::Unknown);

        Self::MetastoreTonic {
            strategy: strat,
            message: value.message().to_string(),
        }
    }
}

pub type Result<T, E = ExecError> = std::result::Result<T, E>;

#[allow(unused_macros)]
macro_rules! internal {
    ($($arg:tt)*) => {
        crate::errors::ExecError::Internal(std::format!($($arg)*))
    };
}
pub(crate) use internal;
use metastore::errors::ResolveErrorStrategy;
