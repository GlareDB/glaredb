use crate::parser::StatementWithExtensions;

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

    #[error("Invalid view statement: {msg}")]
    InvalidViewStatement { msg: &'static str },

    #[error("Connection validation failed: {source}")]
    InvalidConnection {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("External table validation failed: {source}")]
    InvalidExternalTable {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Unknown prepared statement with name: {0}")]
    UnknownPreparedStatement(String),

    #[error("Unknown portal with name: {0}")]
    UnknownPortal(String),

    #[error("Empty search path, unable to resolve schema")]
    EmptySearchPath,

    #[error("Expected exactly on SQL statement, got: {0:?}")]
    ExpectedExactlyOneStatement(Vec<StatementWithExtensions>),

    #[error("Unexpected entry type; got: {got}, want: {want}")]
    UnexpectedEntryType {
        got: metastore::types::catalog::EntryType,
        want: metastore::types::catalog::EntryType,
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
    #[error("Failed Metastore request: {0}")]
    MetastoreTonic(#[from] tonic::Status),

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

    #[error(transparent)]
    Dispatch(#[from] crate::dispatch::DispatchError),

    #[error(transparent)]
    Metastore(#[from] metastore::errors::MetastoreError),

    #[error(transparent)]
    ProtoConvError(#[from] metastore::types::ProtoConvError),

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

    #[error("internal error: {0}")]
    Internal(String),

    #[error(transparent)]
    DatasourceDebug(#[from] datasource_debug::errors::DebugError),

    #[error(transparent)]
    DatasourceCommon(#[from] datasource_common::errors::Error),
}

pub type Result<T, E = ExecError> = std::result::Result<T, E>;

#[allow(unused_macros)]
macro_rules! internal {
    ($($arg:tt)*) => {
        crate::errors::ExecError::Internal(std::format!($($arg)*))
    };
}
pub(crate) use internal;
