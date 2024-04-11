#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("Unsupported feature: '{0}'. Check back soon!")]
    UnsupportedFeature(&'static str),

    #[error("SQL statement currently unsupported: {0}")]
    UnsupportedSQLStatement(String),

    #[error("Failed to create table provider for '{reference}': {e}")]
    FailedToCreateTableProvider {
        reference: String,
        e: crate::dispatch::DispatchError,
    },

    #[error("Failed to find table for reference: {reference}")]
    FailedToFindTableForReference { reference: String },

    #[error(transparent)]
    DataFusion(#[from] datafusion::common::DataFusionError),
    #[error(transparent)]
    Parser(#[from] parser::errors::ParseError),

    #[error(transparent)]
    Preprocess(#[from] crate::planner::preprocess::PreprocessError),

    #[error(transparent)]
    Dispatch(#[from] crate::dispatch::DispatchError),

    #[error("Invalid tunnel '{tunnel}': {reason}")]
    InvalidTunnel { tunnel: String, reason: String },

    #[error("Invalid credentials '{credentials}': {reason}")]
    InvalidCredentials { credentials: String, reason: String },

    #[error("External database validation failed: {source}")]
    InvalidExternalDatabase {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("External table validation failed: {source}")]
    InvalidExternalTable {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Invalid view statement: {msg}")]
    InvalidViewStatement { msg: &'static str },

    #[error("Invalid delete statement: {msg}")]
    InvalidDeleteStatement { msg: &'static str },

    #[error("Invalid insert statement: {msg}")]
    InvalidInsertStatement { msg: &'static str },

    #[error("Invalid alter statement: {msg}")]
    InvalidAlterStatement { msg: &'static str },

    #[error("Invalid copy to statement: {source}")]
    InvalidCopyToStatement {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Invalid number of column aliases for view body; sql: {sql}, aliases: {aliases:?}")]
    InvalidNumberOfAliasesForView { sql: String, aliases: Vec<String> },

    #[error("An ssh connection is not supported datasource for CREATE EXTERNAL TABLE. An ssh connection must be provided as an optional ssh_tunnel with another connection type")]
    ExternalTableWithSsh,

    #[error("Expected exactly on SQL statement, got: {0:?}")]
    ExpectedExactlyOneStatement(Vec<parser::StatementWithExtensions>),

    #[error("Not allowed to write into the object: {0}")]
    ObjectNotAllowedToWriteInto(OwnedTableReference),

    #[error("Exec error: {0}")]
    Exec(Box<crate::errors::ExecError>), // TODO: Try to remove.

    #[error("internal error: {0}")]
    Internal(String),

    #[error(transparent)]
    DatasourceDebug(#[from] datasources::debug::errors::DebugError),

    #[error(transparent)]
    DatasourceCommon(#[from] datasources::common::errors::DatasourceCommonError),

    #[error(transparent)]
    LakeStorageOptions(#[from] datasources::lake::LakeStorageOptionsError),

    #[error(transparent)]
    SshKey(#[from] datasources::common::ssh::key::SshKeyError),

    #[error(transparent)]
    ParseError(#[from] datafusion::sql::sqlparser::parser::ParserError),

    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error(transparent)]
    ResolveError(#[from] crate::resolve::ResolveError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    String(String),

    #[error(transparent)]
    Builtin(#[from] sqlbuiltins::errors::BuiltinError),
}

impl From<PlanError> for datafusion::error::DataFusionError {
    fn from(value: PlanError) -> Self {
        datafusion::error::DataFusionError::Plan(value.to_string())
    }
}

pub type Result<T, E = PlanError> = std::result::Result<T, E>;

impl From<crate::errors::ExecError> for PlanError {
    fn from(value: crate::errors::ExecError) -> Self {
        PlanError::Exec(Box::new(value))
    }
}

macro_rules! impl_from_dispatch_variant {
    ($FromType:ty) => {
        impl From<$FromType> for PlanError {
            #[inline]
            fn from(err: $FromType) -> Self {
                PlanError::Dispatch(crate::dispatch::DispatchError::from(err))
            }
        }
    };
}
impl_from_dispatch_variant!(datasources::lake::delta::errors::DeltaError);
impl_from_dispatch_variant!(datasources::lake::iceberg::errors::IcebergError);
impl_from_dispatch_variant!(datasources::object_store::errors::ObjectStoreSourceError);
impl_from_dispatch_variant!(datasources::sqlserver::errors::SqlServerError);
impl_from_dispatch_variant!(datasources::clickhouse::errors::ClickhouseError);
impl_from_dispatch_variant!(datasources::cassandra::CassandraError);
impl_from_dispatch_variant!(datasources::sqlite::errors::SqliteError);

#[allow(unused_macros)]
macro_rules! internal {
    ($($arg:tt)*) => {
        crate::planner::errors::PlanError::Internal(std::format!($($arg)*))
    };
}
use datafusion::common::OwnedTableReference;
pub(crate) use internal;
