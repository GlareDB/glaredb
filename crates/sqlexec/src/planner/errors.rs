#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("Unsupported feature: '{0}'. Check back soon!")]
    UnsupportedFeature(&'static str),

    #[error("SQL statement currently unsupported: {0}")]
    UnsupportedSQLStatement(String),

    #[error("Failed to create table provider for '{reference}': {e}")]
    FailedToCreateTableProvider {
        reference: String,
        e: crate::planner::dispatch::DispatchError,
    },

    #[error("Failed to find table for reference: {reference}")]
    FailedToFindTableForReference { reference: String },

    #[error("Failed to dispatch to table: {0}")]
    TableDispatch(#[from] crate::planner::dispatch::DispatchError),

    #[error(transparent)]
    DataFusion(#[from] datafusion::common::DataFusionError),

    #[error(transparent)]
    Preprocess(#[from] crate::planner::preprocess::PreprocessError),

    #[error("Invalid tunnel '{tunnel}': {reason}")]
    InvalidTunnel { tunnel: String, reason: String },

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

    #[error("Invalid number of column aliases for view body; sql: {sql}, aliases: {aliases:?}")]
    InvalidNumberOfAliasesForView { sql: String, aliases: Vec<String> },

    #[error("An ssh connection is not supported datasource for CREATE EXTERNAL TABLE. An ssh connection must be provided as an optional ssh_tunnel with another connection type")]
    ExternalTableWithSsh,

    #[error("Expected exactly on SQL statement, got: {0:?}")]
    ExpectedExactlyOneStatement(Vec<crate::parser::StatementWithExtensions>),

    #[error("Unsupported option value: {0}")]
    UnsupportedOptionValue(crate::parser::OptionValue),

    #[error("Unknown copy format: {0}")]
    UnknownCopyFormat(String),

    #[error("Unable to infer copy format")]
    UnableToInferCopyFormat,

    #[error("Exec error: {0}")]
    Exec(Box<crate::errors::ExecError>), // TODO: Try to remove.

    #[error("internal error: {0}")]
    Internal(String),

    #[error(transparent)]
    DatasourceDebug(#[from] datasource_debug::errors::DebugError),

    #[error(transparent)]
    DatasourceCommon(#[from] datasource_common::errors::DatasourceCommonError),

    #[error(transparent)]
    DatasourceObjectStore(#[from] datasource_object_store::errors::ObjectStoreSourceError),

    #[error(transparent)]
    ParseError(#[from] datafusion::sql::sqlparser::parser::ParserError),

    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T, E = PlanError> = std::result::Result<T, E>;

impl From<crate::errors::ExecError> for PlanError {
    fn from(value: crate::errors::ExecError) -> Self {
        PlanError::Exec(Box::new(value))
    }
}

#[allow(unused_macros)]
macro_rules! internal {
    ($($arg:tt)*) => {
        crate::planner::errors::PlanError::Internal(std::format!($($arg)*))
    };
}
pub(crate) use internal;
