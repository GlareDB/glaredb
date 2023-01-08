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

    #[error("Unknown prepared statement with name: {0}")]
    UnknownPreparedStatement(String),

    #[error("Unknown portal with name: {0}")]
    UnknownPortal(String),

    #[error("Empty search path, unable to resolve schema")]
    EmptySearchPath,

    #[error("Expected exactly on SQL statement, got: {0:?}")]
    ExpectedExactlyOneStatement(Vec<StatementWithExtensions>),

    #[error(transparent)]
    DataFusion(#[from] datafusion::common::DataFusionError),

    #[error(transparent)]
    ParseError(#[from] datafusion::sql::sqlparser::parser::ParserError),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    PgRepr(#[from] pgrepr::error::PgReprError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    #[error(transparent)]
    VarError(#[from] std::env::VarError),

    #[error(transparent)]
    Catalog(#[from] crate::catalog::errors::CatalogError),

    #[error("internal error: {0}")]
    Internal(String),

    #[error(transparent)]
    DatasourceDebug(#[from] datasource_debug::errors::DebugError),
}

pub type Result<T, E = ExecError> = std::result::Result<T, E>;

#[allow(unused_macros)]
macro_rules! internal {
    ($($arg:tt)*) => {
        crate::errors::ExecError::Internal(std::format!($($arg)*))
    };
}
pub(crate) use internal;
