#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    #[error("invalid search path; '{schema}' doesn't exist (input: '{input}')")]
    InvalidSearchPath { input: String, schema: String },

    #[error("SQL statement current unsupported: {0}")]
    UnsupportedSQLStatement(String),

    #[error("invalid key for SET: {0}")]
    InvalidSetKey(String),

    #[error("empty search path, unable to resolve schema")]
    EmptySearchPath,

    #[error(transparent)]
    DataFusion(#[from] datafusion::common::DataFusionError),

    #[error(transparent)]
    ParseError(#[from] datafusion::sql::sqlparser::parser::ParserError),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    PgRepr(#[from] pgrepr::error::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    #[error(transparent)]
    VarError(#[from] std::env::VarError),

    #[error(transparent)]
    Persistence(#[from] persistence::errors::PersistenceError),

    #[error(transparent)]
    JsonCat(#[from] jsoncat::errors::CatalogError),

    #[error(transparent)]
    Catalog(#[from] catalog::errors::CatalogError),

    #[error(transparent)]
    Access(#[from] access::errors::AccessError),

    #[error("internal error: {0}")]
    Internal(String),
}

pub type Result<T, E = ExecError> = std::result::Result<T, E>;

#[allow(unused_macros)]
macro_rules! internal {
    ($($arg:tt)*) => {
        crate::errors::ExecError::Internal(std::format!($($arg)*))
    };
}
pub(crate) use internal;
