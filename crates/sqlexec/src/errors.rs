#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    #[error("internal error: {0}")]
    Internal(String),

    #[error(transparent)]
    Access(#[from] access::errors::AccessError),

    #[error(transparent)]
    DataFusion(#[from] datafusion::common::DataFusionError),

    #[error(transparent)]
    ParseError(#[from] datafusion::sql::sqlparser::parser::ParserError),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    PgRepr(#[from] pgrepr::error::Error),
}

pub type Result<T, E = ExecError> = std::result::Result<T, E>;

#[allow(unused_macros)]
macro_rules! internal {
    ($($arg:tt)*) => {
        crate::errors::ExecError::Internal(std::format!($($arg)*))
    };
}
pub(crate) use internal;
