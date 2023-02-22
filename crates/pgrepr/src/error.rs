#[derive(Debug, thiserror::Error)]
pub enum PgReprError {
    #[error("Invalid format code: {0}")]
    InvalidFormatCode(i16),

    #[error("message length '{0}' exceeds the limit of i32 max")]
    MessageTooLarge(usize),

    #[error(transparent)]
    Fmt(#[from] std::fmt::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Utf8Error(#[from] std::str::Utf8Error),

    #[error("Failed to parse: {0}")]
    ParseError(Box<dyn std::error::Error + Sync + Send>),

    #[error("arrow type '{0}' not supported")]
    UnsupportedArrowType(datafusion::arrow::datatypes::DataType),

    #[error(transparent)]
    Datafusion(#[from] datafusion::error::DataFusionError),

    #[error("Internal error: {0}")]
    InternalError(String),
}

pub type Result<T, E = PgReprError> = std::result::Result<T, E>;
