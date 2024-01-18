#[derive(Debug, thiserror::Error)]
pub enum PgReprError {
    #[error("Invalid format code: {0}")]
    InvalidFormatCode(i16),

    #[error(transparent)]
    Fmt(#[from] std::fmt::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Utf8Error(#[from] std::str::Utf8Error),

    #[error(transparent)]
    ReprError(#[from] repr::error::ReprError),

    #[error("Binary read unimplemented.")]
    BinaryReadUnimplemented,

    #[error("Failed to parse: {0}")]
    ParseError(Box<dyn std::error::Error + Sync + Send>),

    #[error("Unsuported pg type for decoding: {0}")]
    UnsupportedPgTypeForDecode(tokio_postgres::types::Type),

    #[error("arrow type '{0}' not supported")]
    UnsupportedArrowType(datafusion::arrow::datatypes::DataType),

    #[error(transparent)]
    Datafusion(#[from] datafusion::error::DataFusionError),

    #[error("Internal error: {0}")]
    InternalError(String),

    #[error("{0}")]
    String(String),
}

pub type Result<T, E = PgReprError> = std::result::Result<T, E>;
