#[derive(Debug, thiserror::Error)]
pub enum SnowflakeError {
    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),

    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),

    #[error(transparent)]
    ArrowError(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    Base64DecodeError(#[from] base64::DecodeError),

    #[error("Invalid URL: {0}")]
    UrlParseError(Box<dyn std::error::Error>),

    #[error("Request errored with status code: {0}")]
    HttpError(reqwest::StatusCode),

    #[error("Snowflake Query Error ({code}): {message}")]
    QueryError { code: String, message: String },

    #[error("Invalid connection parameters: {0}")]
    InvalidConnectionParameters(String),

    #[error("Invalid snowflake data-type: {0}")]
    InvalidSnowflakeDataType(String),
}

pub type Result<T, E = SnowflakeError> = std::result::Result<T, E>;
