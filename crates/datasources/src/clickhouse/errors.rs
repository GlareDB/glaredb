#[derive(Debug, thiserror::Error)]
pub enum ClickhouseError {
    #[error(transparent)]
    Clickhouse(#[from] clickhouse_rs::errors::Error),
    #[error(transparent)]
    Klickhouse(#[from] klickhouse::KlickhouseError),
    #[error(transparent)]
    UrlParse(#[from] url::ParseError),
    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
    #[error(transparent)]
    DatasourceCommon(#[from] crate::common::errors::DatasourceCommonError),
    #[error(transparent)]
    Fmt(#[from] std::fmt::Error),
    #[error(transparent)]
    FromUtf8(#[from] std::string::FromUtf8Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    String(String),
}

pub type Result<T, E = ClickhouseError> = std::result::Result<T, E>;
