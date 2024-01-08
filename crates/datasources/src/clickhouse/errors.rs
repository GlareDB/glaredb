#[derive(Debug, thiserror::Error)]
pub enum ClickhouseError {
    #[error(transparent)]
    Clickhouse(#[from] clickhouse_rs::errors::Error),
    #[error(transparent)]
    UrlParse(#[from] url::ParseError),
    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
    #[error(transparent)]
    DatasourceCommon(#[from] crate::common::errors::DatasourceCommonError),
    #[error(transparent)]
    Fmt(#[from] std::fmt::Error),
    #[error("{0}")]
    String(String),
}

pub type Result<T, E = ClickhouseError> = std::result::Result<T, E>;
