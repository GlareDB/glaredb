#[derive(Debug, thiserror::Error)]
pub enum ClickhouseError {
    #[error(transparent)]
    Clickhouse(#[from] clickhouse_rs::errors::Error),
    #[error(transparent)]
    UrlParse(#[from] url::ParseError),
}

pub type Result<T, E = ClickhouseError> = std::result::Result<T, E>;
