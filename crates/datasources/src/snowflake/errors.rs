#[derive(Debug, thiserror::Error)]
pub enum DatasourceSnowflakeError {
    #[error(transparent)]
    SnowflakeConnectorError(#[from] snowflake_connector::errors::SnowflakeError),

    #[error(transparent)]
    FmtError(#[from] std::fmt::Error),

    #[error(transparent)]
    DatasourceCommonError(#[from] crate::common::errors::DatasourceCommonError),
}

pub type Result<T, E = DatasourceSnowflakeError> = std::result::Result<T, E>;
