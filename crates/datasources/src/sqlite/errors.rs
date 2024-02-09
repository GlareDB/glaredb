#[derive(thiserror::Error, Debug)]
pub enum SqliteError {
    #[error("Internal Error: {0}")]
    Internal(String),

    #[error(transparent)]
    RusqliteError(#[from] async_sqlite::rusqlite::Error),

    #[allow(clippy::enum_variant_names)]
    #[error(transparent)]
    AsyncSqliteError(#[from] async_sqlite::Error),

    #[error("Send error: {0}")]
    MpscSendError(String),

    #[error(transparent)]
    FmtError(#[from] std::fmt::Error),

    #[error(transparent)]
    DatasourceCommonError(#[from] crate::common::errors::DatasourceCommonError),

    #[error("Missing data for column {0}")]
    MissingDataForColumn(usize),

    #[error("Cannot convert {from:?} to {to}")]
    InvalidConversion {
        from: async_sqlite::rusqlite::types::Value,
        to: datafusion::arrow::datatypes::DataType,
    },

    #[error(transparent)]
    ArrowError(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    ReprError(#[from] repr::error::ReprError),
}

pub type Result<T, E = SqliteError> = std::result::Result<T, E>;
