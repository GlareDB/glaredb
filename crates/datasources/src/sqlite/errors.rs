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
    Fmt(#[from] std::fmt::Error),

    #[error(transparent)]
    DatasourceCommon(#[from] crate::common::errors::DatasourceCommonError),

    #[error("Unimplemented: {0}")]
    Unimplemented(&'static str),

    #[error("Missing data for column {0}")]
    MissingDataForColumn(usize),

    #[error("Cannot convert field {field} value {from:?} to {to} [{cause:?}]")]
    InvalidConversion {
        field: String,
        from: async_sqlite::rusqlite::types::Value,
        to: datafusion::arrow::datatypes::DataType,
        cause: Option<String>,
    },

    #[error("found {num} objects matching specification '{url}'")]
    NoMatchingObjectFound {
        url: crate::common::url::DatasourceUrl,
        num: usize,
    },

    #[error(transparent)]
    ArrowError(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    ReprError(#[from] repr::error::ReprError),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    ObjectStoreSource(#[from] crate::object_store::errors::ObjectStoreSourceError),

    #[error(transparent)]
    ObjectStoreError(#[from] object_store::Error),

    #[error(transparent)]
    ObjectStorePath(#[from] object_store::path::Error),

    #[error(transparent)]
    LakeStorageOptions(#[from] crate::lake::LakeStorageOptionsError),

    #[error(transparent)]
    ExtensionError(#[from] datafusion_ext::errors::ExtensionError),
}

pub type Result<T, E = SqliteError> = std::result::Result<T, E>;

impl From<SqliteError> for datafusion_ext::errors::ExtensionError {
    fn from(value: SqliteError) -> Self {
        datafusion_ext::errors::ExtensionError::access(value)
    }
}
