use deltalake::arrow::datatypes::DataType;

#[derive(Debug, thiserror::Error)]
pub enum SqlServerError {
    #[error("{0}")]
    String(String),

    #[error(transparent)]
    Tiberius(#[from] tiberius::error::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    Fmt(#[from] std::fmt::Error),

    #[error(transparent)]
    DatasourceCommon(#[from] crate::common::errors::DatasourceCommonError),

    #[error("usupported sqlserver data for type: {0}")]
    UnsupportedDataType(DataType),

    #[error(transparent)]
    IntConversion(#[from] std::num::TryFromIntError),
}

pub type Result<T, E = SqlServerError> = std::result::Result<T, E>;
