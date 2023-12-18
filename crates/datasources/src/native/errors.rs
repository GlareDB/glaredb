#[derive(Debug, thiserror::Error)]
pub enum NativeError {
    #[error(transparent)]
    DeltaTable(#[from] deltalake::DeltaTableError),

    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    #[error(transparent)]
    ObjectStorePath(#[from] object_store::path::Error),

    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    UrlParse(#[from] url::ParseError),

    #[error("Table entry not a native table: {0}")]
    NotNative(protogen::metastore::types::catalog::TableEntry),

    #[error("{0}")]
    Static(&'static str),
}

pub type Result<T, E = NativeError> = std::result::Result<T, E>;
