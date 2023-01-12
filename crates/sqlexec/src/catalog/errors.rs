#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("missing catalog entry; type: {typ}, name: {name}")]
    MissingEntry { typ: &'static str, name: String },

    #[error("duplicate entry for name: {0}")]
    DuplicateEntryForName(String),

    #[error("missing table or view for access method: {method}, schema: {schema}, name: {name}")]
    MissingTableForAccessMethod {
        method: crate::catalog::access::AccessMethod,
        schema: String,
        name: String,
    },

    #[error("failed to do late planning: {0}")]
    LatePlanning(String),

    #[error("unable to handle access method: {0:?}")]
    UnhandleableAccess(crate::catalog::entry::AccessOrConnectionMethod),

    #[error(transparent)]
    DatasourceBigQuery(#[from] datasource_bigquery::errors::BigQueryError),

    #[error(transparent)]
    DatasourcePostgres(#[from] datasource_postgres::errors::PostgresError),

    #[error(transparent)]
    DatasourceObjectStore(#[from] datasource_object_store::errors::ObjectStoreSourceError),

    #[error(transparent)]
    StableStore(#[from] stablestore::errors::StableStorageError),

    #[error(transparent)]
    Datafusion(#[from] datafusion::error::DataFusionError),

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    #[error("internal: {0}")]
    Internal(String),
}

pub type Result<T, E = CatalogError> = std::result::Result<T, E>;

#[allow(unused_macros)]
macro_rules! internal {
    ($($arg:tt)*) => {
        crate::catalog::errors::CatalogError::Internal(std::format!($($arg)*))
    };
}
pub(crate) use internal;
