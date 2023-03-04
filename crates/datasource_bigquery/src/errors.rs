#[derive(Debug, thiserror::Error)]
pub enum BigQueryError {
    #[error("Unsupported BigQuery type: {0:?}")]
    UnsupportedBigQueryType(gcp_bigquery_client::model::field_type::FieldType),

    #[error("Unknown fields for table")]
    UnknownFieldsForTable,

    #[error(transparent)]
    BigQueryStorage(#[from] bigquery_storage::Error),

    #[error(transparent)]
    BigQueryClient(#[from] gcp_bigquery_client::error::BQError),

    #[error("Failed to use provided service account key: {0}")]
    AuthKey(#[from] std::io::Error),

    #[error("Unknown or no read permissions for project_id {0}")]
    ProjectReadPerm(String),

    #[error("Failed to decode json: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error(transparent)]
    Fmt(#[from] std::fmt::Error),

    #[error(transparent)]
    DatasourceCommon(#[from] datasource_common::errors::Error),
}

pub type Result<T, E = BigQueryError> = std::result::Result<T, E>;
