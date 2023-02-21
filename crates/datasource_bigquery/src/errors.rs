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

    #[error("Failed to decode json: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error(transparent)]
    Fmt(#[from] std::fmt::Error),

    // TODO Remove
    #[error("Coming soon! This feature is unimplemented")]
    Unimplemented,
}

pub type Result<T, E = BigQueryError> = std::result::Result<T, E>;
