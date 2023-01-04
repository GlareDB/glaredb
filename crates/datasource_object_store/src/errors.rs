#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreSourceError {
    #[error("Unknown fields for table")]
    UnknownFieldsForTable,

    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    #[error(transparent)]
    ObjectStorePath(#[from] object_store::path::Error),

    #[error("Failed to decode json: {0}")]
    SerdeJson(#[from] serde_json::Error),
}

pub type Result<T, E = ObjectStoreSourceError> = std::result::Result<T, E>;
