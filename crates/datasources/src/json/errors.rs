use datafusion_ext::errors::ExtensionError;
use object_store::Error as ObjectStoreError;

use crate::object_store::errors::ObjectStoreSourceError;

#[derive(Debug, thiserror::Error)]
pub enum JsonError {
    #[error("Unsupported json type: {0}")]
    UnspportedType(&'static str),

    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),

    #[error("no objects found {0}")]
    NotFound(String),

    #[error(transparent)]
    ObjectStoreSource(#[from] ObjectStoreSourceError),

    #[error(transparent)]
    ObjectStore(#[from] ObjectStoreError),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    Datafusion(#[from] datafusion::error::DataFusionError),
}

impl From<JsonError> for ExtensionError {
    fn from(e: JsonError) -> Self {
        ExtensionError::String(e.to_string())
    }
}
