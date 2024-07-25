use datafusion::error::DataFusionError;
use datafusion_ext::errors::ExtensionError;

#[derive(Debug, thiserror::Error)]
pub enum JsonError {
    #[error("Unsupported json type: {0}")]
    UnspportedType(&'static str),

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    #[error("no objects found at path: {0}")]
    NotFound(String),

    #[error("sending data already in progress")]
    SendAlreadyInProgress,

    #[error("missing filter expression")]
    MissingFilterExpression,

    #[error(transparent)]
    ObjectStoreSource(#[from] crate::object_store::errors::ObjectStoreSourceError),

    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    #[error(transparent)]
    ObjectStorePath(#[from] object_store::path::Error),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    Datafusion(#[from] datafusion::error::DataFusionError),

    #[error(transparent)]
    ChannelSend(#[from] futures::channel::mpsc::SendError),

    #[error(transparent)]
    ChannelRecv(#[from] futures::channel::mpsc::TryRecvError),

    #[error("jaq: {0}")]
    Jaq(#[from] crate::json::jaq::JaqError),

    #[error("jaq: interpeter: {0}")]
    JaqInterpret(String),
}

impl From<JsonError> for ExtensionError {
    fn from(e: JsonError) -> Self {
        ExtensionError::String(e.to_string())
    }
}

impl From<JsonError> for DataFusionError {
    fn from(e: JsonError) -> Self {
        DataFusionError::External(Box::new(e))
    }
}


impl From<jaq_interpret::Error> for JsonError {
    fn from(e: jaq_interpret::Error) -> Self {
        JsonError::JaqInterpret(e.to_string())
    }
}

pub type Result<T, E = JsonError> = std::result::Result<T, E>;
