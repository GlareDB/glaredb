#[derive(Debug, thiserror::Error)]
pub enum ExtensionError {
    #[error("Invalid number of arguments.")]
    InvalidNumArgs,

    #[error("{0}")]
    String(String),

    #[error("Unable to find {obj_typ}: '{name}'")]
    MissingObject { obj_typ: &'static str, name: String },

    #[error("Missing named argument: '{0}'")]
    MissingNamedArgument(&'static str),

    #[error("Invalid parameter value {param}, expected a {expected}")]
    InvalidParamValue {
        param: String,
        expected: &'static str,
    },

    #[error(transparent)]
    Access(Box<dyn std::error::Error + Send + Sync>),

    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error("Unimplemented: {0}")]
    Unimplemented(&'static str),
}

pub type Result<T, E = ExtensionError> = std::result::Result<T, E>;
