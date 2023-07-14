use datafusion::{arrow::datatypes::DataType, scalar::ScalarValue};
use sqlparser::ast::FunctionArg;

#[derive(Debug, thiserror::Error)]
pub enum BuiltinError {
    #[error("Invalid number of arguments.")]
    InvalidNumArgs,

    #[error("Unexpected argument for function. Got '{scalar}', need value of type '{expected}'")]
    UnexpectedArg {
        scalar: FunctionArg,
        expected: DataType,
    },

    #[error("Unexpected argument for function, expected {expected}, found '{scalars:?}'")]
    UnexpectedArgs {
        expected: String,
        scalars: Vec<FunctionArg>,
    },

    #[error("Unable to find {obj_typ}: '{name}'")]
    MissingObject { obj_typ: &'static str, name: String },

    #[error(transparent)]
    Access(Box<dyn std::error::Error + Send + Sync>),

    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error("{0}")]
    Static(&'static str),

    #[error("Unimplemented: {0}")]
    Unimplemented(&'static str),
}

pub type Result<T, E = BuiltinError> = std::result::Result<T, E>;
