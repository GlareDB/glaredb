use datafusion::{arrow::datatypes::DataType, scalar::ScalarValue};

#[derive(Debug, thiserror::Error)]
pub enum BuiltinError {
    #[error("Invalid number of arguments.")]
    InvalidNumArgs,

    #[error("Unexpected argument for function. Got '{scalar}', need value of type '{expected}'")]
    UnexpectedArg {
        scalar: ScalarValue,
        expected: DataType,
    },

    #[error("Unable to find {obj_typ}: '{name}'")]
    MissingObject { obj_typ: &'static str, name: String },

    #[error(transparent)]
    Access(Box<dyn std::error::Error + Send + Sync>),

    #[error("Unimplemented: {0}")]
    Unimplemented(&'static str),
}

pub type Result<T, E = BuiltinError> = std::result::Result<T, E>;
