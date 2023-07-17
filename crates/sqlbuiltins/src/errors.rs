use datafusion::sql::sqlparser::ast::FunctionArg;

#[derive(Debug, thiserror::Error)]
pub enum BuiltinError {
    #[error("Invalid number of arguments.")]
    InvalidNumArgs,

    #[error("Missing named argument: '{0}'")]
    MissingNamedArgument(&'static str),

    #[error("Unexpected argument for function. Got '{param}', need value of type '{expected}'")]
    UnexpectedArg {
        param: crate::functions::FuncParamValue,
        expected: datafusion::arrow::datatypes::DataType,
    },

    #[error(
        "Unexpected argument for function, expected {}, found '{}'",
        expected,
        crate::functions::FuncParamValue::multiple_to_string(params)
    )]
    UnexpectedArgs {
        params: Vec<crate::functions::FuncParamValue>,
        expected: String,
    },
    #[error("Unexpected argument for function. Got '{param}', need value of type '{expected}'")]
    UnexpectedFunctionArg {
        param: FunctionArg,
        expected: datafusion::arrow::datatypes::DataType,
    },

    #[error(
        "Unexpected argument for function, expected {}, found '{:?}'",
        expected,
        params
    )]
    UnexpectedFunctionArgs {
        params: Vec<FunctionArg>,
        expected: String,
    },

    #[error("Unable to find {obj_typ}: '{name}'")]
    MissingObject { obj_typ: &'static str, name: String },

    #[error(transparent)]
    Access(Box<dyn std::error::Error + Send + Sync>),

    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    DatasourceCommonError(#[from] datasources::common::errors::DatasourceCommonError),

    #[error("{0}")]
    Static(&'static str),

    #[error("Unimplemented: {0}")]
    Unimplemented(&'static str),
}

pub type Result<T, E = BuiltinError> = std::result::Result<T, E>;
