use datafusion::error::DataFusionError;

#[derive(Debug, thiserror::Error)]
pub enum VarError {
    #[error("Invalid value for session variable: Variable name: {name}, Value: {val}")]
    InvalidSessionVarValue { name: String, val: String },

    #[error("Variable is readonly: {0}")]
    VariableReadonly(String),

    #[error("Unknown variable: {0}")]
    UnknownVariable(String),

    #[error("Empty search path, unable to resolve schema")]
    EmptySearchPath,
}

impl From<VarError> for DataFusionError {
    fn from(e: VarError) -> Self {
        DataFusionError::Execution(e.to_string())
    }
}
