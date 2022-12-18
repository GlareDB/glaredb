//! Built-in functions.
use crate::context::SessionContext;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{
    ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
    TypeSignature, Volatility,
};
use std::sync::Arc;

/// Additional built-in scalar functions.
#[derive(Debug, Copy, Clone)]
pub enum BuiltinScalarFunction {
    /// 'version' -> String
    /// Get the version of this db instance.
    Version,
    /// 'current_schemas' -> [String]
    /// Get a list of schemas in the current search path.
    CurrentSchemas,
}

impl BuiltinScalarFunction {
    /// Try to get the built-in scalar function from the name.
    pub fn try_from_name(name: &str) -> Option<BuiltinScalarFunction> {
        Some(match name {
            "version" => BuiltinScalarFunction::Version,
            "current_schemas" => BuiltinScalarFunction::CurrentSchemas,
            _ => return None,
        })
    }

    /// Build the scalar function. The session context is used for functions
    /// that rely on session state.
    pub fn build_scalar_udf(self, sess: &SessionContext) -> ScalarUDF {
        ScalarUDF {
            name: self.name().to_string(),
            signature: self.signature(),
            return_type: self.return_type(),
            fun: self.impl_function(sess),
        }
    }

    /// Get the name of the built-in function.
    fn name(&self) -> &'static str {
        match self {
            BuiltinScalarFunction::Version => "version",
            BuiltinScalarFunction::CurrentSchemas => "current_schemas",
        }
    }

    /// Get the signature for a function.
    fn signature(&self) -> Signature {
        match self {
            BuiltinScalarFunction::Version => {
                Signature::new(TypeSignature::Exact(Vec::new()), Volatility::Immutable)
            }
            BuiltinScalarFunction::CurrentSchemas => {
                Signature::new(TypeSignature::Exact(Vec::new()), Volatility::Stable)
            }
        }
    }

    /// Get the return type for a function.
    fn return_type(&self) -> ReturnTypeFunction {
        match self {
            BuiltinScalarFunction::Version => Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
            BuiltinScalarFunction::CurrentSchemas => Arc::new(|_| {
                Ok(Arc::new(DataType::List(Box::new(Field::new(
                    "",
                    DataType::Utf8,
                    false,
                )))))
            }),
        }
    }

    /// Return the function implementation.
    ///
    /// Accepts a session context for functions that rely on values set inside
    /// the session (e.g. retrieving configuration values).
    fn impl_function(&self, sess: &SessionContext) -> ScalarFunctionImplementation {
        match self {
            BuiltinScalarFunction::Version => Arc::new(|_| {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    buildenv::git_tag().to_string(),
                ))))
            }),
            BuiltinScalarFunction::CurrentSchemas => {
                let schemas: Vec<_> = sess
                    .get_search_path()
                    .iter()
                    .map(|path| ScalarValue::Utf8(Some(path.to_string())))
                    .collect();
                let val = ScalarValue::List(
                    Some(schemas),
                    Box::new(Field::new("", DataType::Utf8, false)),
                );
                Arc::new(move |_| Ok(ColumnarValue::Scalar(val.clone()))) // TODO: Figure out how not to clone here.
            }
        }
    }
}
