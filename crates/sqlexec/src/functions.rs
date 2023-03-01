//! Built-in functions.
use crate::context::SessionContext;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{
    ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
    TypeSignature, Volatility,
};
use std::sync::Arc;

/// Whether or not we make the `pg_catalog` schema implicit in regards to
/// resolving builtin functions.
const ENABLE_IMPLICIT_PG_CATALOG: bool = true;

/// Additional built-in scalar functions.
#[derive(Debug, Copy, Clone)]
pub enum BuiltinScalarFunction {
    /// 'version' -> String
    /// Get the version of this db instance.
    Version,

    /// 'connection_id' -> String
    /// Get the connection id that this session was started with.
    ConnectionId,

    /// current_schemas (include_implicit boolean) -> String[]
    /// current_schemas () -> String[]
    ///
    /// (Postgres)
    /// Get a list of schemas in the current search path.
    CurrentSchemas,
}

impl BuiltinScalarFunction {
    /// Try to get the built-in scalar function from the name.
    pub fn try_from_name(name: &str) -> Option<BuiltinScalarFunction> {
        // TODO: We can probably move to some fancier function resolution in the
        // future.
        Some(match name {
            "version" => BuiltinScalarFunction::Version,
            "connection_id" => BuiltinScalarFunction::ConnectionId,

            // Postgres system functions.
            "pg_catalog.current_schemas" => BuiltinScalarFunction::CurrentSchemas,

            _ if ENABLE_IMPLICIT_PG_CATALOG => {
                return Self::try_from_name_implicit_pg_catalog(name)
            }
            _ => return None,
        })
    }

    fn try_from_name_implicit_pg_catalog(name: &str) -> Option<BuiltinScalarFunction> {
        Some(match name {
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
            BuiltinScalarFunction::ConnectionId => "connection_id",
            BuiltinScalarFunction::CurrentSchemas => "current_schemas",
        }
    }

    /// Get the signature for a function.
    fn signature(&self) -> Signature {
        match self {
            BuiltinScalarFunction::Version => {
                Signature::new(TypeSignature::Exact(Vec::new()), Volatility::Immutable)
            }
            BuiltinScalarFunction::ConnectionId => {
                Signature::new(TypeSignature::Exact(Vec::new()), Volatility::Immutable)
            }
            BuiltinScalarFunction::CurrentSchemas => Signature::new(
                TypeSignature::OneOf(vec![
                    TypeSignature::Any(0),
                    TypeSignature::Exact(vec![DataType::Boolean]), // TODO: This isn't exact? I can supply more than one arg.
                ]),
                Volatility::Stable,
            ),
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
            BuiltinScalarFunction::ConnectionId => Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
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
                    .get_session_vars()
                    .search_path
                    .value()
                    .iter()
                    .map(|path| ScalarValue::Utf8(Some(path.to_string())))
                    .collect();
                Arc::new(move |_| {
                    // TODO: Actually look at argument.
                    //
                    // When 'true', we'll want to include implicit schemas as
                    // well (namely `pg_catalog`).

                    let schemas = schemas.clone();
                    Ok(ColumnarValue::Scalar(ScalarValue::List(
                        Some(schemas),
                        Box::new(Field::new("", DataType::Utf8, false)),
                    )))
                })
            }
            BuiltinScalarFunction::ConnectionId => {
                let id = sess.get_info().conn_id;
                Arc::new(move |_| {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                        id.to_string(),
                    ))))
                })
            }
        }
    }
}
