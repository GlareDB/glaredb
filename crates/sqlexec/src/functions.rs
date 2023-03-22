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

    /// 'connection_id' -> String
    /// Get the connection id that this session was started with.
    ConnectionId,

    /// current_schemas (include_implicit boolean) -> String[]
    /// current_schemas () -> String[]
    ///
    /// (Postgres)
    /// Get a list of schemas in the current search path.
    CurrentSchemas,

    /// pg_get_expr (pg_node_tree, relation_oid, pretty_bool) -> String
    /// pg_get_expr (pg_node_tree, relation_oid) -> String
    ///
    /// (Postgres)
    /// Decompile internal form of an expression, assuming that any Vars in it refer to the
    /// relation indicated by the second parameter
    PgGetExpr,
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
            "pg_catalog.pg_get_expr" => BuiltinScalarFunction::PgGetExpr,

            // Always fall back to trying to bare pg functions. Longer term will
            // want to ensure functions are scoped to schemas and do proper
            // search path resolution.
            _ => return Self::try_from_name_implicit_pg_catalog(name),
        })
    }

    fn try_from_name_implicit_pg_catalog(name: &str) -> Option<BuiltinScalarFunction> {
        match name {
            "current_schemas" => Some(BuiltinScalarFunction::CurrentSchemas),
            _ => None,
        }
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
            BuiltinScalarFunction::PgGetExpr => "pg_get_expr",
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
            BuiltinScalarFunction::PgGetExpr => Signature::new(
                TypeSignature::OneOf(vec![
                    TypeSignature::Any(2), // TODO set types for these inputs
                    TypeSignature::Any(3),
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
            BuiltinScalarFunction::PgGetExpr => Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
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
            // TODO: Currently a dummy function
            BuiltinScalarFunction::PgGetExpr => {
                Arc::new(move |_| Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))))
            }
        }
    }
}
