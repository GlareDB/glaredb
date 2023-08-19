//! Built-in functions.
use crate::context::local::SessionContext;
use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{
    ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
    TypeSignature, Volatility,
};
use sqlbuiltins::builtins::POSTGRES_SCHEMA;
use std::sync::Arc;
use tracing::warn;

/// Additional built-in scalar functions.
#[derive(Debug, Copy, Clone)]
pub enum BuiltinScalarFunction {
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
            "connection_id" => BuiltinScalarFunction::ConnectionId,

            // Postgres system functions.
            "pg_catalog.current_schemas" => BuiltinScalarFunction::CurrentSchemas,

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
            BuiltinScalarFunction::ConnectionId => "connection_id",
            BuiltinScalarFunction::CurrentSchemas => "current_schemas",
        }
    }

    /// Get the signature for a function.
    fn signature(&self) -> Signature {
        match self {
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
            BuiltinScalarFunction::CurrentSchemas => Arc::new(|_| {
                Ok(Arc::new(DataType::List(Arc::new(Field::new(
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
            BuiltinScalarFunction::CurrentSchemas => {
                let schemas: Vec<_> = sess
                    .get_session_vars()
                    .search_path()
                    .into_iter()
                    .map(|path| ScalarValue::Utf8(Some(path)))
                    .collect();
                Arc::new(move |_| {
                    // TODO: Actually look at argument.
                    //
                    // When 'true', we'll want to include implicit schemas as
                    // well (namely `pg_catalog`).

                    let schemas = schemas.clone();
                    Ok(ColumnarValue::Scalar(ScalarValue::List(
                        Some(schemas),
                        Arc::new(Field::new("", DataType::Utf8, false)),
                    )))
                })
            }
            BuiltinScalarFunction::ConnectionId => {
                let id = sess.get_session_vars().connection_id();
                Arc::new(move |_| {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                        id.to_string(),
                    ))))
                })
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct PgFunctionBuilder;

impl PgFunctionBuilder {
    /// Try to get a postres function by name.
    ///
    /// If `implicit_pg_schema` is true, try to resolve the function as if the
    /// postgres schema is in the search path.
    pub fn try_from_name(
        ctx: &SessionContext,
        name: &str,
        implicit_pg_schema: bool,
    ) -> Option<Arc<ScalarUDF>> {
        if implicit_pg_schema {
            if let Some(func) = Self::try_from_unqualified(ctx, name) {
                return Some(func);
            }
        }

        let idents: Vec<_> = name.split('.').collect();
        if idents.len() == 1 {
            // No qualification.
            return None;
        }
        if idents.len() != 2 {
            warn!(
                ?idents,
                "received pg function name with more than two idents"
            );
            return None;
        }
        if idents[0] != POSTGRES_SCHEMA {
            return None;
        }
        Self::try_from_unqualified(ctx, idents[1])
    }

    fn try_from_unqualified(ctx: &SessionContext, name: &str) -> Option<Arc<ScalarUDF>> {
        let func = match name {
            "array_to_string" => pg_array_to_string(),
            "current_database" | "current_catalog" => {
                let db_name = ctx.get_session_vars().database_name();
                pg_current_database(&db_name)
            }
            "current_schema" => pg_current_schema(ctx.search_paths().get(0).map(|s| s.as_str())),
            "current_user" | "current_role" | "user" => {
                let user = ctx.get_session_vars().user_name();
                pg_current_user(&user)
            }
            "has_database_privilege" => pg_has_database_privilege(),
            "has_schema_privilege" => pg_has_schema_privilege(),
            "has_table_privilege" => pg_has_table_privilege(),
            "pg_encoding_to_char" => pg_encoding_to_char(),
            "pg_get_userbyid" => pg_get_userbyid(),
            "pg_table_is_visible" => pg_table_is_visible(),
            "version" => {
                let version = ctx.get_session_vars().glaredb_version();
                pg_version(&version)
            }
            _ => return None,
        };

        Some(Arc::new(func))
    }
}

fn pg_get_userbyid() -> ScalarUDF {
    ScalarUDF {
        name: "pg_get_userbyid".to_string(),
        signature: Signature::new(
            TypeSignature::Exact(vec![DataType::Int64]),
            Volatility::Immutable,
        ),
        return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
        fun: Arc::new(move |_| {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "unknown".to_string(),
            ))))
        }),
    }
}

fn pg_table_is_visible() -> ScalarUDF {
    ScalarUDF {
        name: "pg_table_is_visible".to_string(),
        signature: Signature::new(
            TypeSignature::Exact(vec![DataType::Int64]),
            Volatility::Immutable,
        ),
        return_type: Arc::new(|_| Ok(Arc::new(DataType::Boolean))),
        fun: Arc::new(move |input| {
            let is_visible = match get_nth_scalar_value(input, 0) {
                Some(ScalarValue::Int64(Some(_))) => Some(true),
                _ => None,
            };

            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(is_visible)))
        }),
    }
}

fn pg_encoding_to_char() -> ScalarUDF {
    ScalarUDF {
        name: "pg_encoding_to_char".to_string(),
        signature: Signature::new(
            TypeSignature::Exact(vec![DataType::Int64]),
            Volatility::Immutable,
        ),
        return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
        fun: Arc::new(move |input| {
            let enc = match get_nth_scalar_value(input, 0) {
                Some(ScalarValue::Int64(Some(6))) => Some("UTF8".to_string()),
                Some(ScalarValue::Int64(Some(_))) => Some("".to_string()),
                _ => None,
            };

            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(enc)))
        }),
    }
}

fn pg_array_to_string() -> ScalarUDF {
    ScalarUDF {
        name: "array_to_string".to_string(),
        signature: Signature::new(
            TypeSignature::Exact(vec![
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                DataType::Utf8,
            ]),
            Volatility::Immutable,
        ),
        return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
        fun: Arc::new(move |_input| {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "".to_string(),
            ))))
        }),
    }
}

fn pg_has_schema_privilege() -> ScalarUDF {
    ScalarUDF {
        name: "has_schema_privilege".to_string(),
        signature: Signature::new(
            TypeSignature::OneOf(vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ]),
            Volatility::Immutable,
        ),
        return_type: Arc::new(|_| Ok(Arc::new(DataType::Boolean))),
        fun: Arc::new(move |_input| Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))),
    }
}

fn pg_has_database_privilege() -> ScalarUDF {
    ScalarUDF {
        name: "has_database_privilege".to_string(),
        signature: Signature::new(
            TypeSignature::OneOf(vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ]),
            Volatility::Immutable,
        ),
        return_type: Arc::new(|_| Ok(Arc::new(DataType::Boolean))),
        fun: Arc::new(move |_input| Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))),
    }
}

fn pg_has_table_privilege() -> ScalarUDF {
    ScalarUDF {
        name: "has_table_privilege".to_string(),
        signature: Signature::new(
            TypeSignature::OneOf(vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ]),
            Volatility::Immutable,
        ),
        return_type: Arc::new(|_| Ok(Arc::new(DataType::Boolean))),
        fun: Arc::new(move |_input| Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))),
    }
}

fn pg_current_user(user: &str) -> ScalarUDF {
    let mut builder = StringBuilder::new();
    builder.append_value(user);
    let arr = Arc::new(builder.finish());
    ScalarUDF {
        name: "current_user".to_string(),
        signature: Signature::new(TypeSignature::Exact(Vec::new()), Volatility::Immutable),
        return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
        fun: Arc::new(move |_input| Ok(ColumnarValue::Array(arr.clone()))),
    }
}

fn pg_current_database(database: &str) -> ScalarUDF {
    let mut builder = StringBuilder::new();
    builder.append_value(database);
    let arr = Arc::new(builder.finish());
    ScalarUDF {
        name: "current_database".to_string(),
        signature: Signature::new(TypeSignature::Exact(Vec::new()), Volatility::Immutable),
        return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
        fun: Arc::new(move |_input| Ok(ColumnarValue::Array(arr.clone()))),
    }
}

fn pg_current_schema(schema: Option<&str>) -> ScalarUDF {
    let mut builder = StringBuilder::new();
    builder.append_option(schema);
    let arr = Arc::new(builder.finish());
    ScalarUDF {
        name: "current_schema".to_string(),
        signature: Signature::new(TypeSignature::Exact(Vec::new()), Volatility::Immutable),
        return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
        fun: Arc::new(move |_input| Ok(ColumnarValue::Array(arr.clone()))),
    }
}

fn pg_version(version: &str) -> ScalarUDF {
    let mut builder = StringBuilder::new();
    builder.append_value(version);
    let arr = Arc::new(builder.finish());
    ScalarUDF {
        name: "version".to_string(),
        signature: Signature::new(TypeSignature::Exact(Vec::new()), Volatility::Immutable),
        return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
        fun: Arc::new(move |_input| Ok(ColumnarValue::Array(arr.clone()))),
    }
}

fn get_nth_scalar_value(input: &[ColumnarValue], n: usize) -> Option<ScalarValue> {
    match input.get(n) {
        Some(input) => match input {
            ColumnarValue::Scalar(scalar) => Some(scalar.clone()),
            ColumnarValue::Array(arr) => ScalarValue::try_from_array(arr, 0).ok(),
        },
        None => None,
    }
}
