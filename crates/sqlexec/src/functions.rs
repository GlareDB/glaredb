//! Built-in functions.
use crate::context::local::LocalSessionContext;
use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::expr_rewriter::normalize_cols;
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
    PgFunction(BuiltinPostgresFunctions),
}

#[derive(Debug, Copy, Clone)]
pub enum BuiltinPostgresFunctions {
    GetUserById,
    TableIsVisible,
    EncodingToChar,
    ArrayToString,
    HasSchemaPrivilege,
    HasDatabasePrivilege,
    HasTablePrivilege,
}

impl BuiltinScalarFunction {}

#[derive(Debug, Clone)]
pub struct PgFunctionBuilder;

impl PgFunctionBuilder {
    /// Try to get a postres function by name.
    ///
    /// If `implicit_pg_schema` is true, try to resolve the function as if the
    /// postgres schema is in the search path.
    pub fn try_from_name(
        ctx: &LocalSessionContext,
        name: &str,
        implicit_pg_schema: bool,
    ) -> Option<Arc<ScalarUDF>> {
        todo!()
        // if implicit_pg_schema {
        //     if let Some(func) = Self::try_from_unqualified(ctx, name) {
        //         return Some(func);
        //     }
        // }

        // let idents: Vec<_> = name.split('.').collect();
        // if idents.len() == 1 {
        //     // No qualification.
        //     return None;
        // }
        // if idents.len() != 2 {
        //     warn!(
        //         ?idents,
        //         "received pg function name with more than two idents"
        //     );
        //     return None;
        // }
        // if idents[0] != POSTGRES_SCHEMA {
        //     return None;
        // }
        // Self::try_from_unqualified(ctx, idents[1])
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

fn get_nth_scalar_value(input: &[ColumnarValue], n: usize) -> Option<ScalarValue> {
    match input.get(n) {
        Some(input) => match input {
            ColumnarValue::Scalar(scalar) => Some(scalar.clone()),
            ColumnarValue::Array(arr) => ScalarValue::try_from_array(arr, 0).ok(),
        },
        None => None,
    }
}

pub fn get_pg_udfs() -> Vec<ScalarUDF> {
    vec![
        pg_array_to_string(),
        pg_has_database_privilege(),
        pg_has_schema_privilege(),
        pg_has_table_privilege(),
        pg_encoding_to_char(),
        pg_get_userbyid(),
        pg_table_is_visible(),
    ]
}
