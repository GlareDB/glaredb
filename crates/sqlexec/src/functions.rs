//! Built-in functions.
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Signature, TypeSignature, Volatility};
use datafusion::prelude::Expr;
use sqlbuiltins::builtins::POSTGRES_SCHEMA;
use std::str::FromStr;
use std::sync::Arc;
use tracing::warn;

/// Additional built-in scalar functions.
#[derive(Debug, Copy, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub enum BuiltinScalarFunction {
    /// SQL function `connection_id`
    ///
    /// `connection_id()` -> `String`
    /// ```sql
    /// select connection_id();
    /// ```
    ConnectionId,
    /// SQL function `version`
    ///
    /// `version()` -> `String`
    /// ```sql
    /// select version();
    /// ```
    Version,
    /// SQL function `current_user`
    ///
    /// `current_user()` -> `String`
    /// ```sql
    /// select current_user();
    /// ```
    CurrentUser,
    /// SQL function `current_role`
    ///
    /// `current_role()` -> `String`
    /// ```sql
    /// select current_role();
    /// ```
    CurrentRole,
    /// SQL function `user`
    ///
    /// `user()` -> `String`
    /// ```sql
    /// select user();
    /// ```
    User,
    /// SQL function `current_schema`
    ///
    /// `current_schema()` -> `String`
    /// ```sql
    /// select current_schema();
    /// ```
    CurrentSchema,
    /// SQL function `current_database`
    ///
    /// `current_database()` -> `String`
    /// ```sql
    /// select current_database();
    /// ```
    CurrentDatabase,
    /// SQL function `current_catalog`
    ///
    /// `current_catalog()` -> `String`
    /// ```sql
    /// select current_catalog();
    /// ```
    CurrentCatalog,
    /// SQL function `current_schemas`
    ///
    /// `current_schemas()` -> `String[]`
    ///  current_schemas (include_implicit boolean) -> String[]
    ///
    /// (Postgres)
    /// Get a list of schemas in the current search path.
    CurrentSchemas,
    Pg(BuiltinPostgresFunctions),
}

#[derive(Debug, Copy, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub enum BuiltinPostgresFunctions {
    GetUserById,
    TableIsVisible,
    EncodingToChar,
    ArrayToString,
    HasSchemaPrivilege,
    HasDatabasePrivilege,
    HasTablePrivilege,
}

impl BuiltinPostgresFunctions {
    fn into_expr(self, args: Vec<Expr>) -> Expr {
        let udf = match self {
            Self::GetUserById => pg_get_userbyid(),
            Self::TableIsVisible => pg_table_is_visible(),
            Self::EncodingToChar => pg_encoding_to_char(),
            Self::ArrayToString => pg_array_to_string(),
            Self::HasSchemaPrivilege => pg_has_schema_privilege(),
            Self::HasDatabasePrivilege => pg_has_database_privilege(),
            Self::HasTablePrivilege => pg_has_table_privilege(),
        };
        Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
            udf.into(),
            args,
        ))
    }
}

impl BuiltinScalarFunction {
    pub fn find_function(name: &str) -> Option<Self> {
        Self::from_str(name).ok()
    }
    pub fn into_expr(self, args: Vec<Expr>) -> Expr {
        use BuiltinScalarFunction::*;

        fn string_var(s: &str) -> Expr {
            Expr::ScalarVariable(DataType::Utf8, vec![s.to_string()])
        }

        fn list_var(s: &str) -> Expr {
            Expr::ScalarVariable(
                DataType::List(Arc::new(Field::new(s, DataType::Utf8, true))),
                vec![s.to_string()],
            )
        }

        match self {
            ConnectionId => string_var("connection_id"),
            Version => string_var("version"),
            CurrentSchemas => list_var("current_schemas"),
            CurrentUser => string_var("current_user"),
            CurrentRole => string_var("current_role"),
            CurrentCatalog => string_var("current_catalog"),
            User => string_var("user"),
            CurrentSchema => string_var("current_schema"),
            CurrentDatabase => string_var("current_database"),
            Pg(pg) => pg.into_expr(args),
        }
    }
}

impl From<BuiltinPostgresFunctions> for BuiltinScalarFunction {
    fn from(f: BuiltinPostgresFunctions) -> Self {
        Self::Pg(f)
    }
}

impl FromStr for BuiltinPostgresFunctions {
    type Err = datafusion::common::DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pg_get_userbyid" => Ok(Self::GetUserById),
            "pg_table_is_visible" => Ok(Self::TableIsVisible),
            "pg_encoding_to_char" => Ok(Self::EncodingToChar),
            "array_to_string" => Ok(Self::ArrayToString),
            "has_schema_privilege" => Ok(Self::HasSchemaPrivilege),
            "has_database_privilege" => Ok(Self::HasDatabasePrivilege),
            "has_table_privilege" => Ok(Self::HasTablePrivilege),
            s => {
                let idents: Vec<_> = s.split('.').collect();
                if idents.len() != 2 {
                    warn!(
                        ?idents,
                        "received pg function name with more than two idents"
                    );
                    return Err(datafusion::common::DataFusionError::NotImplemented(
                        format!("BuiltinScalarFunction::from_str({})", s),
                    ));
                }
                if idents[0] != POSTGRES_SCHEMA {
                    return Err(datafusion::common::DataFusionError::NotImplemented(
                        format!("BuiltinScalarFunction::from_str({})", s),
                    ));
                }
                Self::from_str(idents[1])
            }
        }
    }
}

impl FromStr for BuiltinScalarFunction {
    type Err = datafusion::common::DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "connection_id" => Ok(Self::ConnectionId),
            "current_schemas" => Ok(Self::CurrentSchemas),
            "version" => Ok(Self::Version),
            "current_user" => Ok(Self::CurrentUser),
            "current_role" => Ok(Self::CurrentRole),
            "current_catalog" => Ok(Self::CurrentCatalog),
            "user" => Ok(Self::User),
            "current_schema" => Ok(Self::CurrentSchema),
            "current_database" => Ok(Self::CurrentDatabase),

            s => BuiltinPostgresFunctions::from_str(s).map(Self::Pg),
        }
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
#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_funcs_from_str() {
        use BuiltinPostgresFunctions::*;
        use BuiltinScalarFunction::*;

        let pairs = vec![
            ("connection_id", ConnectionId),
            ("current_schemas", CurrentSchemas),
            ("current_catalog", CurrentCatalog),
            ("pg_get_userbyid", GetUserById.into()),
            ("pg_table_is_visible", TableIsVisible.into()),
            ("pg_encoding_to_char", EncodingToChar.into()),
            ("array_to_string", ArrayToString.into()),
            ("has_schema_privilege", HasSchemaPrivilege.into()),
            ("has_database_privilege", HasDatabasePrivilege.into()),
            ("has_table_privilege", HasTablePrivilege.into()),
            ("pg_catalog.pg_get_userbyid", GetUserById.into()),
            ("pg_catalog.pg_table_is_visible", TableIsVisible.into()),
            ("pg_catalog.pg_encoding_to_char", EncodingToChar.into()),
            ("pg_catalog.array_to_string", ArrayToString.into()),
            ("pg_catalog.has_schema_privilege", HasSchemaPrivilege.into()),
            (
                "pg_catalog.has_database_privilege",
                HasDatabasePrivilege.into(),
            ),
            ("pg_catalog.has_table_privilege", HasTablePrivilege.into()),
        ];
        for (s, expected) in pairs {
            let func = BuiltinScalarFunction::from_str(s).unwrap();
            assert_eq!(func, expected);
        }

        let failures = vec![
            "pg_get_userby",
            "pg_get_userbyid.foo",
            "pg_catalo.pg_get_userbyid.",
            "test.pg_catalog.pg_get_userbyid",
        ];

        for s in failures {
            let func = BuiltinScalarFunction::from_str(s);
            assert!(func.is_err());
        }
    }
}
