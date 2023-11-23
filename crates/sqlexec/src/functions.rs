//! Built-in functions.
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Signature, TypeSignature, Volatility};
use datafusion::prelude::Expr;
use kdl::{KdlDocument, KdlNode, KdlQuery};
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
    /// postgres functions
    /// All of these functions are  in the `pg_catalog` schema.
    Pg(BuiltinPostgresFunctions),

    // KdlMatches and KdlSelect (kdl_matches and kdl_select) allow for
    // accessing KDL documents using the KQL (a CSS-inspired selector
    // langauge and an analog to XPath) language. Matches is a
    // predicate and can be used in `WHERE` statements while Select is
    // a projection operator.
    KdlMatches,
    KdlSelect,
}

impl BuiltinScalarFunction {
    pub fn find_function(name: &str) -> Option<Self> {
        Self::from_str(name).ok()
    }
    pub fn into_expr(self, args: Vec<Expr>) -> Expr {
        match self {
            Self::ConnectionId => string_var("connection_id"),
            Self::Version => string_var("version"),
            Self::Pg(pg) => pg.into_expr(args),
            Self::KdlMatches => udf_to_expr(kdl_matches(), args),
            Self::KdlSelect => udf_to_expr(kdl_select(), args),
        }
    }
}

impl FromStr for BuiltinScalarFunction {
    type Err = datafusion::common::DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "connection_id" => Ok(Self::ConnectionId),
            "version" => Ok(Self::Version),
            "kdl_matches" => Ok(Self::KdlMatches),
            "kdl_select" => Ok(Self::KdlSelect),
            s => BuiltinPostgresFunctions::from_str(s).map(Self::Pg),
        }
    }
}

#[derive(Debug, Copy, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub enum BuiltinPostgresFunctions {
    /// SQL function `pg_userbyid`
    ///
    /// `pg_get_userbyid(userid int)` -> `String`
    /// ```sql
    /// select pg_get_userbyid(1);
    /// ```
    GetUserById,
    /// SQL function `pg_table_is_visible`
    ///
    /// `pg_table_is_visible(table_oid int)` -> `Boolean`
    /// ```sql
    /// select pg_table_is_visible(1);
    /// ```
    TableIsVisible,
    /// SQL function `pg_encoding_to_char`
    ///
    /// `pg_encoding_to_char(encoding int)` -> `String`
    /// ```sql
    /// select pg_encoding_to_char(1);
    /// ```
    EncodingToChar,
    /// SQL function `array_to_string`
    ///
    /// `array_to_string(array anyarray, delimiter text [, null_string text])` -> `String`
    /// ```sql
    /// select array_to_string(array[1,2,3], ',');
    /// ```
    ArrayToString,
    /// SQL function `has_schema_privilege`
    ///
    /// `has_schema_privilege(user_name text, schema_name text, privilege text) -> Boolean`
    /// ```sql
    HasSchemaPrivilege,
    /// SQL function `has_database_privilege`
    ///
    /// `has_database_privilege(user_name text, database_name text, privilege text) -> Boolean`
    /// ```sql
    /// select has_database_privilege('foo', 'bar', 'baz');
    /// ```
    HasDatabasePrivilege,
    /// SQL function `has_table_privilege`
    ///
    /// `has_table_privilege(user_name text, table_name text, privilege text) -> Boolean`
    /// ```sql
    /// select has_table_privilege('foo', 'bar', 'baz');
    /// ```
    HasTablePrivilege,
    /// SQL function `current_schemas`
    ///
    /// `current_schemas()` -> `String[]`
    ///  current_schemas (include_implicit boolean) -> String[]
    ///
    /// Get a list of schemas in the current search path.
    CurrentSchemas,
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
}

impl From<BuiltinPostgresFunctions> for BuiltinScalarFunction {
    fn from(f: BuiltinPostgresFunctions) -> Self {
        Self::Pg(f)
    }
}

impl BuiltinPostgresFunctions {
    fn into_expr(self, args: Vec<Expr>) -> Expr {
        match self {
            Self::GetUserById => udf_to_expr(pg_get_userbyid(), args),
            Self::TableIsVisible => udf_to_expr(pg_table_is_visible(), args),
            Self::EncodingToChar => udf_to_expr(pg_encoding_to_char(), args),
            Self::ArrayToString => udf_to_expr(pg_array_to_string(), args),
            Self::HasSchemaPrivilege => udf_to_expr(pg_has_schema_privilege(), args),
            Self::HasDatabasePrivilege => udf_to_expr(pg_has_database_privilege(), args),
            Self::HasTablePrivilege => udf_to_expr(pg_has_table_privilege(), args),
            Self::CurrentUser => string_var("current_user"),
            Self::CurrentRole => string_var("current_role"),
            Self::CurrentCatalog => string_var("current_catalog"),
            Self::User => string_var("user"),
            Self::CurrentSchema => string_var("current_schema"),
            Self::CurrentDatabase => string_var("current_database"),
            Self::CurrentSchemas => {
                // There's no good way to handle the `include_implicit` argument,
                // but since its a binary value (true/false),
                // we can just assign it to a different variable
                let var_name =
                    if let Some(Expr::Literal(ScalarValue::Boolean(Some(true)))) = args.get(0) {
                        "current_schemas_include_implicit".to_string()
                    } else {
                        "current_schemas".to_string()
                    };

                Expr::ScalarVariable(
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    vec![var_name],
                )
                .alias("current_schemas")
            }
        }
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
            "current_schemas" => Ok(Self::CurrentSchemas),
            "current_user" => Ok(Self::CurrentUser),
            "current_role" => Ok(Self::CurrentRole),
            "current_catalog" => Ok(Self::CurrentCatalog),
            "user" => Ok(Self::User),
            "current_schema" => Ok(Self::CurrentSchema),
            "current_database" => Ok(Self::CurrentDatabase),
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

fn udf_to_expr(udf: ScalarUDF, args: Vec<Expr>) -> Expr {
    Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
        udf.into(),
        args,
    ))
}

fn kdl_matches() -> ScalarUDF {
    ScalarUDF {
        name: "kdl_matches".to_string(),
        signature: Signature::new(
            TypeSignature::OneOf(vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::LargeUtf8]),
                TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::LargeUtf8]),
            ]),
            Volatility::Immutable,
        ),
        return_type: Arc::new(|_| Ok(Arc::new(DataType::Boolean))),
        fun: Arc::new(move |input| {
            let (doc, filter) = kdl_parse_udf_args(input)?;

            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                doc.query(filter)
                    .map_err(|e| datafusion::common::DataFusionError::Execution(e.to_string()))
                    .map(|val| val.is_some())?,
            ))))
        }),
    }
}

fn kdl_select() -> ScalarUDF {
    ScalarUDF {
        name: "kdl_select".to_string(),
        signature: Signature::new(
            // args: <FIELD>, <QUERY>
            TypeSignature::OneOf(vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::LargeUtf8]),
                TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::LargeUtf8]),
            ]),
            Volatility::Immutable,
        ),
        return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
        fun: Arc::new(move |input| {
            let (sdoc, filter) = kdl_parse_udf_args(input)?;

            let out: Vec<&KdlNode> = sdoc
                .query_all(filter)
                .map_err(|e| datafusion::common::DataFusionError::Execution(e.to_string()))
                .map(|iter| iter.collect())?;

            let mut doc = sdoc.clone();
            let elems = doc.nodes_mut();
            elems.clear();
            for item in &out {
                elems.push(item.to_owned().clone())
            }

            // TODO: consider if we should always return LargeUtf8?
            // could end up with truncation (or an error) the document
            // is too long and we write the data to a table that is
            // established (and mostly) shorter values.
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                doc.to_string(),
            ))))
        }),
    }
}

fn kdl_parse_udf_args(args: &[ColumnarValue]) -> Result<(KdlDocument, KdlQuery)> {
    // parse the filter first, because it's probably shorter and
    // erroring earlier would be preferable to parsing a large that we
    // don't need/want.
    let filter: kdl::KdlQuery = match get_nth_scalar_value(args, 1) {
        Some(ScalarValue::Utf8(Some(val))) | Some(ScalarValue::LargeUtf8(Some(val))) => {
            val.parse().map_err(|err: kdl::KdlError| {
                datafusion::common::DataFusionError::Execution(err.to_string())
            })?
        }
        Some(val) => {
            return Err(datafusion::common::DataFusionError::Execution(format!(
                "invalid type for KQL expression {}",
                val.data_type(),
            )))
        }
        None => {
            return Err(datafusion::common::DataFusionError::Execution(
                "unknown KQL query".to_string(),
            ))
        }
    };

    let doc: kdl::KdlDocument = match get_nth_scalar_value(args, 0) {
        Some(ScalarValue::Utf8(Some(val))) | Some(ScalarValue::LargeUtf8(Some(val))) => {
            val.parse().map_err(|err: kdl::KdlError| {
                datafusion::common::DataFusionError::Execution(err.to_string())
            })?
        }
        Some(val) => {
            return Err(datafusion::common::DataFusionError::Execution(format!(
                "invalid type for KDL value {}",
                val.data_type(),
            )))
        }
        None => {
            return Err(datafusion::common::DataFusionError::Execution(
                "invalid field for KDL".to_string(),
            ))
        }
    };

    Ok((doc, filter))
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
            ("current_schemas", CurrentSchemas.into()),
            ("current_catalog", CurrentCatalog.into()),
            ("kdl_matches", KdlMatches),
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

fn string_var(s: &str) -> Expr {
    Expr::ScalarVariable(DataType::Utf8, vec![s.to_string()])
}
