use datafusion::logical_expr::expr::ScalarFunction;

use super::df_scalars::array_to_string;
use super::*;
use crate::functions::FunctionNamespace;

const PG_CATALOG_NAMESPACE: FunctionNamespace = FunctionNamespace::Optional("pg_catalog");

#[derive(Clone, Copy, Debug)]
pub struct PgGetUserById;

impl ConstBuiltinFunction for PgGetUserById {
    const DESCRIPTION: &'static str = "Postgres `pg_get_userbyid` function";
    const EXAMPLE: &'static str = "pg_get_userbyid(1)";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    const NAME: &'static str = "pg_get_userbyid";

    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![DataType::Int64]),
            Volatility::Immutable,
        ))
    }
}

impl BuiltinScalarUDF for PgGetUserById {
    fn as_expr(&self, args: Vec<Expr>) -> Expr {
        let udf = ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
            fun: Arc::new(move |_| {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    "unknown".to_string(),
                ))))
            }),
        };
        Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
            Arc::new(udf),
            args,
        ))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PgTableIsVisible;

impl ConstBuiltinFunction for PgTableIsVisible {
    const DESCRIPTION: &'static str = "Postgres `pg_table_is_visible` function";
    const EXAMPLE: &'static str = "pg_table_is_visible(1)";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    const NAME: &'static str = "pg_table_is_visible";

    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![DataType::Int64]),
            Volatility::Immutable,
        ))
    }
}

impl BuiltinScalarUDF for PgTableIsVisible {
    fn as_expr(&self, args: Vec<Expr>) -> Expr {
        let udf = ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Boolean))),
            fun: Arc::new(move |input| {
                Ok(get_nth_scalar_value(input, 0, &|value| -> Result<
                    ScalarValue,
                    BuiltinError,
                > {
                    match value {
                        ScalarValue::Int64(Some(_)) => Ok(ScalarValue::Boolean(Some(true))),
                        _ => Ok(ScalarValue::Boolean(None)),
                    }
                })?)
            }),
        };

        Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
            Arc::new(udf),
            args,
        ))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PgEncodingToChar;

impl ConstBuiltinFunction for PgEncodingToChar {
    const DESCRIPTION: &'static str = "Postgres `pg_encoding_to_char` function";
    const EXAMPLE: &'static str = "pg_encoding_to_char(1)";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    const NAME: &'static str = "pg_encoding_to_char";

    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![DataType::Int64]),
            Volatility::Immutable,
        ))
    }
}

impl BuiltinScalarUDF for PgEncodingToChar {
    fn as_expr(&self, args: Vec<Expr>) -> Expr {
        let udf = ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
            fun: Arc::new(move |input| {
                Ok(get_nth_scalar_value(input, 0, &|value| -> Result<
                    ScalarValue,
                    BuiltinError,
                > {
                    match value {
                        ScalarValue::Int64(Some(6)) => {
                            Ok(ScalarValue::Utf8(Some("UTF8".to_string())))
                        }
                        ScalarValue::Int64(Some(_)) => Ok(ScalarValue::Utf8(Some("".to_string()))),
                        _ => Ok(ScalarValue::Utf8(None)),
                    }
                })?)
            }),
        };
        Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
            Arc::new(udf),
            args,
        ))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct HasSchemaPrivilege;

impl ConstBuiltinFunction for HasSchemaPrivilege {
    const DESCRIPTION: &'static str = "Returns true if user have privilege for schema";
    const EXAMPLE: &'static str = "has_schema_privilege('foo', 'bar', 'baz')";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    const NAME: &'static str = "has_schema_privilege";

    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::OneOf(vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ]),
            Volatility::Stable,
        ))
    }
}

impl BuiltinScalarUDF for HasSchemaPrivilege {
    fn as_expr(&self, args: Vec<Expr>) -> Expr {
        let udf = ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Boolean))),
            fun: Arc::new(move |_input| {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
            }),
        };
        Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
            Arc::new(udf),
            args,
        ))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct HasDatabasePrivilege;

impl ConstBuiltinFunction for HasDatabasePrivilege {
    const DESCRIPTION: &'static str = "Returns true if user have privilege for database";
    const EXAMPLE: &'static str = "has_database_privilege('foo', 'bar', 'baz')";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    const NAME: &'static str = "has_database_privilege";

    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::OneOf(vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ]),
            Volatility::Stable,
        ))
    }
}

impl BuiltinScalarUDF for HasDatabasePrivilege {
    fn as_expr(&self, args: Vec<Expr>) -> Expr {
        let udf = ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Boolean))),
            fun: Arc::new(move |_input| {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
            }),
        };
        Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
            Arc::new(udf),
            args,
        ))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct HasTablePrivilege;
impl ConstBuiltinFunction for HasTablePrivilege {
    const DESCRIPTION: &'static str = "Returns true if user have privilege for table";
    const EXAMPLE: &'static str = "has_table_privilege('foo', 'bar', 'baz')";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    const NAME: &'static str = "has_table_privilege";

    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::OneOf(vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ]),
            Volatility::Stable,
        ))
    }
}

impl BuiltinScalarUDF for HasTablePrivilege {
    fn as_expr(&self, args: Vec<Expr>) -> Expr {
        let udf = ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Boolean))),
            fun: Arc::new(move |_input| {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
            }),
        };
        Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
            Arc::new(udf),
            args,
        ))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CurrentSchemas;

impl ConstBuiltinFunction for CurrentSchemas {
    const DESCRIPTION: &'static str = "Returns current schemas";
    const EXAMPLE: &'static str = "current_schemas()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    const NAME: &'static str = "current_schemas";

    fn signature(&self) -> Option<Signature> {
        Some(Signature::one_of(
            vec![
                TypeSignature::Exact(vec![]),
                TypeSignature::Exact(vec![DataType::Boolean]),
            ],
            Volatility::Stable,
        ))
    }
}

impl BuiltinScalarUDF for CurrentSchemas {
    fn as_expr(&self, args: Vec<Expr>) -> Expr {
        // There's no good way to handle the `include_implicit` argument,
        // but since its a binary value (true/false),
        // we can just assign it to a different variable
        let var_name = if let Some(Expr::Literal(ScalarValue::Boolean(Some(true)))) = args.first() {
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

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CurrentUser;

impl ConstBuiltinFunction for CurrentUser {
    const DESCRIPTION: &'static str = "Returns current user";
    const EXAMPLE: &'static str = "current_user()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    const NAME: &'static str = "current_user";

    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![]),
            Volatility::Stable,
        ))
    }
}
impl BuiltinScalarUDF for CurrentUser {
    fn as_expr(&self, _: Vec<Expr>) -> Expr {
        session_var("current_user")
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CurrentRole;

impl ConstBuiltinFunction for CurrentRole {
    const DESCRIPTION: &'static str = "Returns current role";
    const EXAMPLE: &'static str = "current_role()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    const NAME: &'static str = "current_role";

    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![]),
            Volatility::Stable,
        ))
    }
}

impl BuiltinScalarUDF for CurrentRole {
    fn as_expr(&self, _: Vec<Expr>) -> Expr {
        session_var("current_role")
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CurrentSchema;

impl ConstBuiltinFunction for CurrentSchema {
    const DESCRIPTION: &'static str = "Returns current schema";
    const EXAMPLE: &'static str = "current_schema()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    const NAME: &'static str = "current_schema";

    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![]),
            Volatility::Stable,
        ))
    }
}

impl BuiltinScalarUDF for CurrentSchema {
    fn as_expr(&self, _: Vec<Expr>) -> Expr {
        session_var("current_schema")
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CurrentDatabase;

impl ConstBuiltinFunction for CurrentDatabase {
    const DESCRIPTION: &'static str = "Returns current database";
    const EXAMPLE: &'static str = "current_database()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    const NAME: &'static str = "current_database";

    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![]),
            Volatility::Stable,
        ))
    }
}

impl BuiltinScalarUDF for CurrentDatabase {
    fn as_expr(&self, _: Vec<Expr>) -> Expr {
        session_var("current_database")
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CurrentCatalog;

impl ConstBuiltinFunction for CurrentCatalog {
    const DESCRIPTION: &'static str = "Returns current catalog";
    const EXAMPLE: &'static str = "current_catalog()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    const NAME: &'static str = "current_catalog";

    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![]),
            Volatility::Stable,
        ))
    }
}

impl BuiltinScalarUDF for CurrentCatalog {
    fn as_expr(&self, _: Vec<Expr>) -> Expr {
        session_var("current_catalog")
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct User;

impl ConstBuiltinFunction for User {
    const DESCRIPTION: &'static str = "equivalent to `current_user`";
    const EXAMPLE: &'static str = "user()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    const NAME: &'static str = "user";

    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![]),
            Volatility::Stable,
        ))
    }
}

impl BuiltinScalarUDF for User {
    fn as_expr(&self, args: Vec<Expr>) -> Expr {
        CurrentUser.as_expr(args).alias("user")
    }

    fn namespace(&self) -> FunctionNamespace {
        CurrentUser.namespace()
    }
}

// This one is a bit different from the others as it's also handled via
// datafusion. So all we need to do is add it to the pg_catalog namespace and
// map it to the df implementation
#[derive(Clone, Copy, Debug)]
pub struct PgArrayToString;

impl ConstBuiltinFunction for PgArrayToString {
    const DESCRIPTION: &'static str = array_to_string::DESCRIPTION;
    const EXAMPLE: &'static str = array_to_string::EXAMPLE;
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    const NAME: &'static str = array_to_string::NAME;

    fn signature(&self) -> Option<Signature> {
        Some(BuiltinScalarFunction::ArrayToString.signature())
    }
}

impl BuiltinScalarUDF for PgArrayToString {
    fn as_expr(&self, args: Vec<Expr>) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new(
            BuiltinScalarFunction::ArrayToString,
            args,
        ))
    }

    fn namespace(&self) -> FunctionNamespace {
        FunctionNamespace::Required("pg_catalog")
    }
}
