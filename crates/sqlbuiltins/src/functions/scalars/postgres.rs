use std::sync::Arc;

use crate::functions::{BuiltinFunction, BuiltinScalarUDF, ConstBuiltinFunction};
use datafusion::{
    arrow::datatypes::{DataType, Field},
    logical_expr::{Expr, ScalarUDF, Signature, TypeSignature, Volatility},
    physical_plan::ColumnarValue,
    scalar::ScalarValue,
};
use once_cell::sync::Lazy;
use protogen::metastore::types::catalog::FunctionType;

pub const BUILTIN_POSTGRES_FUNCTIONS: Lazy<Vec<Arc<dyn BuiltinScalarUDF>>> = Lazy::new(|| {
    vec![
        Arc::new(HasSchemaPrivilege {}),
        Arc::new(HasDatabasePrivilege {}),
        Arc::new(HasTablePrivilege {}),
        Arc::new(CurrentSchemas {}),
        Arc::new(CurrentUser {}),
        Arc::new(CurrentRole {}),
        Arc::new(CurrentSchema {}),
        Arc::new(CurrentDatabase {}),
        Arc::new(CurrentCatalog {}),
        Arc::new(User {}),
        Arc::new(PgGetUserById {}),
        Arc::new(PgTableIsVisible {}),
        Arc::new(PgEncodingToChar {}),
    ]
});

#[derive(Clone)]
pub struct PgGetUserById {}
impl ConstBuiltinFunction for PgGetUserById {
    const NAME: &'static str = "pg_get_userbyid";
    const DESCRIPTION: &'static str = "Postgres `pg_get_userbyid` function";
    const EXAMPLE: &'static str = "pg_get_userbyid(1)";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![DataType::Int64]),
            Volatility::Immutable,
        ))
    }
}

impl BuiltinScalarUDF for PgGetUserById {
    fn as_builtin_function(&self) -> Arc<dyn BuiltinFunction> {
        Arc::new(self.clone())
    }
    fn udf(&self) -> ScalarUDF {
        ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
            fun: Arc::new(move |_| {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    "unknown".to_string(),
                ))))
            }),
        }
    }
    fn into_expr(&self, args: Vec<Expr>) -> Expr {
        Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
            Arc::new(self.udf()),
            args,
        ))
    }
}
#[derive(Clone)]
pub struct PgTableIsVisible {}
impl ConstBuiltinFunction for PgTableIsVisible {
    const NAME: &'static str = "pg_table_is_visible";
    const DESCRIPTION: &'static str = "Postgres `pg_table_is_visible` function";
    const EXAMPLE: &'static str = "pg_table_is_visible(1)";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![DataType::Int64]),
            Volatility::Immutable,
        ))
    }
}
impl BuiltinScalarUDF for PgTableIsVisible {
    fn as_builtin_function(&self) -> Arc<dyn BuiltinFunction> {
        Arc::new(self.clone())
    }
    fn udf(&self) -> ScalarUDF {
        ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
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
    fn into_expr(&self, args: Vec<Expr>) -> Expr {
        Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
            Arc::new(self.udf()),
            args,
        ))
    }
}

#[derive(Clone)]
pub struct PgEncodingToChar {}
impl ConstBuiltinFunction for PgEncodingToChar {
    const NAME: &'static str = "pg_encoding_to_char";
    const DESCRIPTION: &'static str = "Postgres `pg_encoding_to_char` function";
    const EXAMPLE: &'static str = "pg_encoding_to_char(1)";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![DataType::Int64]),
            Volatility::Immutable,
        ))
    }
}

impl BuiltinScalarUDF for PgEncodingToChar {
    fn as_builtin_function(&self) -> Arc<dyn BuiltinFunction> {
        Arc::new(self.clone())
    }
    fn udf(&self) -> ScalarUDF {
        ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
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
    fn into_expr(&self, args: Vec<Expr>) -> Expr {
        Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
            Arc::new(self.udf()),
            args,
        ))
    }
}
#[derive(Clone)]
pub struct HasSchemaPrivilege {}
impl ConstBuiltinFunction for HasSchemaPrivilege {
    const NAME: &'static str = "has_schema_privilege";
    const DESCRIPTION: &'static str = "Returns true if user have privilege for schema";
    const EXAMPLE: &'static str = "has_schema_privilege('foo', 'bar', 'baz')";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
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
    fn as_builtin_function(&self) -> Arc<dyn BuiltinFunction> {
        Arc::new(self.clone())
    }
    fn udf(&self) -> ScalarUDF {
        ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Boolean))),
            fun: Arc::new(move |_input| {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
            }),
        }
    }
    fn into_expr(&self, args: Vec<Expr>) -> Expr {
        Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
            Arc::new(self.udf()),
            args,
        ))
    }
}
#[derive(Clone)]
pub struct HasDatabasePrivilege {}
impl ConstBuiltinFunction for HasDatabasePrivilege {
    const NAME: &'static str = "has_database_privilege";
    const DESCRIPTION: &'static str = "Returns true if user have privilege for database";
    const EXAMPLE: &'static str = "has_database_privilege('foo', 'bar', 'baz')";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
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
    fn as_builtin_function(&self) -> Arc<dyn BuiltinFunction> {
        Arc::new(self.clone())
    }
    fn udf(&self) -> ScalarUDF {
        ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Boolean))),
            fun: Arc::new(move |_input| {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
            }),
        }
    }
    fn into_expr(&self, args: Vec<Expr>) -> Expr {
        Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
            Arc::new(self.udf()),
            args,
        ))
    }
}

#[derive(Clone)]
pub struct HasTablePrivilege {}
impl ConstBuiltinFunction for HasTablePrivilege {
    const NAME: &'static str = "has_table_privilege";
    const DESCRIPTION: &'static str = "Returns true if user have privilege for table";
    const EXAMPLE: &'static str = "has_table_privilege('foo', 'bar', 'baz')";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
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
    fn as_builtin_function(&self) -> Arc<dyn BuiltinFunction> {
        Arc::new(self.clone())
    }
    fn udf(&self) -> ScalarUDF {
        ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Boolean))),
            fun: Arc::new(move |_input| {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
            }),
        }
    }
    fn into_expr(&self, args: Vec<Expr>) -> Expr {
        Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
            Arc::new(self.udf()),
            args,
        ))
    }
}

#[derive(Clone)]
pub struct CurrentSchemas {}
impl ConstBuiltinFunction for CurrentSchemas {
    const NAME: &'static str = "current_schemas";
    const DESCRIPTION: &'static str = "Returns current schemas";
    const EXAMPLE: &'static str = "current_schemas()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
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
    fn as_builtin_function(&self) -> Arc<dyn BuiltinFunction> {
        Arc::new(self.clone())
    }
    fn udf(&self) -> ScalarUDF {
        ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
            fun: Arc::new(move |input| todo!()),
        }
    }
    fn into_expr(&self, args: Vec<Expr>) -> Expr {
        string_var("current_schemas")
    }
}
#[derive(Clone)]
pub struct CurrentUser {}
impl ConstBuiltinFunction for CurrentUser {
    const NAME: &'static str = "current_user";
    const DESCRIPTION: &'static str = "Returns current user";
    const EXAMPLE: &'static str = "current_user()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![]),
            Volatility::Stable,
        ))
    }
}
impl BuiltinScalarUDF for CurrentUser {
    fn as_builtin_function(&self) -> Arc<dyn BuiltinFunction> {
        Arc::new(self.clone())
    }
    fn udf(&self) -> ScalarUDF {
        ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
            fun: Arc::new(move |input| todo!()),
        }
    }
    fn into_expr(&self, _: Vec<Expr>) -> Expr {
        string_var("current_user")
    }
}

#[derive(Clone)]
pub struct CurrentRole {}
impl ConstBuiltinFunction for CurrentRole {
    const NAME: &'static str = "current_role";
    const DESCRIPTION: &'static str = "Returns current role";
    const EXAMPLE: &'static str = "current_role()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![]),
            Volatility::Stable,
        ))
    }
}
impl BuiltinScalarUDF for CurrentRole {
    fn as_builtin_function(&self) -> Arc<dyn BuiltinFunction> {
        Arc::new(self.clone())
    }
    fn udf(&self) -> ScalarUDF {
        ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
            fun: Arc::new(move |input| todo!()),
        }
    }
    fn into_expr(&self, _: Vec<Expr>) -> Expr {
        string_var("current_role")
    }
}

#[derive(Clone)]
pub struct CurrentSchema {}
impl ConstBuiltinFunction for CurrentSchema {
    const NAME: &'static str = "current_schema";
    const DESCRIPTION: &'static str = "Returns current schema";
    const EXAMPLE: &'static str = "current_schema()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![]),
            Volatility::Stable,
        ))
    }
}
impl BuiltinScalarUDF for CurrentSchema {
    fn as_builtin_function(&self) -> Arc<dyn BuiltinFunction> {
        Arc::new(self.clone())
    }
    fn udf(&self) -> ScalarUDF {
        ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
            fun: Arc::new(move |input| todo!()),
        }
    }
    fn into_expr(&self, _: Vec<Expr>) -> Expr {
        string_var("current_schema")
    }
}
#[derive(Clone)]
pub struct CurrentDatabase {}
impl ConstBuiltinFunction for CurrentDatabase {
    const NAME: &'static str = "current_database";
    const DESCRIPTION: &'static str = "Returns current database";
    const EXAMPLE: &'static str = "current_database()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![]),
            Volatility::Stable,
        ))
    }
}
impl BuiltinScalarUDF for CurrentDatabase {
    fn as_builtin_function(&self) -> Arc<dyn BuiltinFunction> {
        Arc::new(self.clone())
    }
    fn udf(&self) -> ScalarUDF {
        ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
            fun: Arc::new(move |input| todo!()),
        }
    }
    fn into_expr(&self, _: Vec<Expr>) -> Expr {
        string_var("current_database")
    }
}
#[derive(Clone)]
pub struct CurrentCatalog {}
impl ConstBuiltinFunction for CurrentCatalog {
    const NAME: &'static str = "current_catalog";
    const DESCRIPTION: &'static str = "Returns current catalog";
    const EXAMPLE: &'static str = "current_catalog()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![]),
            Volatility::Stable,
        ))
    }
}
impl BuiltinScalarUDF for CurrentCatalog {
    fn as_builtin_function(&self) -> Arc<dyn BuiltinFunction> {
        Arc::new(self.clone())
    }
    fn udf(&self) -> ScalarUDF {
        ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
            fun: Arc::new(move |input| todo!()),
        }
    }
    fn into_expr(&self, _: Vec<Expr>) -> Expr {
        string_var("current_catalog")
    }
}

#[derive(Clone)]
pub struct User {}
impl ConstBuiltinFunction for User {
    const NAME: &'static str = "user";
    const DESCRIPTION: &'static str = "equivalent to `current_user`";
    const EXAMPLE: &'static str = "user()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![]),
            Volatility::Stable,
        ))
    }
}
impl BuiltinScalarUDF for User {
    fn as_builtin_function(&self) -> Arc<dyn BuiltinFunction> {
        Arc::new(self.clone())
    }
    fn udf(&self) -> ScalarUDF {
        CurrentUser {}.udf()
    }
    fn into_expr(&self, args: Vec<Expr>) -> Expr {
        CurrentUser {}.into_expr(args)
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

fn string_var(s: &str) -> Expr {
    Expr::ScalarVariable(DataType::Utf8, vec![s.to_string()])
}
