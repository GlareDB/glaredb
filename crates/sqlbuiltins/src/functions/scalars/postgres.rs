use std::sync::Arc;

use catalog::session_catalog::SessionCatalog;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    ReturnTypeFunction,
    ScalarFunctionImplementation,
    ScalarUDF,
    Signature,
    TypeSignature,
    Volatility,
};
use datafusion::physical_plan::ColumnarValue;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use pgrepr::compatible::server_version_with_build_info;
use protogen::metastore::types::catalog::FunctionType;

use super::df_scalars::array_to_string;
use super::{get_nth_scalar_value, session_var};
use crate::errors::BuiltinError;
use crate::functions::{BuiltinScalarUDF, ConstBuiltinFunction, FunctionNamespace};

const PG_CATALOG_NAMESPACE: FunctionNamespace = FunctionNamespace::Optional("pg_catalog");

#[derive(Clone, Copy, Debug)]
pub struct PgGetUserById;

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
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        let return_type_fn: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Utf8)));
        let scalar_fn_impl: ScalarFunctionImplementation = Arc::new(move |_| {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "unknown".to_string(),
            ))))
        });
        let udf = ScalarUDF::new(
            Self::NAME,
            &ConstBuiltinFunction::signature(self).unwrap(),
            &return_type_fn,
            &scalar_fn_impl,
        );

        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(udf),
            args,
        )))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PgTableIsVisible;

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
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        let return_type_fn: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Boolean)));
        let scalar_fn_impl: ScalarFunctionImplementation = Arc::new(move |input| {
            Ok(get_nth_scalar_value(input, 0, &|value| -> Result<
                ScalarValue,
                BuiltinError,
            > {
                match value {
                    ScalarValue::Int64(Some(_)) => Ok(ScalarValue::Boolean(Some(true))),
                    _ => Ok(ScalarValue::Boolean(None)),
                }
            })?)
        });

        let udf = ScalarUDF::new(
            Self::NAME,
            &ConstBuiltinFunction::signature(self).unwrap(),
            &return_type_fn,
            &scalar_fn_impl,
        );
        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(udf),
            args,
        )))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PgEncodingToChar;

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
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        let return_type_fn: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Utf8)));
        let scalar_fn_impl: ScalarFunctionImplementation = Arc::new(move |input| {
            Ok(get_nth_scalar_value(input, 0, &|value| -> Result<
                ScalarValue,
                BuiltinError,
            > {
                match value {
                    ScalarValue::Int64(Some(6)) => Ok(ScalarValue::Utf8(Some("UTF8".to_string()))),
                    ScalarValue::Int64(Some(_)) => Ok(ScalarValue::Utf8(Some("".to_string()))),
                    _ => Ok(ScalarValue::Utf8(None)),
                }
            })?)
        });
        let udf = ScalarUDF::new(
            Self::NAME,
            &ConstBuiltinFunction::signature(self).unwrap(),
            &return_type_fn,
            &scalar_fn_impl,
        );
        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(udf),
            args,
        )))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct HasSchemaPrivilege;

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
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        let return_type_fn: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Boolean)));
        let scalar_fn_impl: ScalarFunctionImplementation =
            Arc::new(move |_input| Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)))));
        let udf = ScalarUDF::new(
            Self::NAME,
            &ConstBuiltinFunction::signature(self).unwrap(),
            &return_type_fn,
            &scalar_fn_impl,
        );
        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(udf),
            args,
        )))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct HasDatabasePrivilege;

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
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        let return_type_fn: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Boolean)));
        let scalar_fn_impl: ScalarFunctionImplementation =
            Arc::new(move |_input| Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)))));
        let udf = ScalarUDF::new(
            Self::NAME,
            &ConstBuiltinFunction::signature(self).unwrap(),
            &return_type_fn,
            &scalar_fn_impl,
        );
        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(udf),
            args,
        )))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct HasTablePrivilege;
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
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        let return_type_fn: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Boolean)));
        let scalar_fn_impl: ScalarFunctionImplementation =
            Arc::new(move |_| Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)))));
        let udf = ScalarUDF::new(
            Self::NAME,
            &ConstBuiltinFunction::signature(self).unwrap(),
            &return_type_fn,
            &scalar_fn_impl,
        );
        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(udf),
            args,
        )))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CurrentSchemas;

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
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        // There's no good way to handle the `include_implicit` argument,
        // but since its a binary value (true/false),
        // we can just assign it to a different variable
        let var_name = if let Some(Expr::Literal(ScalarValue::Boolean(Some(true)))) = args.first() {
            "current_schemas_include_implicit".to_string()
        } else {
            "current_schemas".to_string()
        };

        Ok(Expr::ScalarVariable(
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            vec![var_name],
        )
        .alias("current_schemas"))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CurrentUser;

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
    fn try_as_expr(&self, _: &SessionCatalog, _: Vec<Expr>) -> DataFusionResult<Expr> {
        Ok(session_var("current_user"))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CurrentRole;

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
    fn try_as_expr(&self, _: &SessionCatalog, _: Vec<Expr>) -> DataFusionResult<Expr> {
        Ok(session_var("current_role"))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CurrentSchema;

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
    fn try_as_expr(&self, _: &SessionCatalog, _: Vec<Expr>) -> DataFusionResult<Expr> {
        Ok(session_var("current_schema"))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CurrentDatabase;

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
    fn try_as_expr(&self, _: &SessionCatalog, _: Vec<Expr>) -> DataFusionResult<Expr> {
        Ok(session_var("current_database"))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CurrentCatalog;

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
    fn try_as_expr(&self, _: &SessionCatalog, _: Vec<Expr>) -> DataFusionResult<Expr> {
        Ok(session_var("current_catalog"))
    }

    fn namespace(&self) -> FunctionNamespace {
        PG_CATALOG_NAMESPACE
    }
}

#[derive(Clone, Copy, Debug)]
pub struct User;

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
    fn try_as_expr(&self, ctx: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        Ok(CurrentUser.try_as_expr(ctx, args)?.alias("user"))
    }

    fn namespace(&self) -> FunctionNamespace {
        CurrentUser.namespace()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PgArrayToString;

impl ConstBuiltinFunction for PgArrayToString {
    const NAME: &'static str = array_to_string::NAME;
    const DESCRIPTION: &'static str = array_to_string::DESCRIPTION;
    const EXAMPLE: &'static str = array_to_string::EXAMPLE;
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        // Datafusion doesn't have a good way to represent the signature of this function
        None
    }
}

impl BuiltinScalarUDF for PgArrayToString {
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        if args.len() < 2 || args.len() > 3 {
            return Err(DataFusionError::Plan(
                "array_to_string() takes exactly two or three arguments".to_string(),
            ));
        }
        Ok(datafusion_functions_array::expr_fn::array_to_string(
            args[0].clone(),
            args[1].clone(),
        ))
    }

    fn namespace(&self) -> FunctionNamespace {
        FunctionNamespace::Optional("pg_catalog")
    }
}

/// `pg_catalog.version()` implementation.
///
/// This provides more informatation that just the 'server_version' session
/// variable, and includes things like the build triple.
///
/// This uses a spoofed version (and so does not match the `version()` function)
/// since many postgres tools, including sqlalchemy, will check the version
/// against hard coded values.
#[derive(Clone, Copy, Debug)]
pub struct PgVersion;

impl ConstBuiltinFunction for PgVersion {
    const NAME: &'static str = "version";
    const DESCRIPTION: &'static str = "Returns the spoofed postgres version of the database";
    const EXAMPLE: &'static str = "pg_catalog.version()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::exact(vec![], Volatility::Stable))
    }
}

impl BuiltinScalarUDF for PgVersion {
    fn try_as_expr(&self, _: &SessionCatalog, _: Vec<Expr>) -> DataFusionResult<Expr> {
        Ok(Expr::Literal(ScalarValue::Utf8(Some(
            server_version_with_build_info().to_string(),
        ))))
    }

    fn namespace(&self) -> FunctionNamespace {
        FunctionNamespace::Required("pg_catalog")
    }
}


#[derive(Clone, Copy, Debug)]
pub struct FormatType;

impl ConstBuiltinFunction for FormatType {
    const NAME: &'static str = "format_type";
    const DESCRIPTION: &'static str = "mock for postgres format_type";
    const EXAMPLE: &'static str = "format_type(oid, int)";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;

    fn signature(&self) -> Option<Signature> {
        Some(Signature::exact(
            vec![DataType::Int32, DataType::Int32],
            Volatility::Stable,
        ))
    }
}

impl BuiltinScalarUDF for FormatType {
    fn try_as_expr(&self, _: &SessionCatalog, _: Vec<Expr>) -> DataFusionResult<Expr> {
        Ok(Expr::Literal(ScalarValue::Null))
    }
}
