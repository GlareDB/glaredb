use std::sync::Arc;

use catalog::session_catalog::SessionCatalog;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    ColumnarValue,
    ReturnTypeFunction,
    ScalarFunctionImplementation,
    ScalarUDF,
    ScalarUDFImpl,
    Signature,
    TypeSignature,
    Volatility,
};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use datasources::json::errors::JsonError;
use datasources::json::jaq::compile_jaq_query;
use jaq_interpret::{Ctx, FilterT, RcIter, Val};
use protogen::metastore::types::catalog::FunctionType;
use serde_json::Value;

use super::{get_nth_string_fn_arg, get_nth_string_value};
use crate::errors::BuiltinError;
use crate::functions::{BuiltinScalarUDF, ConstBuiltinFunction};

#[derive(Debug)]
pub struct JAQSelect {
    signature: Signature,
}

impl ConstBuiltinFunction for JAQSelect {
    const NAME: &'static str = "jaq_select";
    const DESCRIPTION: &'static str = "Select nodes from a JAQ document";
    const EXAMPLE: &'static str = "jaq_select(docs, '[age=120]')";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;

    fn signature(&self) -> Option<Signature> {
        Some(self.signature.clone())
    }
}

impl Default for JAQSelect {
    fn default() -> Self {
        Self::new()
    }
}

impl JAQSelect {
    pub fn new() -> Self {
        Self {
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
        }
    }
}

impl ScalarUDFImpl for JAQSelect {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        Self::NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke(&self, input: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        let filter =
            compile_jaq_query(get_nth_string_fn_arg(input, 1)?).map_err(JsonError::from)?;

        get_nth_string_value(
            input,
            0,
            &|value: String| -> Result<ScalarValue, BuiltinError> {
                let val: Value = serde_json::from_str(&value)?;
                let inputs = RcIter::new(core::iter::empty());

                let output = filter
                    .run((Ctx::new([], &inputs), Val::from(val)))
                    .map(|res| res.map(|v| jaq_to_scalar_string(&v)))
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(match output.len() {
                    0 => ScalarValue::Utf8(None),
                    1 => output.first().unwrap().to_owned(),
                    _ => ScalarValue::List(ScalarValue::new_list(&output, &DataType::Utf8)),
                })
            },
        )
        .map_err(DataFusionError::from)
    }
}


impl BuiltinScalarUDF for JAQSelect {
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        let return_type_fn: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Utf8)));

        let scalar_fn_impl: ScalarFunctionImplementation =
            Arc::new(move |input| ScalarUDFImpl::invoke(&Self::new(), input));

        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(ScalarUDF::new(
                Self::NAME,
                &ConstBuiltinFunction::signature(self).unwrap(),
                &return_type_fn,
                &scalar_fn_impl,
            )),
            args,
        )))
    }

    fn try_into_scalar_udf(self: Arc<Self>) -> datafusion::error::Result<ScalarUDF> {
        Ok(Self::new().into())
    }
}

#[derive(Debug)]
pub struct JAQMatches {
    signature: Signature,
}

impl Default for JAQMatches {
    fn default() -> Self {
        Self::new()
    }
}

impl JAQMatches {
    pub fn new() -> Self {
        Self {
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
        }
    }
}

impl ConstBuiltinFunction for JAQMatches {
    const NAME: &'static str = "jaq_matches";
    const DESCRIPTION: &'static str =
        "Returns a predicate indicating if a JSON document matches a JAQ query";
    const EXAMPLE: &'static str = "jaq_matches(docs, '[b=100]')";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;

    fn signature(&self) -> Option<Signature> {
        Some(self.signature.clone())
    }
}

impl ScalarUDFImpl for JAQMatches {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        Self::NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(&self, input: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        let filter =
            compile_jaq_query(get_nth_string_fn_arg(input, 1)?).map_err(JsonError::from)?;

        get_nth_string_value(
            input,
            0,
            &|value: String| -> Result<ScalarValue, BuiltinError> {
                let val: Value = serde_json::from_str(&value)?;
                let input = RcIter::new(core::iter::empty());

                let output = filter.run((Ctx::new([], &input), Val::from(val)));

                for res in output {
                    match res? {
                        Val::Null => continue,
                        Val::Str(s) if s.is_empty() => continue,
                        Val::Str(_) => return Ok(ScalarValue::Boolean(Some(true))),
                        other if other.to_string().is_empty() => continue,
                        _ => return Ok(ScalarValue::Boolean(Some(true))),
                    }
                }

                Ok(ScalarValue::Boolean(Some(false)))
            },
        )
        .map_err(DataFusionError::from)
    }
}

impl BuiltinScalarUDF for JAQMatches {
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        let return_type_fn: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Boolean)));

        let scalar_fn_impl: ScalarFunctionImplementation =
            Arc::new(move |input| ScalarUDFImpl::invoke(&Self::new(), input));

        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(ScalarUDF::new(
                Self::NAME,
                &ConstBuiltinFunction::signature(self).unwrap(),
                &return_type_fn,
                &scalar_fn_impl,
            )),
            args,
        )))
    }

    fn try_into_scalar_udf(self: Arc<Self>) -> datafusion::error::Result<ScalarUDF> {
        Ok(Self::new().into())
    }
}


fn jaq_to_scalar_string(value: &Val) -> ScalarValue {
    match value {
        Val::Null => ScalarValue::Utf8(None),
        Val::Str(s) => ScalarValue::Utf8(Some(s.as_str().to_owned())),
        v => ScalarValue::Utf8(Some(v.to_string())),
    }
}
