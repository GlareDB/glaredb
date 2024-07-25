use std::sync::Arc;

use bson::Bson;
use catalog::session_catalog::SessionCatalog;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    ColumnarValue,
    ReturnTypeFunction,
    ScalarFunctionImplementation,
    ScalarUDF,
    Signature,
    TypeSignature,
    Volatility,
};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use protogen::metastore::types::catalog::FunctionType;

use super::apply_op_to_col_array;
use crate::errors::BuiltinError;
use crate::functions::{BuiltinScalarUDF, ConstBuiltinFunction};

pub struct Bson2Json;

impl ConstBuiltinFunction for Bson2Json {
    const NAME: &'static str = "bson2json";
    const DESCRIPTION: &'static str = "Converts a bson value to a (relaxed extended) json string";
    const EXAMPLE: &'static str = "bson2json(<value>)";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;

    fn signature(&self) -> Option<Signature> {
        Some(Signature::one_of(
            vec![TypeSignature::OneOf(vec![
                TypeSignature::Exact(vec![]),
                TypeSignature::Exact(vec![DataType::Binary]),
                TypeSignature::Exact(vec![DataType::LargeBinary]),
            ])],
            Volatility::Immutable,
        ))
    }
}

impl Bson2Json {
    fn convert(scalar: &ScalarValue) -> Result<ScalarValue, BuiltinError> {
        match scalar {
            ScalarValue::Binary(Some(v)) | ScalarValue::LargeBinary(Some(v)) => {
                Ok(ScalarValue::new_utf8(
                    bson::de::from_slice::<Bson>(&v)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?
                        .into_relaxed_extjson()
                        .to_string(),
                ))
            }
            ScalarValue::Binary(None) | ScalarValue::LargeBinary(None) => {
                Ok(ScalarValue::Utf8(None))
            }
            other => {
                return Err(BuiltinError::IncorrectType(
                    other.data_type(),
                    DataType::Binary,
                ))
            }
        }
    }
}

impl BuiltinScalarUDF for Bson2Json {
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        let return_type_fn: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Utf8)));
        let scalar_fn_impl: ScalarFunctionImplementation = Arc::new(move |input| {
            Ok(match input {
                [] => ColumnarValue::Scalar(ScalarValue::new_utf8("{}")),
                [ColumnarValue::Scalar(scalar)] => ColumnarValue::Scalar(Self::convert(scalar)?),
                [ColumnarValue::Array(array)] => {
                    ColumnarValue::Array(apply_op_to_col_array(array, &Self::convert)?)
                }
                _ => unreachable!("bson2json expects exactly one argument"),
            })
        });
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
}


pub struct Json2Bson;

impl ConstBuiltinFunction for Json2Bson {
    const NAME: &'static str = "json2bson";
    const DESCRIPTION: &'static str = "Converts a json string value to Bson";
    const EXAMPLE: &'static str = "json2bson(<value>)";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;

    fn signature(&self) -> Option<Signature> {
        Some(Signature::one_of(
            vec![TypeSignature::OneOf(vec![
                TypeSignature::Exact(vec![]),
                TypeSignature::Exact(vec![DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::LargeUtf8]),
            ])],
            Volatility::Immutable,
        ))
    }
}

impl Json2Bson {
    fn convert(scalar: &ScalarValue) -> Result<ScalarValue, BuiltinError> {
        match scalar {
            ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => {
                Ok(ScalarValue::Binary(Some(bson::ser::to_vec(
                    &serde_json::from_str::<serde_json::Value>(v)?,
                )?)))
            }
            ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => Ok(ScalarValue::Binary(None)),
            other => {
                return Err(BuiltinError::IncorrectType(
                    other.data_type(),
                    DataType::Utf8,
                ))
            }
        }
    }
}

impl BuiltinScalarUDF for Json2Bson {
    fn try_as_expr(&self, _: &SessionCatalog, args: Vec<Expr>) -> DataFusionResult<Expr> {
        let return_type_fn: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Binary)));
        let scalar_fn_impl: ScalarFunctionImplementation = Arc::new(move |input| {
            Ok(match input {
                [] => ColumnarValue::Scalar(ScalarValue::Binary(Some(Vec::new()))),
                [ColumnarValue::Scalar(scalar)] => ColumnarValue::Scalar(Self::convert(scalar)?),
                [ColumnarValue::Array(array)] => {
                    ColumnarValue::Array(apply_op_to_col_array(array, &Self::convert)?)
                }
                _ => unreachable!("json2bson expects exactly one argument"),
            })
        });
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
}
