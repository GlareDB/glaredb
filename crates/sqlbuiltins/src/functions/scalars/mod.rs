pub mod df_scalars;
pub mod kdl;
pub mod postgres;

use std::sync::Arc;

use datafusion::arrow::array::{make_array, Array, ArrayDataBuilder};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::BuiltinScalarFunction;
use datafusion::logical_expr::{Expr, ScalarUDF, Signature, TypeSignature, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datafusion::scalar::ScalarValue;
use num_traits::ToPrimitive;

use crate::document;
use crate::errors::BuiltinError;
use crate::functions::{BuiltinFunction, BuiltinScalarUDF, ConstBuiltinFunction};
use protogen::metastore::types::catalog::FunctionType;

pub struct ConnectionId;

impl ConstBuiltinFunction for ConnectionId {
    const NAME: &'static str = "connection_id";
    const DESCRIPTION: &'static str = "Returns the connection id of the current session";
    const EXAMPLE: &'static str = "connection_id()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::exact(vec![], Volatility::Stable))
    }
}

impl BuiltinScalarUDF for ConnectionId {
    fn as_expr(&self, _: Vec<Expr>) -> Expr {
        session_var("connection_id")
    }
}

pub struct Version;

impl ConstBuiltinFunction for Version {
    const NAME: &'static str = "version";
    const DESCRIPTION: &'static str = "Returns the version of the database";
    const EXAMPLE: &'static str = "version()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::exact(vec![], Volatility::Stable))
    }
}

impl BuiltinScalarUDF for Version {
    fn as_expr(&self, _: Vec<Expr>) -> Expr {
        session_var("version")
    }
}

fn get_nth_scalar_value(
    input: &[ColumnarValue],
    n: usize,
    op: &dyn Fn(Option<ScalarValue>) -> Result<ScalarValue, BuiltinError>,
    output_type: DataType,
) -> Result<ColumnarValue, BuiltinError> {
    match input.get(n) {
        Some(input) => match input {
            ColumnarValue::Scalar(scalar) => Ok(ColumnarValue::Scalar(op(Some(scalar.clone()))?)),
            ColumnarValue::Array(arr) => {
                let mut builder = ArrayDataBuilder::new(output_type);

                for idx in 0..arr.len() {
                    builder.add_child_data(
                        op(Some(ScalarValue::try_from_array(arr, idx)?))?
                            .to_array()
                            .into_data(),
                    );
                }

                Ok(ColumnarValue::Array(make_array(builder.build()?)))
            }
        },
        None => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)))),
    }
}

fn safe_up_cast_integer_scalar(
    dt: DataType,
    idx: usize,
    value: i64,
) -> Result<u64, DataFusionError> {
    if value < 0 {
        Err(datafusion::error::DataFusionError::Execution(
            format!(
                "expected {} value at {} to be greater than zero or unsigned",
                dt, idx,
            )
            .to_string(),
        ))
    } else {
        Ok(value as u64)
    }
}

fn get_nth_scalar_as_u64(
    input: &[ColumnarValue],
    n: usize,
    op: &dyn Fn(Option<u64>) -> Result<ScalarValue, BuiltinError>,
    output_type: DataType,
) -> Result<ColumnarValue, BuiltinError> {
    get_nth_scalar_value(
        input,
        n,
        &|scalar| -> Result<ScalarValue, BuiltinError> {
            let scalar = match scalar.clone() {
                Some(v) => v.to_owned(),
                None => return Err(BuiltinError::MissingValueAtIndex(n)),
            };

            let value = match scalar {
                ScalarValue::Int8(Some(value)) => {
                    safe_up_cast_integer_scalar(scalar.data_type(), n, value as i64)
                }
                ScalarValue::Int16(Some(value)) => {
                    safe_up_cast_integer_scalar(scalar.data_type(), n, value as i64)
                }
                ScalarValue::Int32(Some(value)) => {
                    safe_up_cast_integer_scalar(scalar.data_type(), n, value as i64)
                }
                ScalarValue::Int64(Some(value)) => {
                    safe_up_cast_integer_scalar(scalar.data_type(), n, value)
                }
                ScalarValue::UInt8(Some(value)) => Ok(value as u64),
                ScalarValue::UInt16(Some(value)) => Ok(value as u64),
                ScalarValue::UInt32(Some(value)) => Ok(value as u64),
                ScalarValue::Float64(Some(value)) => {
                    if value.trunc() != value {
                        return Err(BuiltinError::InvalidValueAtIndex(
                            n,
                            format!("expected whole value for float {}", value).to_string(),
                        ));
                    }
                    Ok(value.to_i64().ok_or(BuiltinError::IncorrectTypeAtIndex(
                        n,
                        scalar.data_type(),
                        DataType::UInt64,
                    ))? as u64)
                }
                ScalarValue::Float32(Some(value)) => {
                    if value.trunc() != value {
                        return Err(BuiltinError::InvalidValueAtIndex(
                            n,
                            format!("expected whole value for float {}", value).to_string(),
                        ));
                    }
                    Ok(value.to_i64().ok_or(BuiltinError::IncorrectTypeAtIndex(
                        n,
                        scalar.data_type(),
                        DataType::UInt64,
                    ))? as u64)
                }
                ScalarValue::UInt64(Some(value)) => Ok(value),
                _ => {
                    return Err(BuiltinError::IncorrectTypeAtIndex(
                        n,
                        scalar.data_type(),
                        DataType::UInt64,
                    ))
                }
            }?;

            op(Some(value))
        },
        output_type,
    )
}

fn get_nth_string_value(
    input: &[ColumnarValue],
    n: usize,
    op: &dyn Fn(Option<String>) -> Result<ScalarValue, BuiltinError>,
    output_type: DataType,
) -> Result<ColumnarValue, BuiltinError> {
    get_nth_scalar_value(
        input,
        n,
        &|scalar| -> Result<ScalarValue, BuiltinError> {
            let scalar = match scalar.clone() {
                Some(v) => v.to_owned(),
                None => return Err(BuiltinError::MissingValueAtIndex(n)),
            };

            match scalar {
                ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => op(v),
                _ => Err(BuiltinError::IncorrectTypeAtIndex(
                    n,
                    scalar.data_type(),
                    DataType::Utf8,
                )),
            }
        },
        output_type,
    )
}

fn session_var(s: &str) -> Expr {
    Expr::ScalarVariable(DataType::Utf8, vec![s.to_string()])
}
