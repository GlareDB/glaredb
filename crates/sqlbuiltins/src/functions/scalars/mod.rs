pub mod df_scalars;
pub mod hashing;
pub mod kdl;
pub mod postgres;

use std::sync::Arc;

use datafusion::arrow::array::Array;
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
) -> Result<ColumnarValue, BuiltinError> {
    match input.get(n) {
        Some(input) => match input {
            ColumnarValue::Scalar(scalar) => Ok(ColumnarValue::Scalar(op(Some(scalar.clone()))?)),
            ColumnarValue::Array(arr) => {
                let mut values = Vec::with_capacity(arr.len());

                for idx in 0..arr.len() {
                    let value = ScalarValue::try_from_array(arr, idx)?;
                    let value = op(Some(value))?;
                    values.push(value);
                }

                Ok(ColumnarValue::Array(ScalarValue::iter_to_array(
                    values.into_iter(),
                )?))
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

#[allow(dead_code)] // will get removed before this hits mainline; stacked commit issue
fn get_nth_scalar_as_u64(
    input: &[ColumnarValue],
    n: usize,
    op: &dyn Fn(Option<u64>) -> Result<ScalarValue, BuiltinError>,
) -> Result<ColumnarValue, BuiltinError> {
    get_nth_scalar_value(input, n, &|scalar| -> Result<ScalarValue, BuiltinError> {
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
    })
}

// get_nth_string_fn_arg extracts a string value (or tries to) from a
// function argument; columns are always an error.
fn get_nth_string_fn_arg(input: &[ColumnarValue], idx: usize) -> Result<String, BuiltinError> {
    match input.get(idx) {
        Some(input) => match input {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(v)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(v))) => Ok(v.to_owned()),
            ColumnarValue::Array(_) => Err(BuiltinError::InvalidColumnarValue(idx)),
            _ => Err(BuiltinError::IncorrectTypeAtIndex(
                idx,
                input.data_type(),
                DataType::Utf8,
            )),
        },
        None => Err(BuiltinError::MissingValueAtIndex(idx)),
    }
}

// get_nth_string_value processes a function argument that is expected
// to be a string, as a helper for a common case around
// get_nth_scalar_value.
fn get_nth_string_value(
    input: &[ColumnarValue],
    n: usize,
    op: &dyn Fn(Option<String>) -> Result<ScalarValue, BuiltinError>,
) -> Result<ColumnarValue, BuiltinError> {
    get_nth_scalar_value(input, n, &|scalar| -> Result<ScalarValue, BuiltinError> {
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
    })
}

fn session_var(s: &str) -> Expr {
    Expr::ScalarVariable(DataType::Utf8, vec![s.to_string()])
}
