pub mod df_scalars;
pub mod hashing;
pub mod kdl;
pub mod postgres;

use std::sync::Arc;

use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    BuiltinScalarFunction,
    Expr,
    ScalarUDF,
    Signature,
    TypeSignature,
    Volatility,
};
use datafusion::physical_plan::ColumnarValue;
use datafusion::scalar::ScalarValue;
use datafusion_ext::cast::scalar_iter_to_array;
use datafusion_ext::errors::ExtensionError;
use num_traits::ToPrimitive;
use protogen::metastore::types::catalog::FunctionType;

use crate::document;
use crate::errors::BuiltinError;
use crate::functions::{BuiltinFunction, BuiltinScalarUDF, ConstBuiltinFunction};

pub struct ConnectionId;

impl ConstBuiltinFunction for ConnectionId {
    const DESCRIPTION: &'static str = "Returns the connection id of the current session";
    const EXAMPLE: &'static str = "connection_id()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    const NAME: &'static str = "connection_id";

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
    const DESCRIPTION: &'static str = "Returns the version of the database";
    const EXAMPLE: &'static str = "version()";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    const NAME: &'static str = "version";

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
    op: &dyn Fn(ScalarValue) -> Result<ScalarValue, BuiltinError>,
) -> Result<ColumnarValue, BuiltinError> {
    match input.get(n) {
        Some(input) => match input {
            ColumnarValue::Scalar(scalar) => Ok(ColumnarValue::Scalar(op(scalar.clone())?)),
            ColumnarValue::Array(arr) => Ok(ColumnarValue::Array(scalar_iter_to_array(
                (0..arr.len()).map(|idx| -> Result<ScalarValue, ExtensionError> {
                    Ok(op(ScalarValue::try_from_array(arr, idx)?)?)
                }),
            )?)),
        },
        None => Err(BuiltinError::MissingValueAtIndex(n)),
    }
}

fn try_from_u64_scalar(scalar: ScalarValue) -> Result<u64, BuiltinError> {
    match scalar {
        ScalarValue::Int8(Some(value)) => safe_up_cast_integer_scalar(value as i64),
        ScalarValue::Int16(Some(value)) => safe_up_cast_integer_scalar(value as i64),
        ScalarValue::Int32(Some(value)) => safe_up_cast_integer_scalar(value as i64),
        ScalarValue::Int64(Some(value)) => safe_up_cast_integer_scalar(value),
        ScalarValue::UInt8(Some(value)) => Ok(value as u64),
        ScalarValue::UInt16(Some(value)) => Ok(value as u64),
        ScalarValue::UInt32(Some(value)) => Ok(value as u64),
        ScalarValue::Float64(Some(value)) => {
            if value.trunc() != value {
                return Err(BuiltinError::ParseError(
                    format!("expected whole value for float {}", value).to_string(),
                ));
            }
            Ok(value.to_i64().ok_or(BuiltinError::IncorrectType(
                scalar.data_type(),
                DataType::UInt64,
            ))? as u64)
        }
        ScalarValue::Float32(Some(value)) => {
            if value.trunc() != value {
                return Err(BuiltinError::InvalidValue(
                    format!("expected whole value for float {}", value).to_string(),
                ));
            }
            Ok(value.to_i64().ok_or(BuiltinError::IncorrectType(
                scalar.data_type(),
                DataType::UInt64,
            ))? as u64)
        }
        ScalarValue::UInt64(Some(value)) => Ok(value),
        _ => Err(BuiltinError::IncorrectType(
            scalar.data_type(),
            DataType::UInt64,
        )),
    }
}

#[allow(dead_code)] // just for merging order // TODO: What?
fn safe_up_cast_integer_scalar(value: i64) -> Result<u64, BuiltinError> {
    if value < 0 {
        Err(BuiltinError::ParseError(
            format!("{} cannot be a uint64", value).to_string(),
        ))
    } else {
        Ok(value as u64)
    }
}

// get_nth_64_fn_arg extracts a string value (or tries to) from a
// function argument; columns are always an error.
#[allow(dead_code)] // just for merging order
fn get_nth_u64_fn_arg(input: &[ColumnarValue], idx: usize) -> Result<u64, BuiltinError> {
    match input.get(idx) {
        Some(ColumnarValue::Scalar(value)) => try_from_u64_scalar(value.to_owned()),
        Some(ColumnarValue::Array(_)) => Err(BuiltinError::InvalidColumnarValue(idx)),
        None => Err(BuiltinError::MissingValueAtIndex(idx)),
    }
}

// get_nth_string_fn_arg extracts a string value (or tries to) from a
// function argument; columns are always an error.
fn get_nth_string_fn_arg(input: &[ColumnarValue], idx: usize) -> Result<String, BuiltinError> {
    match input.get(idx) {
        Some(input) => match input {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(v)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(v))) => Ok(v.to_owned()),
            ColumnarValue::Array(_) => Err(BuiltinError::InvalidColumnarValue(idx)),
            _ => Err(BuiltinError::IncorrectType(
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
    op: &dyn Fn(String) -> Result<ScalarValue, BuiltinError>,
) -> Result<ColumnarValue, BuiltinError> {
    get_nth_scalar_value(input, n, &|scalar| -> Result<ScalarValue, BuiltinError> {
        match scalar {
            ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => op(v),
            _ => Err(BuiltinError::IncorrectType(
                scalar.data_type(),
                DataType::Utf8,
            )),
        }
    })
}

fn session_var(s: &str) -> Expr {
    Expr::ScalarVariable(DataType::Utf8, vec![s.to_string()])
}
