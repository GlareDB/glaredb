pub mod df_scalars;
pub mod kdl;
pub mod postgres;

use std::sync::Arc;

use datafusion::arrow::array::{make_array, Array, ArrayDataBuilder};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::logical_expr::BuiltinScalarFunction;
use datafusion::logical_expr::{Expr, ScalarUDF, Signature, TypeSignature, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datafusion::scalar::ScalarValue;

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

fn session_var(s: &str) -> Expr {
    Expr::ScalarVariable(DataType::Utf8, vec![s.to_string()])
}
