pub mod df_scalars;
pub mod kdl;
pub mod postgres;
use crate::{
    document,
    functions::{BuiltinFunction, BuiltinScalarUDF, ConstBuiltinFunction},
};
use datafusion::logical_expr::BuiltinScalarFunction;
pub use df_scalars::*;
use protogen::metastore::types::catalog::FunctionType;

use std::sync::Arc;

use datafusion::{
    arrow::datatypes::{DataType, Field},
    logical_expr::{Expr, ScalarUDF, Signature, TypeSignature, Volatility},
    physical_plan::ColumnarValue,
    scalar::ScalarValue,
};

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

fn get_nth_scalar_value(input: &[ColumnarValue], n: usize) -> Option<ScalarValue> {
    match input.get(n) {
        Some(input) => match input {
            ColumnarValue::Scalar(scalar) => Some(scalar.clone()),
            ColumnarValue::Array(arr) => ScalarValue::try_from_array(arr, 0).ok(),
        },
        None => None,
    }
}

fn session_var(s: &str) -> Expr {
    Expr::ScalarVariable(DataType::Utf8, vec![s.to_string()])
}
