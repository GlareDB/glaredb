use std::rc::Rc;
use std::sync::Mutex;

use datafusion::error::DataFusionError;
use jq_rs;
use memoize::memoize;

use super::*;
use crate::errors::BuiltinError;

pub struct JQ;

impl ConstBuiltinFunction for JQ {
    const NAME: &'static str = "jq";
    const DESCRIPTION: &'static str = "Returns the result of a JQ (1.6) query.";
    const EXAMPLE: &'static str = "jq(<json>, <jq>)";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;

    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            // args: <FIELD>, <QUERY>
            TypeSignature::OneOf(vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::LargeUtf8]),
                TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::LargeUtf8]),
            ]),
            Volatility::Immutable,
        ))
    }
}

impl BuiltinScalarUDF for JQ {
    fn as_expr(&self, args: Vec<Expr>) -> Expr {
        let udf = ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
            fun: Arc::new(move |input| {
                let query = compile_jq(get_nth_string_value(input, 1)?)?;
                let mut query = query.lock().unwrap();

                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    query
                        .run(get_nth_string_value(input, 0)?.as_str())
                        .map_err(|e| DataFusionError::Execution(e.to_string()))?
                        .trim_end()
                        .to_string(),
                ))))
            }),
        };
        Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
            Arc::new(udf),
            args,
        ))
    }
}

#[memoize(Capacity: 256, TimeToLive: std::time::Duration::from_secs(300))]
fn compile_jq(query: String) -> Result<Rc<Mutex<jq_rs::JqProgram>>, BuiltinError> {
    jq_rs::compile(query.as_str())
        .map(|v| Rc::new(Mutex::new(v)))
        .map_err(|e| BuiltinError::ParseError(e.to_string()))
}
