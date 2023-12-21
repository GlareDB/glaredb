use std::sync::Arc;

use datafusion::{
    arrow::datatypes::{DataType, Field},
    logical_expr::{Expr, ScalarUDF, Signature, TypeSignature, Volatility},
    physical_plan::ColumnarValue,
    scalar::ScalarValue,
};
use jaq_interpret::{Ctx, FilterT, ParseCtx, RcIter, Val};
use protogen::metastore::types::catalog::FunctionType;
use serde_json::Value;

use crate::functions::{BuiltinScalarUDF, ConstBuiltinFunction};

use super::get_nth_scalar_value;

#[derive(Clone)]
pub struct Jq;

impl ConstBuiltinFunction for Jq {
    const NAME: &'static str = "jq";
    const DESCRIPTION: &'static str = "Run a jq query on a JSON document. 
Note: this uses the rust jaq library, which does not yet support the full jq language.";
    const EXAMPLE: &'static str = r#"jq('{"a": 1}', '.a')"#;
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;

    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            Volatility::Immutable,
        ))
    }
}

impl BuiltinScalarUDF for Jq {
    fn as_expr(&self, args: Vec<datafusion::prelude::Expr>) -> datafusion::prelude::Expr {
        let udf = ScalarUDF {
            name: "jq".to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
            fun: Arc::new(move |input| {
                let (filter, doc) = jq_parse_udf_args(input)?;

                let inputs = RcIter::new(core::iter::empty());

                // iterator over the output values
                let mut out = filter.run((Ctx::new([], &inputs), Val::from(doc)));
                // .map_err(|e| datafusion::common::DataFusionError::Execution(e.to_string()))?;
                let mut res = Vec::new();
                while let Some(Ok(val)) = out.next() {
                    res.push(val);
                }
                if res.len() == 1 {
                    let scalar = jq_to_scalar(&res[0]);

                    Ok(ColumnarValue::Scalar(scalar))
                } else {
                    let values = res.iter().map(jq_to_scalar).collect::<Vec<_>>();
                    Ok(ColumnarValue::Scalar(ScalarValue::List(
                        Some(values),
                        Arc::new(Field::new("item", DataType::Utf8, false)),
                    )))
                }
            }),
        };
        Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
            Arc::new(udf),
            args,
        ))
    }
}
fn jq_to_scalar(value: &Val) -> ScalarValue {
    match value {
        Val::Null => ScalarValue::Null,
        Val::Bool(b) => ScalarValue::Boolean(Some(*b)),
        Val::Int(n) => ScalarValue::Int64(Some(*n as i64)),
        Val::Float(n) => ScalarValue::Float64(Some(*n)),
        Val::Str(s) => ScalarValue::Utf8(Some(s.to_string())),
        // TODO: handle vec and object types
        // This requires inference or merging of types
        // so we just cast to string for now
        other => ScalarValue::Utf8(Some(other.to_string())),
    }
}

fn jq_parse_udf_args(
    args: &[ColumnarValue],
) -> datafusion::error::Result<(jaq_interpret::Filter, Value)> {
    // parse the filter first, because it's probably shorter and
    // erroring earlier would be preferable to parsing a large that we
    // don't need/want.
    let filter = match get_nth_scalar_value(args, 1) {
        Some(ScalarValue::Utf8(Some(val))) | Some(ScalarValue::LargeUtf8(Some(val))) => {
            let mut defs = ParseCtx::new(Vec::new());
            let (f, errs) = jaq_parse::parse(&val, jaq_parse::main());
            if !errs.is_empty() {
                return Err(datafusion::common::DataFusionError::Execution(
                    errs.into_iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<_>>()
                        .join("\n"),
                ));
            }
            defs.compile(f.unwrap())
        }
        Some(val) => {
            return Err(datafusion::common::DataFusionError::Execution(format!(
                "invalid type for JQ expression {}",
                val.data_type(),
            )))
        }
        None => {
            return Err(datafusion::common::DataFusionError::Execution(
                "missing JQ query".to_string(),
            ))
        }
    };

    let doc: Value = match get_nth_scalar_value(args, 0) {
        Some(ScalarValue::Utf8(Some(val))) | Some(ScalarValue::LargeUtf8(Some(val))) => {
            serde_json::from_str(&val)
                .map_err(|err| datafusion::common::DataFusionError::Execution(err.to_string()))?
        }
        Some(val) => {
            return Err(datafusion::common::DataFusionError::Execution(format!(
                "invalid type for JQ value {}",
                val.data_type(),
            )))
        }
        None => {
            return Err(datafusion::common::DataFusionError::Execution(
                "invalid field for JQ".to_string(),
            ))
        }
    };
    Ok((filter, doc))
}
