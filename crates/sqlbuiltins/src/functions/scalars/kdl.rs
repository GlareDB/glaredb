use ::kdl::{KdlDocument, KdlNode, KdlQuery};
use memoize::memoize;

use super::*;

use crate::errors::BuiltinError;

pub struct KDLSelect;

impl ConstBuiltinFunction for KDLSelect {
    const NAME: &'static str = "kdl_select";
    const DESCRIPTION: &'static str = "Select nodes from a KDL document";
    const EXAMPLE: &'static str = "kdl_select(docs, '[age=120]')";
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

impl BuiltinScalarUDF for KDLSelect {
    fn as_expr(&self, args: Vec<Expr>) -> Expr {
        let udf = ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
            fun: Arc::new(move |input| {
                let filter = get_nth_string_fn_arg(input, 1)?;

                get_nth_string_value(
                    input,
                    0,
                    &|value: String| -> Result<ScalarValue, BuiltinError> {
                        let sdoc: kdl::KdlDocument =
                            value.parse().map_err(BuiltinError::KdlError)?;

                        let out: Vec<&KdlNode> = sdoc
                            .query_all(compile_kdl_query(filter.clone())?)
                            .map_err(BuiltinError::KdlError)
                            .map(|iter| iter.collect())?;

                        let mut doc = sdoc.clone();
                        let elems = doc.nodes_mut();
                        elems.clear();
                        for item in &out {
                            elems.push(item.to_owned().clone())
                        }

                        // TODO: consider if we should always return LargeUtf8?
                        // could end up with truncation (or an error) the document
                        // is too long and we write the data to a table that is
                        // established (and mostly) shorter values.
                        Ok(ScalarValue::Utf8(Some(doc.to_string())))
                    },
                )
                .map_err(DataFusionError::from)
            }),
        };
        Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
            Arc::new(udf),
            args,
        ))
    }
}

pub struct KDLMatches;

impl ConstBuiltinFunction for KDLMatches {
    const NAME: &'static str = "kdl_matches";
    const DESCRIPTION: &'static str =
        "Returns a predicate indicating if a KDL document matches a KDL query";
    const EXAMPLE: &'static str = "kdl_matches(docs, '[b=100]')";
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

impl BuiltinScalarUDF for KDLMatches {
    fn as_expr(&self, args: Vec<Expr>) -> Expr {
        let udf = ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Boolean))),
            fun: Arc::new(move |input| {
                let filter = get_nth_string_fn_arg(input, 1)?;

                get_nth_string_value(
                    input,
                    0,
                    &|value: String| -> Result<ScalarValue, BuiltinError> {
                        let doc: kdl::KdlDocument =
                            value.parse().map_err(BuiltinError::KdlError)?;

                        Ok(ScalarValue::Boolean(Some(
                            doc.query(compile_kdl_query(filter.clone())?)
                                .map(|v| v.is_some())
                                .map_err(BuiltinError::KdlError)?,
                        )))
                    },
                )
                .map_err(DataFusionError::from)
            }),
        };
        Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
            Arc::new(udf),
            args,
        ))
    }
}

#[memoize(Capacity: 256)]
fn compile_kdl_query(query: String) -> Result<KdlQuery, BuiltinError> {
    query.parse().map_err(BuiltinError::KdlError)
}
