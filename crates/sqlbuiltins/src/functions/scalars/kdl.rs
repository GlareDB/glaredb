use std::sync::Arc;

use ::kdl::{KdlNode, KdlQuery};
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
use memoize::memoize;
use protogen::metastore::types::catalog::FunctionType;

use super::{get_nth_string_fn_arg, get_nth_string_value};
use crate::errors::BuiltinError;
use crate::functions::{BuiltinScalarUDF, ConstBuiltinFunction};

#[derive(Debug)]
pub struct KDLSelect {
    signature: Signature,
}


impl ConstBuiltinFunction for KDLSelect {
    const NAME: &'static str = "kdl_select";
    const DESCRIPTION: &'static str = "Select nodes from a KDL document";
    const EXAMPLE: &'static str = "kdl_select(docs, '[age=120]')";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;

    fn signature(&self) -> Option<Signature> {
        Some(self.signature.clone())
    }
}

impl Default for KDLSelect {
    fn default() -> Self {
        Self::new()
    }
}

impl KDLSelect {
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

impl ScalarUDFImpl for KDLSelect {
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
        let filter = get_nth_string_fn_arg(input, 1)?;

        get_nth_string_value(
            input,
            0,
            &|value: &String| -> Result<ScalarValue, BuiltinError> {
                let sdoc: kdl::KdlDocument = value.parse().map_err(BuiltinError::KdlError)?;

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
    }
}


impl BuiltinScalarUDF for KDLSelect {
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
pub struct KDLMatches {
    signature: Signature,
}

impl Default for KDLMatches {
    fn default() -> Self {
        Self::new()
    }
}

impl KDLMatches {
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

impl ConstBuiltinFunction for KDLMatches {
    const NAME: &'static str = "kdl_matches";
    const DESCRIPTION: &'static str =
        "Returns a predicate indicating if a KDL document matches a KDL query";
    const EXAMPLE: &'static str = "kdl_matches(docs, '[b=100]')";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;

    fn signature(&self) -> Option<Signature> {
        Some(self.signature.clone())
    }
}

impl ScalarUDFImpl for KDLMatches {
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
        let filter = get_nth_string_fn_arg(input, 1)?;

        get_nth_string_value(
            input,
            0,
            &|value: &String| -> Result<ScalarValue, BuiltinError> {
                let doc: kdl::KdlDocument = value.parse().map_err(BuiltinError::KdlError)?;

                Ok(ScalarValue::Boolean(Some(
                    doc.query(compile_kdl_query(filter.clone())?)
                        .map(|v| v.is_some())
                        .map_err(BuiltinError::KdlError)?,
                )))
            },
        )
        .map_err(DataFusionError::from)
    }
}

impl BuiltinScalarUDF for KDLMatches {
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

#[memoize(Capacity: 256)]
fn compile_kdl_query(query: String) -> Result<KdlQuery, BuiltinError> {
    query.parse().map_err(BuiltinError::KdlError)
}
