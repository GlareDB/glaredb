use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{
    ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
    TypeSignature, Volatility,
};
use std::sync::Arc;

/// Create the builtin 'version' function.
pub fn version_func() -> Arc<ScalarUDF> {
    let sig = Signature::new(TypeSignature::Exact(Vec::new()), Volatility::Immutable);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));
    let impl_fn: ScalarFunctionImplementation = Arc::new(move |_| {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            buildenv::git_tag().to_string(),
        ))))
    });
    let f = ScalarUDF::new("version", &sig, &return_type, &impl_fn);
    Arc::new(f)
}
