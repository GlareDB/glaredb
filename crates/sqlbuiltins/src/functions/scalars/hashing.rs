use std::hash::{Hash, Hasher};

use fnv::FnvHasher;
use siphasher::sip::SipHasher24;

use super::*;

pub struct SipHash;

impl ConstBuiltinFunction for SipHash {
    const NAME: &'static str = "siphash";
    const DESCRIPTION: &'static str =
        "Calculates a 64bit non-cryptographic hash (SipHash24) of the value.";
    const EXAMPLE: &'static str = "siphash(<value>)";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;

    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            // args: <FIELD>
            TypeSignature::Any(1),
            Volatility::Immutable,
        ))
    }
}
impl BuiltinScalarUDF for SipHash {
    fn as_expr(&self, args: Vec<Expr>) -> Expr {
        let udf = ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
            fun: Arc::new(move |input| match get_nth_scalar_value(input, 0) {
                Some(value) => {
                    let mut hasher = SipHasher24::new();

                    value.hash(&mut hasher);

                    Ok(ColumnarValue::Scalar(ScalarValue::UInt64(Some(
                        hasher.finish(),
                    ))))
                }
                None => {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "must have exactly one value to hash".to_string(),
                    ))
                }
            }),
        };
        Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
            Arc::new(udf),
            args,
        ))
    }
}

pub struct FnvHash;

impl ConstBuiltinFunction for FnvHash {
    const NAME: &'static str = "fnv";
    const DESCRIPTION: &'static str =
        "Calculates a 64bit non-cryptographic hash (fnv1a) of the value.";
    const EXAMPLE: &'static str = "fnv(<value>)";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;

    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            // args: <FIELD>
            TypeSignature::Any(1),
            Volatility::Immutable,
        ))
    }
}
impl BuiltinScalarUDF for FnvHash {
    fn as_expr(&self, args: Vec<Expr>) -> Expr {
        let udf = ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
            fun: Arc::new(move |input| match get_nth_scalar_value(input, 0) {
                Some(value) => {
                    let mut hasher = FnvHasher::default();

                    value.hash(&mut hasher);

                    Ok(ColumnarValue::Scalar(ScalarValue::UInt64(Some(
                        hasher.finish(),
                    ))))
                }
                None => {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "must have exactly one value to hash".to_string(),
                    ))
                }
            }),
        };
        Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
            Arc::new(udf),
            args,
        ))
    }
}
