use std::hash::{Hash, Hasher};

use fnv::FnvHasher;
use siphasher::sip::SipHasher24;

use super::{
    get_nth_scalar_value, get_nth_u64_fn_arg, Arc, BuiltinError, BuiltinScalarUDF,
    ConstBuiltinFunction, DataFusionError, DataType, Expr, FunctionType, ScalarUDF, ScalarValue,
    Signature, TypeSignature, Volatility,
};

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
            return_type: Arc::new(|_| Ok(Arc::new(DataType::UInt64))),
            fun: Arc::new(move |input| {
                Ok(get_nth_scalar_value(input, 0, &|value| -> Result<
                    ScalarValue,
                    BuiltinError,
                > {
                    let mut hasher = SipHasher24::new();
                    value.hash(&mut hasher);
                    Ok(ScalarValue::UInt64(Some(hasher.finish())))
                })?)
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
            return_type: Arc::new(|_| Ok(Arc::new(DataType::UInt64))),
            fun: Arc::new(move |input| {
                Ok(get_nth_scalar_value(input, 0, &|value| -> Result<
                    ScalarValue,
                    BuiltinError,
                > {
                    let mut hasher = FnvHasher::default();
                    value.hash(&mut hasher);
                    Ok(ScalarValue::UInt64(Some(hasher.finish())))
                })?)
            }),
        };
        Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
            Arc::new(udf),
            args,
        ))
    }
}

pub struct PartitionResults;

impl ConstBuiltinFunction for PartitionResults {
    const NAME: &'static str = "partition_results";
    const DESCRIPTION: &'static str =
        "Returns true if the value is in the partition ID given the number of partitions.";
    const EXAMPLE: &'static str = "partition_results(<value>, <num_partitions>, <partition_id>)";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;

    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            // args: <FIELD>, <num_partitions>, <partition_id>
            TypeSignature::Any(3),
            Volatility::Immutable,
        ))
    }
}

impl BuiltinScalarUDF for PartitionResults {
    fn as_expr(&self, args: Vec<Expr>) -> Expr {
        let udf = ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
            fun: Arc::new(move |input| {
                if input.len() != 3 {
                    return Err(DataFusionError::Execution(
                        "must specify exactly three arguments".to_string(),
                    ));
                }

                let num_partitions = get_nth_u64_fn_arg(input, 1)?;
                let partition_id = get_nth_u64_fn_arg(input, 2)?;

                if partition_id >= num_partitions {
                    return Err(DataFusionError::Execution(
                        format!(
                            "id {} must be less than number of partitions {}",
                            partition_id, num_partitions,
                        )
                        .to_string(),
                    ));
                }

                // hash at the end once the other arguments are
                // validated because the hashing is potentially the
                // expensive part
                Ok(get_nth_scalar_value(input, 0, &|value| -> Result<
                    ScalarValue,
                    BuiltinError,
                > {
                    let mut hasher = FnvHasher::default();
                    value.hash(&mut hasher);
                    Ok(ScalarValue::Boolean(Some(
                        hasher.finish() % num_partitions == partition_id,
                    )))
                })?)
            }),
        };
        Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
            Arc::new(udf),
            args,
        ))
    }
}
