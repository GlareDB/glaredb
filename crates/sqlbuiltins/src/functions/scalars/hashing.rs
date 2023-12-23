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

pub struct PartitionResults;

impl ConstBuiltinFunction for PartitionResults {
    const NAME: &'static str = "parition_results";
    const DESCRIPTION: &'static str =
        "Returns true if the value is in the partition ID given the number of partitions.";
    const EXAMPLE: &'static str = "partition_results(<value>, <num_partitions>, <partition_id>)";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;

    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            // args: <FIELD>, <num_partitions>, <partition_id>
            TypeSignature::Exact(vec![
                DataType::LargeBinary, // arguments should downcast
                DataType::UInt64,
                DataType::UInt64,
            ]),
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
                let num_partitions = match get_nth_scalar_value(input, 1) {
                    Some(ScalarValue::UInt64(Some(val))) => val,
                    Some(val) => {
                        return Err(datafusion::error::DataFusionError::Execution(
                            format!("invalid for number of partitions '{}'", val).to_string(),
                        ))
                    }
                    None => {
                        return Err(datafusion::error::DataFusionError::Execution(
                            "must specify a number of partitions".to_string(),
                        ))
                    }
                };

                let partition_id = match get_nth_scalar_value(input, 2) {
                    Some(ScalarValue::UInt64(Some(val))) => val,
                    Some(val) => {
                        return Err(datafusion::error::DataFusionError::Execution(
                            format!("invalid for value for partition_id '{}'", val).to_string(),
                        ))
                    }
                    None => {
                        return Err(datafusion::error::DataFusionError::Execution(
                            "must specify a number of partitions".to_string(),
                        ))
                    }
                };

                if partition_id >= num_partitions {
                    return Err(datafusion::error::DataFusionError::Execution(
                        format!(
                            "partition_id {} must be less than num_partitions {}",
                            partition_id, num_partitions
                        )
                        .to_string(),
                    ));
                }

                // hash at the end once the other arguments are
                // validated because the hashing is potentially the
                // expensive part
                let hashed_value = match get_nth_scalar_value(input, 0) {
                    Some(value) => {
                        let mut hasher = FnvHasher::default();

                        value.hash(&mut hasher);

                        hasher.finish()
                    }
                    None => {
                        return Err(datafusion::error::DataFusionError::Execution(
                            "must have exactly one value to hash".to_string(),
                        ))
                    }
                };

                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                    hashed_value % num_partitions == partition_id,
                ))))
            }),
        };
        Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
            Arc::new(udf),
            args,
        ))
    }
}
