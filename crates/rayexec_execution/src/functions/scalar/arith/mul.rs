use crate::functions::scalar::macros::{
    primitive_binary_execute, primitive_binary_execute_no_wrap,
};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};

use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction};
use rayexec_bullet::array::{Array, Decimal128Array, Decimal64Array};
use rayexec_bullet::datatype::{DataType, DataTypeId, DecimalTypeMeta};
use rayexec_bullet::scalar::decimal::{Decimal128Type, Decimal64Type, DecimalType};
use rayexec_bullet::scalar::interval::Interval;
use rayexec_error::Result;
use rayexec_proto::packed::PackedDecoder;
use rayexec_proto::{packed::PackedEncoder, ProtoConv};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mul;

impl FunctionInfo for Mul {
    fn name(&self) -> &'static str {
        "*"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["mul"]
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: &[DataTypeId::Float32, DataTypeId::Float32],
                variadic: None,
                return_type: DataTypeId::Float32,
            },
            Signature {
                input: &[DataTypeId::Float64, DataTypeId::Float64],
                variadic: None,
                return_type: DataTypeId::Float64,
            },
            Signature {
                input: &[DataTypeId::Int8, DataTypeId::Int8],
                variadic: None,
                return_type: DataTypeId::Int8,
            },
            Signature {
                input: &[DataTypeId::Int16, DataTypeId::Int16],
                variadic: None,
                return_type: DataTypeId::Int16,
            },
            Signature {
                input: &[DataTypeId::Int32, DataTypeId::Int32],
                variadic: None,
                return_type: DataTypeId::Int32,
            },
            Signature {
                input: &[DataTypeId::Int64, DataTypeId::Int64],
                variadic: None,
                return_type: DataTypeId::Int64,
            },
            Signature {
                input: &[DataTypeId::UInt8, DataTypeId::UInt8],
                variadic: None,
                return_type: DataTypeId::UInt8,
            },
            Signature {
                input: &[DataTypeId::UInt16, DataTypeId::UInt16],
                variadic: None,
                return_type: DataTypeId::UInt16,
            },
            Signature {
                input: &[DataTypeId::UInt32, DataTypeId::UInt32],
                variadic: None,
                return_type: DataTypeId::UInt32,
            },
            Signature {
                input: &[DataTypeId::UInt64, DataTypeId::UInt64],
                variadic: None,
                return_type: DataTypeId::UInt64,
            },
            Signature {
                input: &[DataTypeId::Date32, DataTypeId::Int64],
                variadic: None,
                return_type: DataTypeId::Date32,
            },
            Signature {
                input: &[DataTypeId::Interval, DataTypeId::Int64],
                variadic: None,
                return_type: DataTypeId::Interval,
            },
            Signature {
                input: &[DataTypeId::Decimal64, DataTypeId::Decimal64],
                variadic: None,
                return_type: DataTypeId::Decimal64,
            },
        ]
    }
}

impl ScalarFunction for Mul {
    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        let datatype = DataType::from_proto(PackedDecoder::new(state).decode_next()?)?;
        Ok(Box::new(MulImpl { datatype }))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float32, DataType::Float32)
            | (DataType::Float64, DataType::Float64)
            | (DataType::Int8, DataType::Int8)
            | (DataType::Int16, DataType::Int16)
            | (DataType::Int32, DataType::Int32)
            | (DataType::Int64, DataType::Int64)
            | (DataType::UInt8, DataType::UInt8)
            | (DataType::UInt16, DataType::UInt16)
            | (DataType::UInt32, DataType::UInt32)
            | (DataType::UInt64, DataType::UInt64)
            | (DataType::Date32, DataType::Int64)
            | (DataType::Interval, DataType::Int64) => Ok(Box::new(MulImpl {
                datatype: inputs[0].clone(),
            })),
            (DataType::Decimal64(a), DataType::Decimal64(b)) => {
                // Since we're multiplying, might as well go wide as possible.
                // Eventually we'll want to bumpt up to 128 if the precision is
                // over some threshold to be more resilient to overflows.
                let precision = Decimal64Type::MAX_PRECISION;
                let scale = a.scale + b.scale;
                Ok(Box::new(MulImpl {
                    datatype: DataType::Decimal64(DecimalTypeMeta { precision, scale }),
                }))
            }
            (DataType::Decimal128(a), DataType::Decimal128(b)) => {
                let precision = Decimal128Type::MAX_PRECISION;
                let scale = a.scale + b.scale;
                Ok(Box::new(MulImpl {
                    datatype: DataType::Decimal128(DecimalTypeMeta { precision, scale }),
                }))
            }
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MulImpl {
    datatype: DataType,
}

impl PlannedScalarFunction for MulImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Mul
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        PackedEncoder::new(state).encode_next(&self.datatype.to_proto()?)
    }

    fn return_type(&self) -> DataType {
        self.datatype.clone()
    }

    fn execute(&self, arrays: &[&Arc<Array>]) -> Result<Array> {
        let first = arrays[0];
        let second = arrays[1];
        Ok(match (first.as_ref(), second.as_ref()) {
            (Array::Int8(first), Array::Int8(second)) => {
                primitive_binary_execute!(first, second, Int8, |a, b| a * b)
            }
            (Array::Int16(first), Array::Int16(second)) => {
                primitive_binary_execute!(first, second, Int16, |a, b| a * b)
            }
            (Array::Int32(first), Array::Int32(second)) => {
                primitive_binary_execute!(first, second, Int32, |a, b| a * b)
            }
            (Array::Int64(first), Array::Int64(second)) => {
                primitive_binary_execute!(first, second, Int64, |a, b| a * b)
            }
            (Array::UInt8(first), Array::UInt8(second)) => {
                primitive_binary_execute!(first, second, UInt8, |a, b| a * b)
            }
            (Array::UInt16(first), Array::UInt16(second)) => {
                primitive_binary_execute!(first, second, UInt16, |a, b| a * b)
            }
            (Array::UInt32(first), Array::UInt32(second)) => {
                primitive_binary_execute!(first, second, UInt32, |a, b| a * b)
            }
            (Array::UInt64(first), Array::UInt64(second)) => {
                primitive_binary_execute!(first, second, UInt64, |a, b| a * b)
            }
            (Array::Float32(first), Array::Float32(second)) => {
                primitive_binary_execute!(first, second, Float32, |a, b| a * b)
            }
            (Array::Float64(first), Array::Float64(second)) => {
                primitive_binary_execute!(first, second, Float64, |a, b| a * b)
            }
            (Array::Decimal64(first), Array::Decimal64(second)) => {
                let meta = self.datatype.try_get_decimal_type_meta()?;
                Decimal64Array::new(
                    meta.precision,
                    meta.scale,
                    primitive_binary_execute_no_wrap!(
                        first.get_primitive(),
                        second.get_primitive(),
                        |a, b| {
                            a.checked_mul(b).unwrap_or(0) // TODO
                        }
                    ),
                )
                .into()
            }
            (Array::Decimal128(first), Array::Decimal128(second)) => {
                let meta = self.datatype.try_get_decimal_type_meta()?;
                Decimal128Array::new(
                    meta.precision,
                    meta.scale,
                    primitive_binary_execute_no_wrap!(
                        first.get_primitive(),
                        second.get_primitive(),
                        |a, b| {
                            a.checked_mul(b).unwrap_or(0) // TODO
                        }
                    ),
                )
                .into()
            }
            (Array::Interval(first), Array::Int64(second)) => {
                primitive_binary_execute!(first, second, Interval, |a, b| {
                    Interval {
                        months: a.months * (b as i32),
                        days: a.days * (b as i32),
                        nanos: a.nanos * b,
                    }
                })
            }
            other => panic!("unexpected array type: {other:?}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rayexec_bullet::{
        array::{Array, Int32Array},
        datatype::DataType,
    };

    use crate::functions::scalar::ScalarFunction;

    use super::*;

    #[test]
    fn mul_i32() {
        let a = Arc::new(Array::Int32(Int32Array::from_iter([4, 5, 6])));
        let b = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));

        let specialized = Mul
            .plan_from_datatypes(&[DataType::Int32, DataType::Int32])
            .unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();
        let expected = Array::Int32(Int32Array::from_iter([4, 10, 18]));

        assert_eq!(expected, out);
    }
}
