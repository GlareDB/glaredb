use crate::functions::scalar::macros::{
    primitive_binary_execute, primitive_binary_execute_no_wrap,
};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};

use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction};
use rayexec_bullet::array::{Array, Decimal128Array, Decimal64Array};
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_error::Result;
use rayexec_proto::packed::PackedDecoder;
use rayexec_proto::{packed::PackedEncoder, ProtoConv};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sub;

impl FunctionInfo for Sub {
    fn name(&self) -> &'static str {
        "-"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["sub"]
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

impl ScalarFunction for Sub {
    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        let datatype = DataType::from_proto(PackedDecoder::new(state).decode_next()?)?;
        Ok(Box::new(SubImpl { datatype }))
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
            | (DataType::Decimal64(_), DataType::Decimal64(_))
            | (DataType::Decimal128(_), DataType::Decimal128(_))
            | (DataType::Date32, DataType::Int64) => Ok(Box::new(SubImpl {
                datatype: inputs[0].clone(),
            })),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubImpl {
    datatype: DataType,
}

impl PlannedScalarFunction for SubImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Sub
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
                primitive_binary_execute!(first, second, Int8, |a, b| a - b)
            }
            (Array::Int16(first), Array::Int16(second)) => {
                primitive_binary_execute!(first, second, Int16, |a, b| a - b)
            }
            (Array::Int32(first), Array::Int32(second)) => {
                primitive_binary_execute!(first, second, Int32, |a, b| a - b)
            }
            (Array::Int64(first), Array::Int64(second)) => {
                primitive_binary_execute!(first, second, Int64, |a, b| a - b)
            }
            (Array::UInt8(first), Array::UInt8(second)) => {
                primitive_binary_execute!(first, second, UInt8, |a, b| a - b)
            }
            (Array::UInt16(first), Array::UInt16(second)) => {
                primitive_binary_execute!(first, second, UInt16, |a, b| a - b)
            }
            (Array::UInt32(first), Array::UInt32(second)) => {
                primitive_binary_execute!(first, second, UInt32, |a, b| a - b)
            }
            (Array::UInt64(first), Array::UInt64(second)) => {
                primitive_binary_execute!(first, second, UInt64, |a, b| a - b)
            }
            (Array::Float32(first), Array::Float32(second)) => {
                primitive_binary_execute!(first, second, Float32, |a, b| a - b)
            }
            (Array::Float64(first), Array::Float64(second)) => {
                primitive_binary_execute!(first, second, Float64, |a, b| a - b)
            }
            (Array::Decimal64(first), Array::Decimal64(second)) => {
                // TODO: Scale
                Decimal64Array::new(
                    first.precision(),
                    first.scale(),
                    primitive_binary_execute_no_wrap!(
                        first.get_primitive(),
                        second.get_primitive(),
                        |a, b| a - b
                    ),
                )
                .into()
            }
            (Array::Decimal128(first), Array::Decimal128(second)) => {
                // TODO: Scale
                Decimal128Array::new(
                    first.precision(),
                    first.scale(),
                    primitive_binary_execute_no_wrap!(
                        first.get_primitive(),
                        second.get_primitive(),
                        |a, b| a - b
                    ),
                )
                .into()
            }
            (Array::Date32(first), Array::Int64(second)) => {
                // Date32 is stored as "days", so just sub the values.
                primitive_binary_execute!(first, second, Date32, |a, b| a - b as i32)
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
    fn sub_i32() {
        let a = Arc::new(Array::Int32(Int32Array::from_iter([4, 5, 6])));
        let b = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));

        let specialized = Sub
            .plan_from_datatypes(&[DataType::Int32, DataType::Int32])
            .unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();
        let expected = Array::Int32(Int32Array::from_iter([3, 3, 3]));

        assert_eq!(expected, out);
    }
}
