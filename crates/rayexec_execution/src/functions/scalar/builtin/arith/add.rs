use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_bullet::array::{Array, ArrayData};
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, PrimitiveBuffer};
use rayexec_bullet::executor::physical_type::{
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalStorage,
    PhysicalType,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
};
use rayexec_bullet::executor::scalar::BinaryExecutor;
use rayexec_bullet::storage::PrimitiveStorage;
use rayexec_error::Result;
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};

use crate::expr::Expression;
use crate::functions::scalar::{
    PlannedScalarFunction2,
    PlannedScalarFuntion,
    ScalarFunction,
    ScalarFunctionImpl,
};
use crate::functions::{
    invalid_input_types_error,
    plan_check_num_args,
    unhandled_physical_types_err,
    FunctionInfo,
    Signature,
};
use crate::logical::binder::bind_context::BindContext;
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Add;

impl FunctionInfo for Add {
    fn name(&self) -> &'static str {
        "+"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["add"]
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: &[DataTypeId::Float16, DataTypeId::Float16],
                variadic: None,
                return_type: DataTypeId::Float16,
            },
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

impl ScalarFunction for Add {
    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedScalarFunction2>> {
        let datatype = DataType::from_proto(PackedDecoder::new(state).decode_next()?)?;
        Ok(Box::new(AddImpl2 { datatype }))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction2>> {
        plan_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float16, DataType::Float16)
            |(DataType::Float32, DataType::Float32)
            | (DataType::Float64, DataType::Float64)
            | (DataType::Int8, DataType::Int8)
            | (DataType::Int16, DataType::Int16)
            | (DataType::Int32, DataType::Int32)
            | (DataType::Int64, DataType::Int64)
            | (DataType::UInt8, DataType::UInt8)
            | (DataType::UInt16, DataType::UInt16)
            | (DataType::UInt32, DataType::UInt32)
            | (DataType::UInt64, DataType::UInt64)
            | (DataType::Decimal64(_), DataType::Decimal64(_)) // TODO: Split out decimal
            | (DataType::Decimal128(_), DataType::Decimal128(_))
            | (DataType::Date32, DataType::Int64) => Ok(Box::new(AddImpl2 {
                datatype: inputs[0].clone(),
            })),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }

    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFuntion> {
        let function_impl = match (
            inputs[0].datatype(table_list)?,
            inputs[1].datatype(table_list)?,
        ) {
            (DataType::Float16, DataType::Float16) => {
                AddImpl::<PhysicalF16>::new(DataType::Float16)
            }
            _ => unimplemented!(),
        };

        unimplemented!()
    }
}

#[derive(Debug, Clone)]
pub struct AddImpl<S> {
    datatype: DataType,
    _s: PhantomData<S>,
}

impl<S> AddImpl<S> {
    fn new(datatype: DataType) -> Self {
        AddImpl {
            datatype,
            _s: PhantomData,
        }
    }
}

impl<S> ScalarFunctionImpl for AddImpl<S>
where
    S: PhysicalStorage,
    for<'a> S::Type<'a>: std::ops::Add<Output = S::Type<'static>> + Default + Copy,
    ArrayData: From<PrimitiveStorage<S::Type<'static>>>,
{
    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let a = inputs[0];
        let b = inputs[1];

        let builder = ArrayBuilder {
            datatype: self.datatype.clone(),
            buffer: PrimitiveBuffer::with_len(a.logical_len()),
        };

        BinaryExecutor::execute::<S, S, _, _>(a, b, builder, |a, b, buf| buf.put(&(a + b)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddImpl2 {
    datatype: DataType,
}

impl PlannedScalarFunction2 for AddImpl2 {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Add
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        PackedEncoder::new(state).encode_next(&self.datatype.to_proto()?)
    }

    fn return_type(&self) -> DataType {
        self.datatype.clone()
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let a = inputs[0];
        let b = inputs[1];

        let datatype = self.datatype.clone();

        // Special cases.
        match (a.datatype(), b.datatype()) {
            (DataType::Date32, DataType::Int32) => {
                // Date32 is stored as "days", so just add the values.
                return BinaryExecutor::execute::<PhysicalI32, PhysicalI32, _, _>(
                    a,
                    b,
                    ArrayBuilder {
                        datatype,
                        buffer: PrimitiveBuffer::with_len(a.logical_len()),
                    },
                    |a, b, buf| buf.put(&(a + b)),
                );
            }
            (DataType::Date32, DataType::Int64) => {
                // Same as above.
                return BinaryExecutor::execute::<PhysicalI32, PhysicalI64, _, _>(
                    a,
                    b,
                    ArrayBuilder {
                        datatype,
                        buffer: PrimitiveBuffer::with_len(a.logical_len()),
                    },
                    |a, b, buf| buf.put(&(a + b as i32)),
                );
            }
            _ => (),
        }

        match (a.physical_type(), b.physical_type()) {
            (PhysicalType::Int8, PhysicalType::Int8) => {
                BinaryExecutor::execute::<PhysicalI8, PhysicalI8, _, _>(
                    a,
                    b,
                    ArrayBuilder {
                        datatype,
                        buffer: PrimitiveBuffer::with_len(a.logical_len()),
                    },
                    |a, b, buf| buf.put(&(a + b)),
                )
            }
            (PhysicalType::Int16, PhysicalType::Int16) => {
                BinaryExecutor::execute::<PhysicalI16, PhysicalI16, _, _>(
                    a,
                    b,
                    ArrayBuilder {
                        datatype,
                        buffer: PrimitiveBuffer::with_len(a.logical_len()),
                    },
                    |a, b, buf| buf.put(&(a + b)),
                )
            }
            (PhysicalType::Int32, PhysicalType::Int32) => {
                BinaryExecutor::execute::<PhysicalI32, PhysicalI32, _, _>(
                    a,
                    b,
                    ArrayBuilder {
                        datatype,
                        buffer: PrimitiveBuffer::with_len(a.logical_len()),
                    },
                    |a, b, buf| buf.put(&(a + b)),
                )
            }
            (PhysicalType::Int64, PhysicalType::Int64) => {
                BinaryExecutor::execute::<PhysicalI64, PhysicalI64, _, _>(
                    a,
                    b,
                    ArrayBuilder {
                        datatype,
                        buffer: PrimitiveBuffer::with_len(a.logical_len()),
                    },
                    |a, b, buf| buf.put(&(a + b)),
                )
            }
            (PhysicalType::Int128, PhysicalType::Int128) => {
                BinaryExecutor::execute::<PhysicalI128, PhysicalI128, _, _>(
                    a,
                    b,
                    ArrayBuilder {
                        datatype,
                        buffer: PrimitiveBuffer::with_len(a.logical_len()),
                    },
                    |a, b, buf| buf.put(&(a + b)),
                )
            }

            (PhysicalType::UInt8, PhysicalType::UInt8) => {
                BinaryExecutor::execute::<PhysicalU8, PhysicalU8, _, _>(
                    a,
                    b,
                    ArrayBuilder {
                        datatype,
                        buffer: PrimitiveBuffer::with_len(a.logical_len()),
                    },
                    |a, b, buf| buf.put(&(a + b)),
                )
            }
            (PhysicalType::UInt16, PhysicalType::UInt16) => {
                BinaryExecutor::execute::<PhysicalU16, PhysicalU16, _, _>(
                    a,
                    b,
                    ArrayBuilder {
                        datatype,
                        buffer: PrimitiveBuffer::with_len(a.logical_len()),
                    },
                    |a, b, buf| buf.put(&(a + b)),
                )
            }
            (PhysicalType::UInt32, PhysicalType::UInt32) => {
                BinaryExecutor::execute::<PhysicalU32, PhysicalU32, _, _>(
                    a,
                    b,
                    ArrayBuilder {
                        datatype,
                        buffer: PrimitiveBuffer::with_len(a.logical_len()),
                    },
                    |a, b, buf| buf.put(&(a + b)),
                )
            }
            (PhysicalType::UInt64, PhysicalType::UInt64) => {
                BinaryExecutor::execute::<PhysicalU64, PhysicalU64, _, _>(
                    a,
                    b,
                    ArrayBuilder {
                        datatype,
                        buffer: PrimitiveBuffer::with_len(a.logical_len()),
                    },
                    |a, b, buf| buf.put(&(a + b)),
                )
            }
            (PhysicalType::UInt128, PhysicalType::UInt128) => {
                BinaryExecutor::execute::<PhysicalU128, PhysicalU128, _, _>(
                    a,
                    b,
                    ArrayBuilder {
                        datatype,
                        buffer: PrimitiveBuffer::with_len(a.logical_len()),
                    },
                    |a, b, buf| buf.put(&(a + b)),
                )
            }
            (PhysicalType::Float16, PhysicalType::Float16) => {
                BinaryExecutor::execute::<PhysicalF16, PhysicalF16, _, _>(
                    a,
                    b,
                    ArrayBuilder {
                        datatype,
                        buffer: PrimitiveBuffer::with_len(a.logical_len()),
                    },
                    |a, b, buf| buf.put(&(a + b)),
                )
            }
            (PhysicalType::Float32, PhysicalType::Float32) => {
                BinaryExecutor::execute::<PhysicalF32, PhysicalF32, _, _>(
                    a,
                    b,
                    ArrayBuilder {
                        datatype,
                        buffer: PrimitiveBuffer::with_len(a.logical_len()),
                    },
                    |a, b, buf| buf.put(&(a + b)),
                )
            }
            (PhysicalType::Float64, PhysicalType::Float64) => {
                BinaryExecutor::execute::<PhysicalF64, PhysicalF64, _, _>(
                    a,
                    b,
                    ArrayBuilder {
                        datatype,
                        buffer: PrimitiveBuffer::with_len(a.logical_len()),
                    },
                    |a, b, buf| buf.put(&(a + b)),
                )
            }

            (a, b) => Err(unhandled_physical_types_err(self, [a, b])),
        }
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::datatype::DataType;

    use super::*;
    use crate::functions::scalar::ScalarFunction;

    #[test]
    fn add_i32() {
        let a = Array::from_iter([1, 2, 3]);
        let b = Array::from_iter([4, 5, 6]);

        let specialized = Add
            .plan_from_datatypes(&[DataType::Int32, DataType::Int32])
            .unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();
        let expected = Array::from_iter([5, 7, 9]);

        assert_eq!(expected, out);
    }
}
