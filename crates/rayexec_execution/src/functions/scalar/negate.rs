use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, BooleanBuffer, PrimitiveBuffer};
use rayexec_bullet::executor::physical_type::{
    PhysicalBool,
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalType,
};
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_error::Result;
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};

use super::{PlannedScalarFunction, ScalarFunction};
use crate::functions::{
    invalid_input_types_error,
    plan_check_num_args,
    unhandled_physical_types_err,
    FunctionInfo,
    Signature,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Negate;

impl FunctionInfo for Negate {
    fn name(&self) -> &'static str {
        "negate"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: &[DataTypeId::Float32],
                variadic: None,
                return_type: DataTypeId::Float32,
            },
            Signature {
                input: &[DataTypeId::Float64],
                variadic: None,
                return_type: DataTypeId::Float64,
            },
            Signature {
                input: &[DataTypeId::Int8],
                variadic: None,
                return_type: DataTypeId::Int8,
            },
            Signature {
                input: &[DataTypeId::Int16],
                variadic: None,
                return_type: DataTypeId::Int16,
            },
            Signature {
                input: &[DataTypeId::Int32],
                variadic: None,
                return_type: DataTypeId::Int32,
            },
            Signature {
                input: &[DataTypeId::Int64],
                variadic: None,
                return_type: DataTypeId::Int64,
            },
            Signature {
                input: &[DataTypeId::Interval],
                variadic: None,
                return_type: DataTypeId::Interval,
            },
        ]
    }
}

impl ScalarFunction for Negate {
    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(NegateImpl {
            datatype: DataType::from_proto(PackedDecoder::new(state).decode_next()?)?,
        }))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64 => Ok(Box::new(NegateImpl {
                datatype: inputs[0].clone(),
            })),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NegateImpl {
    datatype: DataType,
}

impl PlannedScalarFunction for NegateImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Negate
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        PackedEncoder::new(state).encode_next(&self.datatype.to_proto()?)
    }

    fn return_type(&self) -> DataType {
        self.datatype.clone()
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let a = inputs[0];

        let datatype = self.datatype.clone();

        match a.physical_type() {
            PhysicalType::Int8 => UnaryExecutor::execute::<PhysicalI8, _, _>(
                a,
                ArrayBuilder {
                    datatype,
                    buffer: PrimitiveBuffer::with_len(a.logical_len()),
                },
                |a, buf| buf.put(&(-a)),
            ),
            PhysicalType::Int16 => UnaryExecutor::execute::<PhysicalI16, _, _>(
                a,
                ArrayBuilder {
                    datatype,
                    buffer: PrimitiveBuffer::with_len(a.logical_len()),
                },
                |a, buf| buf.put(&(-a)),
            ),
            PhysicalType::Int32 => UnaryExecutor::execute::<PhysicalI32, _, _>(
                a,
                ArrayBuilder {
                    datatype,
                    buffer: PrimitiveBuffer::with_len(a.logical_len()),
                },
                |a, buf| buf.put(&(-a)),
            ),
            PhysicalType::Int64 => UnaryExecutor::execute::<PhysicalI64, _, _>(
                a,
                ArrayBuilder {
                    datatype,
                    buffer: PrimitiveBuffer::with_len(a.logical_len()),
                },
                |a, buf| buf.put(&(-a)),
            ),
            PhysicalType::Int128 => UnaryExecutor::execute::<PhysicalI128, _, _>(
                a,
                ArrayBuilder {
                    datatype,
                    buffer: PrimitiveBuffer::with_len(a.logical_len()),
                },
                |a, buf| buf.put(&(-a)),
            ),
            PhysicalType::Float32 => UnaryExecutor::execute::<PhysicalF32, _, _>(
                a,
                ArrayBuilder {
                    datatype,
                    buffer: PrimitiveBuffer::with_len(a.logical_len()),
                },
                |a, buf| buf.put(&(-a)),
            ),
            PhysicalType::Float64 => UnaryExecutor::execute::<PhysicalF64, _, _>(
                a,
                ArrayBuilder {
                    datatype,
                    buffer: PrimitiveBuffer::with_len(a.logical_len()),
                },
                |a, buf| buf.put(&(-a)),
            ),
            other => Err(unhandled_physical_types_err(self, [other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Not;

impl FunctionInfo for Not {
    fn name(&self) -> &'static str {
        "not"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Boolean],
            variadic: None,
            return_type: DataTypeId::Boolean,
        }]
    }
}

impl ScalarFunction for Not {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(NotImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Boolean => Ok(Box::new(NotImpl)),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotImpl;

impl PlannedScalarFunction for NotImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Not
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        UnaryExecutor::execute::<PhysicalBool, _, _>(
            inputs[0],
            ArrayBuilder {
                datatype: DataType::Boolean,
                buffer: BooleanBuffer::with_len(inputs[0].logical_len()),
            },
            |b, buf| buf.put(&(!b)),
        )
    }
}
