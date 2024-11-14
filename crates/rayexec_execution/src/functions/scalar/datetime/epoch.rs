use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId, TimeUnit, TimestampTypeMeta};
use rayexec_bullet::executor::builder::{ArrayBuilder, PrimitiveBuffer};
use rayexec_bullet::executor::physical_type::PhysicalI64;
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_error::Result;

use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Epoch;

impl FunctionInfo for Epoch {
    fn name(&self) -> &'static str {
        "epoch"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["epoch_s"]
    }

    fn signatures(&self) -> &[Signature] {
        &[
            // S -> Timestamp
            Signature {
                input: &[DataTypeId::Int64],
                variadic: None,
                return_type: DataTypeId::Timestamp,
            },
        ]
    }
}

impl ScalarFunction for Epoch {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(EpochImpl::<1_000_000>))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Int64 => Ok(Box::new(EpochImpl::<1_000_000>)),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochMs;

impl FunctionInfo for EpochMs {
    fn name(&self) -> &'static str {
        "epoch_ms"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            // MS -> Timestamp
            Signature {
                input: &[DataTypeId::Int64],
                variadic: None,
                return_type: DataTypeId::Timestamp,
            },
        ]
    }
}

impl ScalarFunction for EpochMs {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(EpochImpl::<1000>))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Int64 => Ok(Box::new(EpochImpl::<1000>)),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochImpl<const S: i64>;

impl<const S: i64> PlannedScalarFunction for EpochImpl<S> {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        match S {
            1_000_000 => &Epoch,
            1000 => &EpochMs,
            other => unreachable!("scale: {other}"),
        }
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Timestamp(TimestampTypeMeta {
            unit: TimeUnit::Microsecond,
        })
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let input = inputs[0];
        to_timestamp::<S>(input)
    }
}

fn to_timestamp<const S: i64>(input: &Array) -> Result<Array> {
    let builder = ArrayBuilder {
        datatype: DataType::Timestamp(TimestampTypeMeta {
            unit: TimeUnit::Microsecond,
        }),
        buffer: PrimitiveBuffer::with_len(input.logical_len()),
    };

    UnaryExecutor::execute::<PhysicalI64, _, _>(input, builder, |v, buf| {
        buf.put(&(v * S));
    })
}
