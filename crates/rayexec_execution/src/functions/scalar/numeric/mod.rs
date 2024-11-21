mod abs;
mod ceil;
mod floor;
use std::fmt::Debug;

pub use abs::*;
pub use ceil::*;
pub use floor::*;
use rayexec_bullet::array::{Array, ArrayData};
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{
    ArrayBuilder,
    ArrayDataBuffer,
    BooleanBuffer,
    PrimitiveBuffer,
};
use rayexec_bullet::executor::physical_type::{PhysicalF32, PhysicalF64, PhysicalType};
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_bullet::storage::{AddressableStorage, PrimitiveStorage};
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

/// Signature for functions that accept a single numeric and produce a numeric.
// TODO: Include decimals.
const UNARY_NUMERIC_INPUT_OUTPUT_SIGS: &'static [Signature] = &[
    Signature {
        input: &[DataTypeId::Float16],
        variadic: None,
        return_type: DataTypeId::Float16,
    },
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
];

/// Helper for checking if the input is a numeric type.
fn check_is_unary_numeric_input(info: &impl FunctionInfo, inputs: &[DataType]) -> Result<()> {
    plan_check_num_args(info, inputs, 1)?;
    // TODO: Decimals too
    match &inputs[0] {
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Ok(()),
        other => Err(invalid_input_types_error(info, &[other])),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsNan;

impl FunctionInfo for IsNan {
    fn name(&self) -> &'static str {
        "isnan"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: &[DataTypeId::Float32],
                variadic: None,
                return_type: DataTypeId::Boolean,
            },
            Signature {
                input: &[DataTypeId::Float64],
                variadic: None,
                return_type: DataTypeId::Boolean,
            },
        ]
    }
}

impl ScalarFunction for IsNan {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(IsNanImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Float32 | DataType::Float64 => Ok(Box::new(IsNanImpl)),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IsNanImpl;

impl PlannedScalarFunction for IsNanImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &IsNan
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let builder = ArrayBuilder {
            datatype: DataType::Boolean,
            buffer: BooleanBuffer::with_len(inputs[0].logical_len()),
        };
        match inputs[0].physical_type() {
            PhysicalType::Float32 => {
                UnaryExecutor::execute::<PhysicalF32, _, _>(inputs[0], builder, |v, buf| {
                    buf.put(&v.is_nan())
                })
            }
            PhysicalType::Float64 => {
                UnaryExecutor::execute::<PhysicalF64, _, _>(inputs[0], builder, |v, buf| {
                    buf.put(&v.is_nan())
                })
            }
            other => Err(unhandled_physical_types_err(self, [other])),
        }
    }
}
