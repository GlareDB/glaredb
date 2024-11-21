mod abs;
mod acos;
mod asin;
mod atan;
mod cbrt;
mod ceil;
mod cos;
mod exp;
mod floor;
mod isnan;
mod ln;
mod log;
mod sin;
mod sqrt;
mod tan;
use std::fmt::Debug;
use std::marker::PhantomData;

pub use abs::*;
pub use acos::*;
pub use asin::*;
pub use atan::*;
pub use cbrt::*;
pub use ceil::*;
pub use cos::*;
pub use exp::*;
pub use floor::*;
pub use isnan::*;
pub use ln::*;
pub use log::*;
use num_traits::Float;
use rayexec_bullet::array::{Array, ArrayData};
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::physical_type::{
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalStorage,
    PhysicalType,
};
use rayexec_bullet::storage::PrimitiveStorage;
use rayexec_error::Result;
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use rayexec_proto::ProtoConv;
pub use sin::*;
pub use sqrt::*;
pub use tan::*;

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

/// Helper trait for defining math functions on floats.
pub trait UnaryInputNumericOperation: Debug + Clone + Copy + Sync + Send + 'static {
    const NAME: &'static str;
    const DESCRIPTION: &'static str;

    fn execute_float<'a, S>(input: &'a Array, ret: DataType) -> Result<Array>
    where
        S: PhysicalStorage<'a>,
        S::Type: Float + Default,
        ArrayData: From<PrimitiveStorage<S::Type>>;
}

/// Helper struct for creating functions that accept and produce a single
/// numeric argument.
#[derive(Debug, Clone, Copy)]
pub struct UnaryInputNumericScalar<O: UnaryInputNumericOperation> {
    _op: PhantomData<O>,
}

impl<O: UnaryInputNumericOperation> UnaryInputNumericScalar<O> {
    pub const fn new() -> Self {
        UnaryInputNumericScalar { _op: PhantomData }
    }
}

impl<O: UnaryInputNumericOperation> FunctionInfo for UnaryInputNumericScalar<O> {
    fn name(&self) -> &'static str {
        O::NAME
    }

    fn description(&self) -> &'static str {
        O::DESCRIPTION
    }

    fn signatures(&self) -> &[Signature] {
        UNARY_NUMERIC_INPUT_OUTPUT_SIGS
    }
}

impl<O: UnaryInputNumericOperation> ScalarFunction for UnaryInputNumericScalar<O> {
    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        let ret = DataType::from_proto(PackedDecoder::new(state).decode_next()?)?;
        Ok(Box::new(UnaryInputNumericScalarImpl::<O> {
            ret,
            scalar: *self,
            _op: PhantomData,
        }))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        check_is_unary_numeric_input(self, inputs)?;
        Ok(Box::new(UnaryInputNumericScalarImpl {
            scalar: *self,
            ret: inputs[0].clone(),
            _op: PhantomData,
        }))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct UnaryInputNumericScalarImpl<O: UnaryInputNumericOperation> {
    ret: DataType,
    scalar: UnaryInputNumericScalar<O>,
    _op: PhantomData<O>,
}

impl<O: UnaryInputNumericOperation> PlannedScalarFunction for UnaryInputNumericScalarImpl<O> {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &self.scalar
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        PackedEncoder::new(state).encode_next(&self.ret.to_proto()?)
    }

    fn return_type(&self) -> DataType {
        self.ret.clone()
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let input = inputs[0];
        match input.physical_type() {
            PhysicalType::Float16 => O::execute_float::<PhysicalF16>(input, self.ret.clone()),
            PhysicalType::Float32 => O::execute_float::<PhysicalF32>(input, self.ret.clone()),
            PhysicalType::Float64 => O::execute_float::<PhysicalF64>(input, self.ret.clone()),
            other => Err(unhandled_physical_types_err(self, [other])),
        }
    }
}
