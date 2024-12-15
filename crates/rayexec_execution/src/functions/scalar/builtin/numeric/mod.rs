mod abs;
mod acos;
mod asin;
mod atan;
mod cbrt;
mod ceil;
mod cos;
mod degrees;
mod exp;
mod floor;
mod isnan;
mod ln;
mod log;
mod radians;
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
pub use degrees::*;
pub use exp::*;
pub use floor::*;
pub use isnan::*;
pub use ln::*;
pub use log::*;
use num_traits::Float;
pub use radians::*;
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
use rayexec_error::{RayexecError, Result};
pub use sin::*;
pub use sqrt::*;
pub use tan::*;

use crate::expr::Expression;
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

/// Signature for functions that accept a single numeric and produce a numeric.
// TODO: Include decimals.
const UNARY_NUMERIC_INPUT_OUTPUT_SIGS: &[Signature] = &[
    Signature {
        positional_args: &[DataTypeId::Float16],
        variadic_arg: None,
        return_type: DataTypeId::Float16,
    },
    Signature {
        positional_args: &[DataTypeId::Float32],
        variadic_arg: None,
        return_type: DataTypeId::Float32,
    },
    Signature {
        positional_args: &[DataTypeId::Float64],
        variadic_arg: None,
        return_type: DataTypeId::Float64,
    },
];

/// Helper trait for defining math functions on floats.
pub trait UnaryInputNumericOperation: Debug + Clone + Copy + Sync + Send + 'static {
    const NAME: &'static str;
    const DESCRIPTION: &'static str;

    fn execute_float<'a, S>(input: &'a Array, ret: DataType) -> Result<Array>
    where
        S: PhysicalStorage,
        S::Type<'a>: Float + Default,
        ArrayData: From<PrimitiveStorage<S::Type<'a>>>;
}

/// Helper struct for creating functions that accept and produce a single
/// numeric argument.
#[derive(Debug, Clone, Copy, Default)]
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
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args(self, &inputs, 1)?;
        let datatype = inputs[0].datatype(table_list)?;

        // TODO: Decimals too
        match &datatype {
            DataType::Float16 | DataType::Float32 | DataType::Float64 => (),
            other => return Err(invalid_input_types_error(self, &[other])),
        }

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: datatype.clone(),
            inputs,
            function_impl: Box::new(UnaryInputNumericScalarImpl::<O> {
                ret: datatype,
                _op: PhantomData,
            }),
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct UnaryInputNumericScalarImpl<O: UnaryInputNumericOperation> {
    ret: DataType,
    _op: PhantomData<O>,
}

impl<O: UnaryInputNumericOperation> ScalarFunctionImpl for UnaryInputNumericScalarImpl<O> {
    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let input = inputs[0];
        match input.physical_type() {
            PhysicalType::Float16 => O::execute_float::<PhysicalF16>(input, self.ret.clone()),
            PhysicalType::Float32 => O::execute_float::<PhysicalF32>(input, self.ret.clone()),
            PhysicalType::Float64 => O::execute_float::<PhysicalF64>(input, self.ret.clone()),
            other => Err(RayexecError::new(format!(
                "Invalid physical type: {other:?}"
            ))),
        }
    }
}
