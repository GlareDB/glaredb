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
use rayexec_error::{RayexecError, Result};
pub use sin::*;
pub use sqrt::*;
use stdutil::iter::IntoExactSizeIterator;
pub use tan::*;

use crate::arrays::array::physical_type::{
    MutablePhysicalStorage,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
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
        doc: None,
    },
    Signature {
        positional_args: &[DataTypeId::Float32],
        variadic_arg: None,
        return_type: DataTypeId::Float32,
        doc: None,
    },
    Signature {
        positional_args: &[DataTypeId::Float64],
        variadic_arg: None,
        return_type: DataTypeId::Float64,
        doc: None,
    },
];

/// Helper trait for defining math functions on floats.
pub trait UnaryInputNumericOperation: Debug + Clone + Copy + Sync + Send + 'static {
    const NAME: &'static str;
    const DESCRIPTION: &'static str;

    fn execute_float<S>(
        input: &Array,
        selection: impl IntoExactSizeIterator<Item = usize>,
        output: &mut Array,
    ) -> Result<()>
    where
        S: MutablePhysicalStorage,
        S::StorageType: Float;
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
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let input = &input.arrays()[0];

        match input.datatype() {
            DataType::Float16 => O::execute_float::<PhysicalF16>(input, sel, output),
            DataType::Float32 => O::execute_float::<PhysicalF32>(input, sel, output),
            DataType::Float64 => O::execute_float::<PhysicalF64>(input, sel, output),
            other => Err(RayexecError::new(format!(
                "Invalid physical type: {other:?}"
            ))),
        }
    }
}
