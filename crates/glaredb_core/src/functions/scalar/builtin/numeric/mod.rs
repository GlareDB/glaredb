mod abs;
mod acosh;
mod acos;
mod asin;
mod asinh;
mod atan;
mod atan2;
mod atanh;
mod cbrt;
mod ceil;
mod cos;
mod cosh;
mod cot;
mod degrees;
mod exp;
mod floor;
mod isfinite;
mod isinf;
mod isnan;
mod ln;
mod log;
mod pi;
mod power;
mod radians;
mod round;
mod sin;
mod sinh;
mod sqrt;
mod tan;
mod tanh;
mod trunc;
use std::fmt::Debug;
use std::marker::PhantomData;

pub use abs::*;
pub use acosh::*;
pub use acos::*;
pub use asin::*;
pub use asinh::*;
pub use atan::*;
pub use atan2::*;
pub use atanh::*;
pub use cbrt::*;
pub use ceil::*;
pub use cos::*;
pub use cosh::*;
pub use cot::*;
pub use degrees::*;
pub use exp::*;
pub use floor::*;
use glaredb_error::Result;
pub use isfinite::*;
pub use isinf::*;
pub use isnan::*;
pub use ln::*;
pub use log::*;
use num_traits::Float;
pub use pi::*;
pub use power::*;
pub use radians::*;
pub use round::*;
pub use sin::*;
pub use sinh::*;
pub use sqrt::*;
pub use tan::*;
pub use tanh::*;
pub use trunc::*;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::MutableScalarStorage;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::expr::Expression;
use crate::functions::scalar::{BindState, ScalarFunction};
use crate::util::iter::IntoExactSizeIterator;

/// Helper trait for defining math functions on floats.
pub trait UnaryInputNumericOperation: Debug + Clone + Copy + Sync + Send + 'static {
    fn execute_float<S>(
        input: &Array,
        selection: impl IntoExactSizeIterator<Item = usize>,
        output: &mut Array,
    ) -> Result<()>
    where
        S: MutableScalarStorage,
        S::StorageType: Float;
}

/// Helper struct for creating functions that accept and produce a single
/// numeric argument.
#[derive(Debug, Clone, Copy)]
pub struct UnaryInputNumericScalar<S: MutableScalarStorage, O: UnaryInputNumericOperation> {
    return_type: &'static DataType,
    _s: PhantomData<S>,
    _op: PhantomData<O>,
}

impl<S, O> UnaryInputNumericScalar<S, O>
where
    S: MutableScalarStorage,
    O: UnaryInputNumericOperation,
{
    pub const fn new(return_type: &'static DataType) -> Self {
        UnaryInputNumericScalar {
            return_type,
            _s: PhantomData,
            _op: PhantomData,
        }
    }
}

impl<S, O> ScalarFunction for UnaryInputNumericScalar<S, O>
where
    S: MutableScalarStorage,
    S::StorageType: Float,
    O: UnaryInputNumericOperation,
{
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: self.return_type.clone(),
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let input = &input.arrays()[0];

        O::execute_float::<S>(input, sel, output)
    }
}
