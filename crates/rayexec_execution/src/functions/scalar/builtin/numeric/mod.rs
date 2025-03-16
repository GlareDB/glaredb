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
use rayexec_error::Result;
pub use sin::*;
pub use sqrt::*;
pub use tan::*;

use crate::arrays::array::physical_type::MutableScalarStorage;
use crate::arrays::array::Array;
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
