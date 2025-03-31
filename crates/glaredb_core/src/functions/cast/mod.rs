pub mod array;
pub mod behavior;
pub mod builtin;
pub mod format;
pub mod parse;

use std::fmt::Debug;

use behavior::CastErrorState;
use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::util::iter::IntoExactSizeIterator;

#[derive(Debug)]
pub struct CastFunctionSet {
    /// Name of the cast function.
    pub name: &'static str,
    /// The functions part of this cast.
    pub functions: &'static [RawCastFunction],
}

#[derive(Debug, Clone, Copy)]
pub struct RawCastFunction {}

impl RawCastFunction {
    pub const fn new<F>(src: DataTypeId, function: &'static F) -> Self
    where
        F: CastFunction,
    {
        RawCastFunction {}
    }
}

pub trait CastFunction: Copy + Debug + Sync + Send + Sized + 'static {
    type State: Sync + Send;

    /// Binds this cast function, returning any state needed to execute the
    /// cast.
    fn bind(src: &DataType, target: &DataType) -> Result<Self::State>;

    /// Cast `src` to the target type, writing the results to `out`.
    fn cast(
        state: &Self::State,
        error_state: CastErrorState,
        src: &Array,
        sel: impl IntoExactSizeIterator<Item = usize>,
        out: &mut Array,
    ) -> Result<()>;
}
