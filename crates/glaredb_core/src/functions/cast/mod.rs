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

pub const TO_INT8_CAST_RULE: CastRule = CastRule::Implicit(191);
pub const TO_UINT8_CAST_RULE: CastRule = CastRule::Implicit(190);

pub const TO_INT16_CAST_RULE: CastRule = CastRule::Implicit(181);
pub const TO_UINT16_CAST_RULE: CastRule = CastRule::Implicit(180);

pub const TO_INT32_CAST_RULE: CastRule = CastRule::Implicit(171);
pub const TO_UINT32_CAST_RULE: CastRule = CastRule::Implicit(170);

pub const TO_INT64_CAST_RULE: CastRule = CastRule::Implicit(161);
pub const TO_UINT64_CAST_RULE: CastRule = CastRule::Implicit(160);

pub const TO_F64_CAST_RULE: CastRule = CastRule::Implicit(151);
pub const TO_F32_CAST_RULE: CastRule = CastRule::Implicit(150);

pub const TO_DECIMAL_64_CAST_RULE: CastRule = CastRule::Implicit(131);
pub const TO_DECIMAL_128_CAST_RULE: CastRule = CastRule::Implicit(130);

pub const TO_STRING_CAST_RULE: CastRule = CastRule::Implicit(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CastRule {
    /// Casting is explicit-only.
    Explicit,
    /// Casting can be implicit or explicit. The score determines which is the
    /// "best" cast to use if there are many.
    Implicit(u32),
}

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
    pub const fn new<F>(src: DataTypeId, function: &'static F, rule: CastRule) -> Self
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
