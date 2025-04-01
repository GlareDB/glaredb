pub mod behavior;
pub mod builtin;
pub mod format;
pub mod parse;

use std::any::Any;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use behavior::{CastErrorState, CastFailBehavior};
use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::selection::Selection;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::util::iter::IntoExactSizeIterator;

// TODO: Need to update the implicit "from utf8" casts to be "from utf8
// literals".

// Implicit rule constants.
//
// Higher the score, the more preferred that cast is.

pub const TO_BOOL_CAST_RULE: CastRule = CastRule::Implicit(100);
pub const TO_DATE32_CAST_RULE: CastRule = CastRule::Implicit(100);
pub const TO_INTERVAL_CAST_RULE: CastRule = CastRule::Implicit(100);

pub const TO_INT8_CAST_RULE: CastRule = CastRule::Implicit(191);
pub const TO_UINT8_CAST_RULE: CastRule = CastRule::Implicit(190);

pub const TO_INT16_CAST_RULE: CastRule = CastRule::Implicit(181);
pub const TO_UINT16_CAST_RULE: CastRule = CastRule::Implicit(180);

pub const TO_INT32_CAST_RULE: CastRule = CastRule::Implicit(171);
pub const TO_UINT32_CAST_RULE: CastRule = CastRule::Implicit(170);

pub const TO_INT64_CAST_RULE: CastRule = CastRule::Implicit(161);
pub const TO_UINT64_CAST_RULE: CastRule = CastRule::Implicit(160);

pub const TO_F16_CAST_RULE: CastRule = CastRule::Implicit(152);
pub const TO_F32_CAST_RULE: CastRule = CastRule::Implicit(151);
pub const TO_F64_CAST_RULE: CastRule = CastRule::Implicit(150);

pub const TO_DECIMAL64_CAST_RULE: CastRule = CastRule::Implicit(131);
pub const TO_DECIMAL128_CAST_RULE: CastRule = CastRule::Implicit(130);

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
    /// The target type we'll be casting to.
    pub target: DataTypeId,
    /// The functions part of this cast.
    pub functions: &'static [RawCastFunction],
}

#[derive(Debug, Clone)]
pub struct PlannedCastFunction {
    pub(crate) name: &'static str,
    pub(crate) raw: RawCastFunction,
    pub(crate) state: RawCastBindState,
}

impl PlannedCastFunction {
    pub fn call_cast(&self, src: &Array, sel: Selection, target: &mut Array) -> Result<()> {
        let error_state = CastFailBehavior::Error.new_state();
        unsafe {
            (self.raw.vtable.cast_fn)(self.state.state.as_ref(), error_state, src, sel, target)
        }
    }

    pub fn call_try_cast(&self, src: &Array, sel: Selection, target: &mut Array) -> Result<()> {
        let error_state = CastFailBehavior::Null.new_state();
        unsafe {
            (self.raw.vtable.cast_fn)(self.state.state.as_ref(), error_state, src, sel, target)
        }
    }
}

/// Assumes functions with the same name and source data type id are considered
/// equal.
impl PartialEq for PlannedCastFunction {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.raw.src == other.raw.src
    }
}

impl Eq for PlannedCastFunction {}

impl Hash for PlannedCastFunction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.raw.src;
    }
}

#[derive(Debug, Clone)]
pub struct RawCastBindState {
    state: Arc<dyn Any + Sync + Send>,
}

#[derive(Debug, Clone, Copy)]
pub struct RawCastFunctionVTable {
    /// Create any state needed for the cast function.
    bind_fn: unsafe fn(
        function: *const (),
        src: &DataType,
        target: &DataType,
    ) -> Result<RawCastBindState>,
    /// Execute the cast. First argument is the state.
    cast_fn: unsafe fn(
        state: &dyn Any,
        error_state: CastErrorState,
        src: &Array,
        sel: Selection,
        out: &mut Array,
    ) -> Result<()>,
}

#[derive(Debug, Clone, Copy)]
pub struct RawCastFunction {
    pub(crate) src: DataTypeId,
    pub(crate) rule: CastRule,

    function: *const (),
    vtable: &'static RawCastFunctionVTable,
}

unsafe impl Send for RawCastFunction {}
unsafe impl Sync for RawCastFunction {}

impl RawCastFunction {
    pub const fn new<F>(src: DataTypeId, function: &'static F, rule: CastRule) -> Self
    where
        F: CastFunction,
    {
        let function = (function as *const F).cast();
        RawCastFunction {
            function,
            src,
            rule,
            vtable: F::VTABLE,
        }
    }

    pub fn call_bind(&self, src: &DataType, target: &DataType) -> Result<RawCastBindState> {
        unsafe { (self.vtable.bind_fn)(self.function, src, target) }
    }
}

pub trait CastFunction: Copy + Debug + Sync + Send + Sized + 'static {
    type State: Sync + Send;

    /// Binds this cast function, returning any state needed to execute the
    /// cast.
    fn bind(&self, src: &DataType, target: &DataType) -> Result<Self::State>;

    /// Cast `src` to the target type, writing the results to `out`.
    fn cast(
        state: &Self::State,
        error_state: CastErrorState,
        src: &Array,
        sel: impl IntoExactSizeIterator<Item = usize>,
        out: &mut Array,
    ) -> Result<()>;
}

trait CastFunctionVTable: CastFunction {
    const VTABLE: &'static RawCastFunctionVTable = &RawCastFunctionVTable {
        bind_fn: |function, src, target| -> Result<RawCastBindState> {
            let function = unsafe { function.cast::<Self>().as_ref().unwrap() };
            let state = function.bind(src, target)?;

            Ok(RawCastBindState {
                state: Arc::new(state),
            })
        },
        cast_fn: |state, error_state, src, sel, out| -> Result<()> {
            let state = state.downcast_ref::<Self::State>().unwrap();
            Self::cast(state, error_state, src, sel, out)
        },
    };
}

impl<F> CastFunctionVTable for F where F: CastFunction {}
