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
//
// Need to update "to utf8" casts to be more restrictive. Currently it applies
// to all functions, so things like `select 'hello' || 1` work, but incidentally
// the implicit cast also allows `select repeat(2, 4)` to also work, which may
// be unexpected.

// Implicit rule constants.
//
// Higher the score, the more preferred that cast is.

#[derive(Debug)]
pub struct ImplicitCastScores {
    pub i8: u32,
    pub i16: u32,
    pub i32: u32,
    pub i64: u32,
    pub u8: u32,
    pub u16: u32,
    pub u32: u32,
    pub u64: u32,
    pub f16: u32,
    pub f32: u32,
    pub f64: u32,
    pub utf8: u32,
    pub bool: u32,
    pub interval: u32,
    pub date32: u32,
    pub decimal64: u32,
    pub decimal128: u32,
}

pub const DEFAULT_IMPLICIT_CAST_SCORES: ImplicitCastScores = ImplicitCastScores {
    // Prefer casting to i32 or i64 first.
    i32: 191,
    i64: 190,

    // Then floats, prefer f64 over f32.
    f64: 181,
    f32: 180,
    // Small float, don't really want to cast to this, but it prevents the error
    // 'Cannot create decimal datatype for casting from Float16 to Decimal64'
    // since we're preferring to cast to it over a decimal.
    //
    // We should fix the underlying error, but I'm not really sure how. The
    // error results from trying to find the best decimal prec/scale to use for
    // intergers, but we don't have anything similar for floats.
    f16: 179,

    // Now try some other "common" types.
    bool: 162,
    i16: 161,
    i8: 160,

    // Now try casting to unsigned ints, same ordering for signed ints.
    u32: 154,
    u64: 153,
    u16: 152,
    u8: 151,

    // Then decimals, prefer decimal64 over decimal128.
    decimal64: 141,
    decimal128: 140,

    // Then date (and timestamp... when there's implicit casts)
    interval: 132,
    date32: 131,

    // Try to string last
    utf8: 80,
};

pub const TO_INT32_CAST_RULE: CastRule = CastRule::Implicit(DEFAULT_IMPLICIT_CAST_SCORES.i32);
pub const TO_UINT32_CAST_RULE: CastRule = CastRule::Implicit(DEFAULT_IMPLICIT_CAST_SCORES.u32);
pub const TO_INT64_CAST_RULE: CastRule = CastRule::Implicit(DEFAULT_IMPLICIT_CAST_SCORES.i64);
pub const TO_UINT64_CAST_RULE: CastRule = CastRule::Implicit(DEFAULT_IMPLICIT_CAST_SCORES.u64);

pub const TO_F64_CAST_RULE: CastRule = CastRule::Implicit(DEFAULT_IMPLICIT_CAST_SCORES.f64);
pub const TO_F32_CAST_RULE: CastRule = CastRule::Implicit(DEFAULT_IMPLICIT_CAST_SCORES.f32);
pub const TO_F16_CAST_RULE: CastRule = CastRule::Implicit(DEFAULT_IMPLICIT_CAST_SCORES.f16);

pub const TO_DECIMAL64_CAST_RULE: CastRule =
    CastRule::Implicit(DEFAULT_IMPLICIT_CAST_SCORES.decimal64);
pub const TO_DECIMAL128_CAST_RULE: CastRule =
    CastRule::Implicit(DEFAULT_IMPLICIT_CAST_SCORES.decimal128);

pub const TO_BOOL_CAST_RULE: CastRule = CastRule::Implicit(DEFAULT_IMPLICIT_CAST_SCORES.bool);

pub const TO_DATE32_CAST_RULE: CastRule = CastRule::Implicit(DEFAULT_IMPLICIT_CAST_SCORES.date32);
pub const TO_INTERVAL_CAST_RULE: CastRule =
    CastRule::Implicit(DEFAULT_IMPLICIT_CAST_SCORES.interval);

pub const TO_INT16_CAST_RULE: CastRule = CastRule::Implicit(DEFAULT_IMPLICIT_CAST_SCORES.i16);
pub const TO_UINT16_CAST_RULE: CastRule = CastRule::Implicit(DEFAULT_IMPLICIT_CAST_SCORES.u16);
pub const TO_INT8_CAST_RULE: CastRule = CastRule::Implicit(DEFAULT_IMPLICIT_CAST_SCORES.i8);
pub const TO_UINT8_CAST_RULE: CastRule = CastRule::Implicit(DEFAULT_IMPLICIT_CAST_SCORES.u8);

pub const TO_STRING_CAST_RULE: CastRule = CastRule::Implicit(DEFAULT_IMPLICIT_CAST_SCORES.utf8);

/// Determines when we can apply a cast.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CastRule {
    /// Casting is explicit-only.
    ///
    /// User needs to apply the cast themselves.
    Explicit,
    /// Casting can be implicit or explicit. The score determines which is the
    /// "best" cast to use if there are many.
    ///
    /// We may apply the cast for the user in order to fit a function signature.
    Implicit(u32),
}

impl CastRule {
    pub const fn is_implicit(&self) -> bool {
        matches!(self, Self::Implicit(_))
    }
}

/// Determines if the cast is safe to use for flattening nested casts.
///
/// E.g. a cast expression like `CAST(CAST a AS INT) AS BIGINT` is safe to
/// flatten to `CAST(a AS BIGINT)`.
///
/// Not all casts are safe to flatten, even if the cast is considered implicit.
///
/// E.g. We can't turn `'123456789e-1234'::FLOAT::INT` into
/// `'123456789e-1234'::INT` directly as that string cannot be parsed as an
/// integer, even though both cast functions are implicit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CastFlatten {
    /// Cast is safe to use for flattening another cast.
    ///
    /// This should be used conservatively for cast the we know will always
    /// work, e.g. casting from an i16 to i64.
    Safe,
    /// Cast is not safe to use.
    Unsafe,
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
    pub(crate) raw: &'static RawCastFunction,
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
        self.raw.src.hash(state);
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
    pub(crate) flatten: CastFlatten,

    function: *const (),
    vtable: &'static RawCastFunctionVTable,
}

unsafe impl Send for RawCastFunction {}
unsafe impl Sync for RawCastFunction {}

impl RawCastFunction {
    pub const fn new<F>(
        src: DataTypeId,
        function: &'static F,
        rule: CastRule,
        flatten: CastFlatten,
    ) -> Self
    where
        F: CastFunction,
    {
        let function = (function as *const F).cast();
        RawCastFunction {
            function,
            src,
            rule,
            flatten,
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
