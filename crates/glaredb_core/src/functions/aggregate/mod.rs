pub mod builtin;
pub mod simple;

use std::any::Any;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use glaredb_error::Result;

use super::Signature;
use super::bind_state::{BindState, RawBindState};
use super::function_set::FnName;
use crate::arrays::array::Array;
use crate::expr::Expression;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggregateStateInfo {
    /// Size in bytes of the aggregate state (stack).
    pub size: usize,
    /// Alignment requirement of the aggregate state.
    pub align: usize,
}

#[derive(Debug, Clone)]
pub struct PlannedAggregateFunction {
    pub(crate) name: FnName,
    pub(crate) raw: &'static RawAggregateFunction,
    pub(crate) state: RawBindState,
}

impl PlannedAggregateFunction {
    pub(crate) const fn aggregate_state_info(&self) -> AggregateStateInfo {
        AggregateStateInfo {
            size: self.raw.state_size,
            align: self.raw.state_align,
        }
    }

    pub(crate) unsafe fn call_new_aggregate_state(&self, agg_state_out: *mut u8) {
        unsafe {
            (self.raw.vtable.new_aggregate_state_fn)(self.state.state_as_any(), agg_state_out)
        }
    }

    pub(crate) unsafe fn call_update(
        &self,
        inputs: &[Array],
        num_rows: usize,
        agg_states: &mut [*mut u8],
    ) -> Result<()> {
        unsafe {
            (self.raw.vtable.update_fn)(self.state.state_as_any(), inputs, num_rows, agg_states)
        }
    }

    /// Combines `src` pointers into `dest` pointers, consuming the source
    /// values.
    pub(crate) unsafe fn call_combine(
        &self,
        src: &mut [*mut u8],
        dest: &mut [*mut u8],
    ) -> Result<()> {
        unsafe { (self.raw.vtable.combine_fn)(self.state.state_as_any(), src, dest) }
    }

    pub(crate) unsafe fn call_finalize(
        &self,
        agg_states: &mut [*mut u8],
        output: &mut Array,
    ) -> Result<()> {
        unsafe { (self.raw.vtable.finalize_fn)(self.state.state_as_any(), agg_states, output) }
    }
}

/// Assumes that a function with same inputs and return type is using the same
/// function implementation.
impl PartialEq for PlannedAggregateFunction {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.state.return_type == other.state.return_type
            && self.state.inputs == other.state.inputs
    }
}

impl Eq for PlannedAggregateFunction {}

impl Hash for PlannedAggregateFunction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.state.return_type.hash(state);
        self.state.inputs.hash(state);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RawAggregateFunction {
    function: *const (),
    signature: &'static Signature,
    vtable: &'static RawAggregateFunctionVTable,
    state_align: usize,
    state_size: usize,
}

unsafe impl Send for RawAggregateFunction {}
unsafe impl Sync for RawAggregateFunction {}

impl RawAggregateFunction {
    pub const fn new<F>(sig: &'static Signature, function: &'static F) -> Self
    where
        F: AggregateFunction,
    {
        let function = (function as *const F).cast();
        RawAggregateFunction {
            function,
            signature: sig,
            vtable: F::VTABLE,
            state_size: std::mem::size_of::<F::GroupState>(),
            state_align: std::mem::align_of::<F::GroupState>(),
        }
    }

    pub fn call_bind(&self, inputs: Vec<Expression>) -> Result<RawBindState> {
        unsafe { (self.vtable.bind_fn)(self.function, inputs) }
    }

    pub fn signature(&self) -> &Signature {
        self.signature
    }
}

/// VTable for aggregate functions.
///
/// # Safety
///
/// This is expected to be used with the aggregate collection where state are
/// written inline according to a row layout.
///
/// This collection ensures pointers are aligned.
#[derive(Debug, Clone, Copy)]
#[allow(clippy::type_complexity)]
pub struct RawAggregateFunctionVTable {
    bind_fn: unsafe fn(function: *const (), inputs: Vec<Expression>) -> Result<RawBindState>,
    new_aggregate_state_fn: unsafe fn(state: &dyn Any, state: *mut u8),
    update_fn: unsafe fn(
        state: &dyn Any,
        inputs: &[Array],
        num_rows: usize,
        states: &mut [*mut u8],
    ) -> Result<()>,
    combine_fn: unsafe fn(state: &dyn Any, src: &mut [*mut u8], dest: &mut [*mut u8]) -> Result<()>,
    finalize_fn:
        unsafe fn(state: &dyn Any, states: &mut [*mut u8], output: &mut Array) -> Result<()>,
}

// TODO: State naming.
pub trait AggregateFunction: Debug + Copy + Sync + Send + Sized + 'static {
    /// Bind state passed to update, combine, and finalize functions.
    type BindState: Sync + Send;

    /// The type for aggregate values for a single group.
    type GroupState: Sync + Send;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>>;

    /// Compute the return type from the expression inputs and return a function
    /// state.
    ///
    /// This will only be called with expressions that match the signature this
    /// function was registered with.
    fn new_aggregate_state(state: &Self::BindState) -> Self::GroupState;

    /// Update states using rows from `inputs`.
    ///
    /// Note that `states` is a slice of pointers instead of state references as
    /// we may be updating the same state for different rows (e.g. rows
    /// corresponding to the same group).
    ///
    /// Implementations must ensure there only exists a single reference per
    /// state at a time.
    fn update(
        state: &Self::BindState,
        inputs: &[Array],
        num_rows: usize,
        states: &mut [*mut Self::GroupState],
    ) -> Result<()>;

    /// Combine states from `src` into `dest`.
    fn combine(
        state: &Self::BindState,
        src: &mut [&mut Self::GroupState],
        dest: &mut [&mut Self::GroupState],
    ) -> Result<()>;

    /// Finalize `states`, writing the final output to the output array.
    fn finalize(
        state: &Self::BindState,
        states: &mut [&mut Self::GroupState],
        output: &mut Array,
    ) -> Result<()>;
}

// TODO: More comprehensive drop checking (specifically we need to drop
// arbitrarily on query errors).
// TODO: Explicit drop function.
trait AggregateFunctionVTable: AggregateFunction {
    const VTABLE: &'static RawAggregateFunctionVTable = &RawAggregateFunctionVTable {
        bind_fn: |function: *const (), inputs: Vec<Expression>| -> Result<RawBindState> {
            let function = unsafe { function.cast::<Self>().as_ref().unwrap() };
            let state = function.bind(inputs)?;

            Ok(RawBindState {
                state: Arc::new(state.state),
                return_type: state.return_type,
                inputs: state.inputs,
            })
        },
        new_aggregate_state_fn: |state, out_ptr| {
            let state = state.downcast_ref::<Self::BindState>().unwrap();
            let agg_state = Self::new_aggregate_state(state);
            unsafe { out_ptr.cast::<Self::GroupState>().write(agg_state) };
        },
        update_fn: |state, inputs, num_rows, states| {
            let state = state.downcast_ref::<Self::BindState>().unwrap();
            let agg_states: &mut [*mut Self::GroupState] = unsafe {
                std::slice::from_raw_parts_mut(
                    states.as_mut_ptr() as *mut *mut Self::GroupState,
                    states.len(),
                )
            };
            Self::update(state, inputs, num_rows, agg_states)
        },
        combine_fn: |state, src_ptrs, dest_ptrs| {
            let state = state.downcast_ref::<Self::BindState>().unwrap();
            let src: &mut [&mut Self::GroupState] = unsafe {
                std::slice::from_raw_parts_mut(
                    src_ptrs.as_mut_ptr() as *mut &mut Self::GroupState,
                    src_ptrs.len(),
                )
            };

            let dest: &mut [&mut Self::GroupState] = unsafe {
                std::slice::from_raw_parts_mut(
                    dest_ptrs.as_mut_ptr() as *mut &mut Self::GroupState,
                    dest_ptrs.len(),
                )
            };

            Self::combine(state, src, dest)?;

            // Drop src states.
            for src_ptr in src_ptrs {
                unsafe {
                    src_ptr.drop_in_place();
                }
            }

            Ok(())
        },
        finalize_fn: |state, states, output| {
            let state = state.downcast_ref::<Self::BindState>().unwrap();
            let typed_states: &mut [&mut Self::GroupState] = unsafe {
                std::slice::from_raw_parts_mut(
                    states.as_mut_ptr() as *mut &mut Self::GroupState,
                    states.len(),
                )
            };
            Self::finalize(state, typed_states, output)?;

            // Drop all states, they'll never be read from again.
            for state_ptr in states {
                unsafe {
                    state_ptr.drop_in_place();
                }
            }

            Ok(())
        },
    };
}

impl<F> AggregateFunctionVTable for F where F: AggregateFunction {}
