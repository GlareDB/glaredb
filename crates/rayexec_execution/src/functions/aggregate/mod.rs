pub mod builtin;
pub mod states;

use std::fmt::Debug;
use std::hash::Hash;

use dyn_clone::DynClone;
use rayexec_error::{RayexecError, Result};
use states::AggregateFunctionImpl;

use super::bind_state::{BindState, RawBindState, RawBindStateInner};
use super::{FunctionInfo, Signature};
use crate::arrays::array::Array;
use crate::arrays::datatype::DataType;
use crate::expr::Expression;
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggregateStateInfo {
    /// Size in bytes of the aggregate state (stack).
    pub size: usize,
    /// Alignment requirement of the aggregate state.
    pub align: usize,
}

#[derive(Debug, Clone)]
pub struct PlannedAggregateFunction {
    pub(crate) name: &'static str,
    pub(crate) raw: RawAggregateFunction,
    pub(crate) state: RawBindState,
}

impl PlannedAggregateFunction {
    pub const fn aggregate_state_info(&self) -> AggregateStateInfo {
        AggregateStateInfo {
            size: self.raw.state_size,
            align: self.raw.state_align,
        }
    }

    pub unsafe fn call_new_aggregate_state(&self, agg_state_out: *mut u8) {
        (self.raw.vtable.new_aggregate_state_fn)(self.state.state_ptr(), agg_state_out)
    }

    pub unsafe fn call_update(
        &self,
        inputs: &[Array],
        num_rows: usize,
        agg_states: &mut [*mut u8],
    ) -> Result<()> {
        (self.raw.vtable.update_fn)(self.state.state_ptr(), inputs, num_rows, agg_states)
    }

    pub unsafe fn call_combine(&self, src: &mut [*mut u8], dest: &mut [*mut u8]) -> Result<()> {
        (self.raw.vtable.combine_fn)(self.state.state_ptr(), src, dest)
    }

    pub unsafe fn call_finalize(
        &self,
        agg_states: &mut [*mut u8],
        output: &mut Array,
    ) -> Result<()> {
        (self.raw.vtable.finalize_fn)(self.state.state_ptr(), agg_states, output)
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
            state_size: std::mem::size_of::<F::AggregateState>(),
            state_align: std::mem::align_of::<F::AggregateState>(),
        }
    }

    pub fn call_bind(&self, inputs: Vec<Expression>) -> Result<RawBindState> {
        unsafe { (self.vtable.bind_fn)(self.function, inputs) }
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
pub struct RawAggregateFunctionVTable {
    bind_fn: unsafe fn(function: *const (), inputs: Vec<Expression>) -> Result<RawBindState>,
    new_aggregate_state_fn: unsafe fn(state: *const (), state: *mut u8),
    update_fn: unsafe fn(
        state: *const (),
        inputs: &[Array],
        num_rows: usize,
        states: &mut [*mut u8],
    ) -> Result<()>,
    combine_fn:
        unsafe fn(state: *const (), src: &mut [*mut u8], dest: &mut [*mut u8]) -> Result<()>,
    finalize_fn:
        unsafe fn(state: *const (), states: &mut [*mut u8], output: &mut Array) -> Result<()>,
}

// TODO: State naming.
pub trait AggregateFunction: Debug + Sync + Send + Sized {
    /// Bind state passed to update, combine, and finalize functions.
    type State: Sync + Send;

    /// The type of the aggregate values that get updated.
    type AggregateState: Sync + Send;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>>;

    /// Compute the return type from the expression inputs and return a function
    /// state.
    ///
    /// This will only be called with expressions that match the signature this
    /// function was registered with.
    fn new_aggregate_state(state: &Self::State) -> Self::AggregateState;

    /// Update states using rows from `inputs`.
    ///
    /// Note that `states` is a slice of pointers instead of state references as
    /// we may be updating the same state for different rows (e.g. rows
    /// corresponding to the same group).
    ///
    /// Implementations must ensure there only exists a single reference per
    /// state at a time.
    fn update(
        state: &Self::State,
        inputs: &[Array],
        num_rows: usize,
        states: &mut [*mut Self::AggregateState],
    ) -> Result<()>;

    /// Combine states from `src` into `dest`.
    fn combine(
        state: &Self::State,
        src: &mut [&mut Self::AggregateState],
        dest: &mut [&mut Self::AggregateState],
    ) -> Result<()>;

    /// Finalize `states`, writing the final output to the output array.
    fn finalize(
        state: &Self::State,
        states: &mut [&mut Self::AggregateState],
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
            let raw = RawBindStateInner::from_state(state.state);

            Ok(RawBindState {
                state: raw,
                return_type: state.return_type,
                inputs: state.inputs,
            })
        },
        new_aggregate_state_fn: |state: *const (), out_ptr: *mut u8| {
            let state = unsafe { state.cast::<Self::State>().as_ref().unwrap() };
            let agg_state = Self::new_aggregate_state(state);
            unsafe { out_ptr.cast::<Self::AggregateState>().write(agg_state) };
        },
        update_fn: |state: *const (), inputs: &[Array], num_rows: usize, states: &mut [*mut u8]| {
            let state = unsafe { state.cast::<Self::State>().as_ref().unwrap() };
            let agg_states: &mut [*mut Self::AggregateState] = unsafe {
                std::slice::from_raw_parts_mut(
                    states.as_mut_ptr() as *mut *mut Self::AggregateState,
                    states.len(),
                )
            };
            Self::update(state, inputs, num_rows, agg_states)
        },
        combine_fn: |state: *const (), src_ptrs: &mut [*mut u8], dest_ptrs: &mut [*mut u8]| {
            let state = unsafe { state.cast::<Self::State>().as_ref().unwrap() };
            if src_ptrs.len() != dest_ptrs.len() {
                return Err(
                    RayexecError::new("Different lengths with src and dest ptrs")
                        .with_field("src", src_ptrs.len())
                        .with_field("dest", dest_ptrs.len()),
                );
            }

            let src: &mut [&mut Self::AggregateState] = unsafe {
                std::slice::from_raw_parts_mut(
                    src_ptrs.as_mut_ptr() as *mut &mut Self::AggregateState,
                    src_ptrs.len(),
                )
            };

            let dest: &mut [&mut Self::AggregateState] = unsafe {
                std::slice::from_raw_parts_mut(
                    dest_ptrs.as_mut_ptr() as *mut &mut Self::AggregateState,
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
        finalize_fn: |state: *const (), states: &mut [*mut u8], output: &mut Array| {
            let state = unsafe { state.cast::<Self::State>().as_ref().unwrap() };
            let typed_states: &mut [&mut Self::AggregateState] = unsafe {
                std::slice::from_raw_parts_mut(
                    states.as_mut_ptr() as *mut &mut Self::AggregateState,
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

/// A generic aggregate function that can be specialized into a more specific
/// function depending on type.
pub trait AggregateFunction2: FunctionInfo + Debug + Sync + Send + DynClone {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction2>;
}

impl Clone for Box<dyn AggregateFunction2> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

impl PartialEq<dyn AggregateFunction2> for Box<dyn AggregateFunction2 + '_> {
    fn eq(&self, other: &dyn AggregateFunction2) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq for dyn AggregateFunction2 + '_ {
    fn eq(&self, other: &dyn AggregateFunction2) -> bool {
        self.name() == other.name() && self.signatures() == other.signatures()
    }
}

impl Eq for dyn AggregateFunction2 {}

#[derive(Debug, Clone)]
pub struct PlannedAggregateFunction2 {
    pub function: Box<dyn AggregateFunction2>,
    pub return_type: DataType,
    pub inputs: Vec<Expression>,
    pub function_impl: AggregateFunctionImpl,
}

/// Assumes that a function with same inputs and return type is using the same
/// function implementation.
impl PartialEq for PlannedAggregateFunction2 {
    fn eq(&self, other: &Self) -> bool {
        self.function == other.function
            && self.return_type == other.return_type
            && self.inputs == other.inputs
    }
}

impl Eq for PlannedAggregateFunction2 {}

impl Hash for PlannedAggregateFunction2 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.function.name().hash(state);
        self.return_type.hash(state);
        self.inputs.hash(state);
    }
}
