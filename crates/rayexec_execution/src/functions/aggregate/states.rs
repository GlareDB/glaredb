use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use rayexec_error::{RayexecError, Result};
use stdutil::marker::PhantomCovariant;

use crate::arrays::array::physical_type::{MutableScalarStorage, ScalarStorage};
use crate::arrays::array::Array;
use crate::arrays::executor::aggregate::{
    AggregateState,
    BinaryNonNullUpdater,
    UnaryNonNullUpdater,
};
use crate::arrays::executor::PutBuffer;

/// Logic for handling aggregate state changes.
///
/// `UnaryStateLogic` or `BinaryStateLogic` should be used if `extra` isn't
/// needed.
pub trait AggregateStateLogic {
    type State;

    /// Initialize a new state.
    fn init_state(extra: Option<&dyn Any>) -> Self::State;

    /// Update states using rows from `inputs`.
    ///
    /// Note that `states` is a slice of pointers instead of state references as
    /// we may be updating the same state for different rows (e.g. rows
    /// corresponding to the same group).
    ///
    /// Implementations must ensure there only exists a single reference per
    /// state at a time.
    fn update(
        extra: Option<&dyn Any>,
        inputs: &[&Array],
        num_rows: usize,
        states: &mut [*mut Self::State],
    ) -> Result<()>;

    /// Combine states from `src` into `dest`.
    fn combine(
        extra: Option<&dyn Any>,
        src: &mut [&mut Self::State],
        dest: &mut [&mut Self::State],
    ) -> Result<()>;

    /// Finalize `states`, writing the final output to the output array.
    fn finalize(
        extra: Option<&dyn Any>,
        states: &mut [&mut Self::State],
        output: &mut Array,
    ) -> Result<()>;
}

/// Default logic for unary aggregates.
#[derive(Debug)]
pub struct UnaryStateLogic<State, Input, Output> {
    _input: PhantomCovariant<Input>,
    _output: PhantomCovariant<Output>,
    _state: PhantomCovariant<State>,
}

impl<State, Input, Output> AggregateStateLogic for UnaryStateLogic<State, Input, Output>
where
    State: for<'a> AggregateState<&'a Input::StorageType, Output::StorageType> + Default,
    Input: ScalarStorage,
    Output: MutableScalarStorage,
{
    type State = State;

    fn init_state(_extra: Option<&dyn Any>) -> Self::State {
        Default::default()
    }

    fn update(
        _extra: Option<&dyn Any>,
        inputs: &[&Array],
        num_rows: usize,
        states: &mut [*mut Self::State],
    ) -> Result<()> {
        UnaryNonNullUpdater::update::<Input, _, _>(inputs[0], 0..num_rows, states)
    }

    fn combine(
        _extra: Option<&dyn Any>,
        src: &mut [&mut Self::State],
        dest: &mut [&mut Self::State],
    ) -> Result<()> {
        if src.len() != dest.len() {
            return Err(RayexecError::new(
                "Source and destination have different number of states",
            )
            .with_field("source", src.len())
            .with_field("dest", dest.len()));
        }

        for (src, dest) in src.iter_mut().zip(dest) {
            dest.merge(src)?;
        }

        Ok(())
    }

    fn finalize(
        _extra: Option<&dyn Any>,
        states: &mut [&mut Self::State],
        output: &mut Array,
    ) -> Result<()> {
        let buffer = &mut Output::get_addressable_mut(&mut output.data)?;
        let validity = &mut output.validity;

        for (idx, state) in states.iter_mut().enumerate() {
            state.finalize(PutBuffer::new(idx, buffer, validity))?;
        }

        Ok(())
    }
}

/// Default logic for binary aggregates.
#[derive(Debug)]
pub struct BinaryStateLogic<State, Input1, Input2, Output> {
    _input1: PhantomCovariant<Input1>,
    _input2: PhantomCovariant<Input2>,
    _output: PhantomCovariant<Output>,
    _state: PhantomCovariant<State>,
}

impl<State, Input1, Input2, Output> AggregateStateLogic
    for BinaryStateLogic<State, Input1, Input2, Output>
where
    State: for<'a> AggregateState<
            (&'a Input1::StorageType, &'a Input2::StorageType),
            Output::StorageType,
        > + Default,
    Input1: ScalarStorage,
    Input2: ScalarStorage,
    Output: MutableScalarStorage,
{
    type State = State;

    fn init_state(_extra: Option<&dyn Any>) -> Self::State {
        Default::default()
    }

    fn update(
        _extra: Option<&dyn Any>,
        inputs: &[&Array],
        num_rows: usize,
        states: &mut [*mut Self::State],
    ) -> Result<()> {
        BinaryNonNullUpdater::update::<Input1, Input2, _, _>(
            inputs[0],
            inputs[1],
            0..num_rows,
            states,
        )
    }

    fn combine(
        _extra: Option<&dyn Any>,
        src: &mut [&mut Self::State],
        dest: &mut [&mut Self::State],
    ) -> Result<()> {
        if src.len() != dest.len() {
            return Err(RayexecError::new(
                "Source and destination have different number of states",
            )
            .with_field("source", src.len())
            .with_field("dest", dest.len()));
        }

        for (src, dest) in src.iter_mut().zip(dest) {
            dest.merge(src)?;
        }

        Ok(())
    }

    fn finalize(
        _extra: Option<&dyn Any>,
        states: &mut [&mut Self::State],
        output: &mut Array,
    ) -> Result<()> {
        let buffer = &mut Output::get_addressable_mut(&mut output.data)?;
        let validity = &mut output.validity;

        for (idx, state) in states.iter_mut().enumerate() {
            state.finalize(PutBuffer::new(idx, buffer, validity))?;
        }

        Ok(())
    }
}

/// Aggregagate function implemenation holding logic for updating and finalizing
/// aggregate states.
///
/// This makes heavy of arbitrary pointers as aggregate states are stored inline
/// with their group values following a row layout.
// TODO: More comprehensive drop checking (specifically we need to drop
// arbitrarily on query errors).
// TODO: Explicit drop function.
#[derive(Debug, Clone)]
pub struct AggregateFunctionImpl {
    /// Alignment requirement of the state.
    pub state_align: usize,
    /// Size in bytes of the state (stack).
    pub state_size: usize,

    /// Extra state to use when initializing an updating states.
    ///
    /// For example, this would contain the string separate for STRING_AGG.
    pub extra: Option<Arc<dyn Any + Sync + Send>>,

    /// Initialize a new aggregate state at the given pointer.
    pub init_fn: unsafe fn(extra: Option<&dyn Any>, state: *mut u8),
    /// Update the states using the provided inputs.
    ///
    /// The 'i'th row should update the 'i'th state.
    pub update_fn: unsafe fn(
        extra: Option<&dyn Any>,
        inputs: &[&Array],
        num_rows: usize,
        states: &mut [*mut u8],
    ) -> Result<()>,
    /// Combine states.
    ///
    /// This will consume states from `src` combining them with states in
    /// `dest`.
    ///
    /// State objects in `src` will be dropped.
    pub combine_fn:
        unsafe fn(extra: Option<&dyn Any>, src: &mut [*mut u8], dest: &mut [*mut u8]) -> Result<()>,
    /// Finalize the given states writing the outputs to `output`.
    ///
    /// This is guaranteed to be called exactly once in the aggregate operators.
    ///
    /// This will also Drop the state object to ensure cleanup.
    pub finalize_fn: unsafe fn(
        extra: Option<&dyn Any>,
        states: &mut [*mut u8],
        output: &mut Array,
    ) -> Result<()>,
}

impl AggregateFunctionImpl {
    pub fn new<Agg>(extra: Option<Arc<dyn Any + Sync + Send>>) -> Self
    where
        Agg: AggregateStateLogic,
    {
        let init_fn = |extra: Option<&dyn Any>, state_ptr: *mut u8| {
            let state = Agg::init_state(extra);
            unsafe { state_ptr.cast::<Agg::State>().write(state) };
        };

        let update_fn = |extra: Option<&dyn Any>,
                         inputs: &[&Array],
                         num_rows: usize,
                         states: &mut [*mut u8]| {
            let states: &mut [*mut Agg::State] = unsafe {
                std::slice::from_raw_parts_mut(
                    states.as_mut_ptr() as *mut *mut Agg::State,
                    states.len(),
                )
            };
            Agg::update(extra, inputs, num_rows, states)
        };

        let combine_fn = |extra: Option<&dyn Any>,
                          src_ptrs: &mut [*mut u8],
                          dest_ptrs: &mut [*mut u8]|
         -> Result<()> {
            if src_ptrs.len() != dest_ptrs.len() {
                return Err(
                    RayexecError::new("Different lengths with src and dest ptrs")
                        .with_field("src", src_ptrs.len())
                        .with_field("dest", dest_ptrs.len()),
                );
            }

            let src: &mut [&mut Agg::State] = unsafe {
                std::slice::from_raw_parts_mut(
                    src_ptrs.as_mut_ptr() as *mut &mut Agg::State,
                    src_ptrs.len(),
                )
            };

            let dest: &mut [&mut Agg::State] = unsafe {
                std::slice::from_raw_parts_mut(
                    dest_ptrs.as_mut_ptr() as *mut &mut Agg::State,
                    dest_ptrs.len(),
                )
            };

            Agg::combine(extra, src, dest)?;

            // Drop src states.
            for src_ptr in src_ptrs {
                unsafe {
                    src_ptr.drop_in_place();
                }
            }

            Ok(())
        };

        let finalize_fn =
            |extra: Option<&dyn Any>, states: &mut [*mut u8], output: &mut Array| -> Result<()> {
                let typed_states: &mut [&mut Agg::State] = unsafe {
                    std::slice::from_raw_parts_mut(
                        states.as_mut_ptr() as *mut &mut Agg::State,
                        states.len(),
                    )
                };
                Agg::finalize(extra, typed_states, output)?;

                // Drop all states, they'll never be read from again.
                for state_ptr in states {
                    unsafe {
                        state_ptr.drop_in_place();
                    }
                }

                Ok(())
            };

        AggregateFunctionImpl {
            state_align: std::mem::align_of::<Agg::State>(),
            state_size: std::mem::size_of::<Agg::State>(),
            extra,
            init_fn,
            update_fn,
            combine_fn,
            finalize_fn,
        }
    }

    /// Deref the extra state for passing into the state functions.
    pub fn extra_deref(&self) -> Option<&dyn Any> {
        self.extra.as_deref().map(|v| v as _)
    }
}
