use core::fmt;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use rayexec_error::{RayexecError, Result};
use stdutil::marker::PhantomCovariant;

use crate::arrays::array::physical_type::{MutableScalarStorage, ScalarStorage};
use crate::arrays::array::selection::Selection;
use crate::arrays::array::Array;
use crate::arrays::executor::aggregate::{AggregateState, StateCombiner, UnaryNonNullUpdater};
use crate::arrays::executor::PutBuffer;

pub trait AggregateStateLogic {
    type State;

    fn init_state(extra: Option<&dyn Any>) -> Self::State;

    fn update(
        extra: Option<&dyn Any>,
        inputs: &[&Array],
        num_rows: usize,
        states: &mut [*mut Self::State],
    ) -> Result<()>;

    fn combine(
        extra: Option<&dyn Any>,
        src: &mut [&mut Self::State],
        dest: &mut [&mut Self::State],
    ) -> Result<()>;

    fn finalize(
        extra: Option<&dyn Any>,
        states: &mut [&mut Self::State],
        output: &mut Array,
    ) -> Result<()>;
}

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

#[derive(Debug, Clone)]
pub struct AggregateFunctionImpl {
    /// Alignment requirement of the state.
    pub state_align: usize,
    /// Size in bytes of the state (stack).
    pub state_size: usize,

    /// Extra state to use when initializing an updating states.
    ///
    /// For example, this would contain the string separate for STRING_AGG.
    pub extra: Option<Arc<dyn Any>>,

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
    pub fn new<Agg>(extra: Option<Arc<dyn Any>>) -> Self
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
}

pub struct TypedAggregateGroupStates<
    State,
    Input,
    Output: ?Sized,
    StateInit,
    StateUpdate,
    StateFinalize,
> {
    /// States we're tracking for each "group".
    states: Vec<State>,

    /// Index we should start draining from. Updated after every call to
    /// `drain`.
    drain_idx: usize,

    /// State initialize function.
    state_init: StateInit,
    /// State update function.
    state_update: StateUpdate,
    /// State finalize function.
    state_finalize: StateFinalize,

    // Note that these don't use `PhantomData` since we'll want to allow unsized
    // types for the output type.
    _input: PhantomCovariant<Input>,
    _output: PhantomCovariant<Output>,
}

impl<State, Input, Output: ?Sized, StateInit, StateUpdate, StateFinalize>
    TypedAggregateGroupStates<State, Input, Output, StateInit, StateUpdate, StateFinalize>
{
    pub fn new(
        state_init: StateInit,
        state_update: StateUpdate,
        state_finalize: StateFinalize,
    ) -> Self {
        TypedAggregateGroupStates {
            states: Vec::new(),
            drain_idx: 0,
            state_init,
            state_update,
            state_finalize,
            _input: PhantomCovariant::new(),
            _output: PhantomCovariant::new(),
        }
    }
}

impl<State, Input, Output, StateInit, StateUpdate, StateFinalize> AggregateGroupStates
    for TypedAggregateGroupStates<State, Input, Output, StateInit, StateUpdate, StateFinalize>
where
    State: AggregateState<Input, Output> + Sync + Send + 'static,
    Input: Sync + Send,
    Output: Sync + Send + ?Sized,
    StateInit: Fn() -> State + Sync + Send,
    StateUpdate: Fn(&[&Array], Selection, &[usize], &mut [State]) -> Result<()> + Sync + Send,
    StateFinalize: Fn(&mut [State], &mut Array) -> Result<()> + Sync + Send,
{
    fn opaque_states_mut(&mut self) -> OpaqueStatesMut<'_> {
        debug_assert_eq!(0, self.drain_idx);
        OpaqueStatesMut(&mut self.states)
    }

    fn new_groups(&mut self, count: usize) {
        debug_assert_eq!(0, self.drain_idx);
        self.states.extend((0..count).map(|_| (self.state_init)()))
    }

    fn num_states(&self) -> usize {
        self.states.len()
    }

    fn update_group_states(
        &mut self,
        inputs: &[&Array],
        selection: Selection,
        mapping: &[usize],
    ) -> Result<()> {
        debug_assert_eq!(0, self.drain_idx);
        debug_assert_eq!(selection.len(), mapping.len());

        (self.state_update)(inputs, selection, mapping, &mut self.states)
    }

    fn combine(
        &mut self,
        consume: &mut Box<dyn AggregateGroupStates>,
        selection: Selection,
        mapping: &[usize],
    ) -> Result<()> {
        debug_assert_eq!(0, self.drain_idx);
        debug_assert_eq!(selection.len(), mapping.len());

        let consume_states = consume.opaque_states_mut().downcast::<Vec<State>>()?;

        StateCombiner::combine(
            consume_states,
            selection.iter().zip(mapping.iter().copied()),
            &mut self.states,
        )
    }

    fn drain(&mut self, output: &mut Array) -> Result<usize> {
        let num_drain = usize::min(self.states.len() - self.drain_idx, output.capacity());
        let drain_states = &mut self.states[self.drain_idx..self.drain_idx + num_drain];

        (self.state_finalize)(drain_states, output)?;
        self.drain_idx += num_drain;

        Ok(num_drain)
    }
}

impl<State, Input, Output: ?Sized, StateInit, StateUpdate, StateFinalize> fmt::Debug
    for TypedAggregateGroupStates<State, Input, Output, StateInit, StateUpdate, StateFinalize>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedAggregateGroupedStates")
            .field("num_states", &self.states.len())
            .finish_non_exhaustive()
    }
}

pub trait AggregateGroupStates: Debug + Sync + Send {
    /// Get a mutable reference to the underlying states.
    ///
    /// This may be called multiple times when combining states from multiple
    /// partitions.
    fn opaque_states_mut(&mut self) -> OpaqueStatesMut<'_>;

    /// Create `count` number of new states for new groups.
    fn new_groups(&mut self, count: usize);

    /// Returns the number of states being tracked.
    fn num_states(&self) -> usize;

    /// Updates groups states from array inputs.
    ///
    /// Selection indicates with rows from the input array to use during state
    /// updates, and `mapping` provides the state index to use for each row.
    /// Selection length and mapping array must be the same length.
    fn update_group_states(
        &mut self,
        inputs: &[&Array],
        selection: Selection,
        mapping: &[usize],
    ) -> Result<()>;

    /// Combine states from another partition into self using some mapping.
    ///
    /// Selection indices which states to use from the `consume`, and mapping
    /// indicates the target states to merge into for each selected states.
    fn combine(
        &mut self,
        consume: &mut Box<dyn AggregateGroupStates>,
        selection: Selection,
        mapping: &[usize],
    ) -> Result<()>;

    /// Finalize and drain state into `output`.
    ///
    /// Returns the number of states drained. If the number of states drained is
    /// less than the capacity of the output arrays, then draining is finished.
    fn drain(&mut self, output: &mut Array) -> Result<usize>;
}

#[derive(Debug)]
pub struct OpaqueStatesMut<'a>(pub &'a mut dyn Any);

impl<'a> OpaqueStatesMut<'a> {
    pub fn downcast<T: 'static>(self) -> Result<&'a mut T> {
        let states = self.0.downcast_mut::<T>().ok_or_else(|| {
            RayexecError::new("Attempted to combine aggregate states of different types")
        })?;

        Ok(states)
    }
}

pub fn drain<S, State, I>(states: &mut [State], output: &mut Array) -> Result<()>
where
    S: MutableScalarStorage,
    State: AggregateState<I, S::StorageType>,
{
    let buffer = &mut S::get_addressable_mut(&mut output.data)?;
    let validity = &mut output.validity;

    for (idx, state) in states.iter_mut().enumerate() {
        state.finalize(PutBuffer::new(idx, buffer, validity))?;
    }

    Ok(())
}

pub fn unary_update2<Storage, Output, State>(
    arrays: &[&Array],
    selection: Selection,
    mapping: &[usize],
    states: &mut [State],
) -> Result<()>
where
    Storage: ScalarStorage,
    Output: MutableScalarStorage,
    State: for<'a> AggregateState<&'a Storage::StorageType, Output::StorageType>,
{
    unimplemented!()
    // UnaryNonNullUpdater::update::<Storage, State, _>(
    //     arrays[0],
    //     selection,
    //     mapping.iter().copied(),
    //     states,
    // )
}

pub fn binary_update2<Storage1, Storage2, Output, State>(
    arrays: &[&Array],
    selection: Selection,
    mapping: &[usize],
    states: &mut [State],
) -> Result<()>
where
    Storage1: ScalarStorage,
    Storage2: ScalarStorage,
    Output: MutableScalarStorage,
    State: for<'a> AggregateState<
        (&'a Storage1::StorageType, &'a Storage2::StorageType),
        Output::StorageType,
    >,
{
    unimplemented!()
    // BinaryNonNullUpdater::update::<Storage1, Storage2, State, _>(
    //     arrays[0],
    //     arrays[1],
    //     selection,
    //     mapping.iter().copied(),
    //     states,
    // )
}
