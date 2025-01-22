use core::fmt;
use std::any::Any;
use std::fmt::Debug;

use rayexec_error::{RayexecError, Result};
use stdutil::marker::PhantomCovariant;

use crate::arrays::array::physical_type::{MutablePhysicalStorage, PhysicalStorage};
use crate::arrays::array::selection::Selection;
use crate::arrays::array::Array;
use crate::arrays::executor::aggregate::{
    AggregateState,
    BinaryNonNullUpdater,
    StateCombiner,
    UnaryNonNullUpdater,
};
use crate::arrays::executor::PutBuffer;
use crate::arrays::storage::{AddressableStorage, PrimitiveStorage};

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
    S: MutablePhysicalStorage,
    State: AggregateState<I, S::StorageType>,
{
    let next = output.next_mut();
    let buffer = &mut S::get_addressable_mut(next.data.try_as_mut()?)?;
    let validity = &mut next.validity;

    for (idx, state) in states.iter_mut().enumerate() {
        state.finalize(PutBuffer {
            idx,
            buffer,
            validity,
        })?;
    }

    Ok(())
}

pub fn unary_update<Storage, Output, State>(
    arrays: &[&Array],
    selection: Selection,
    mapping: &[usize],
    states: &mut [State],
) -> Result<()>
where
    Storage: PhysicalStorage,
    Output: MutablePhysicalStorage,
    State: for<'a> AggregateState<&'a Storage::StorageType, Output::StorageType>,
{
    UnaryNonNullUpdater::update::<Storage, State, _>(
        arrays[0],
        selection,
        mapping.iter().copied(),
        states,
    )
}

pub fn binary_update<Storage1, Storage2, Output, State>(
    arrays: &[&Array],
    selection: Selection,
    mapping: &[usize],
    states: &mut [State],
) -> Result<()>
where
    Storage1: PhysicalStorage,
    Storage2: PhysicalStorage,
    Output: MutablePhysicalStorage,
    State: for<'a> AggregateState<
        (&'a Storage1::StorageType, &'a Storage2::StorageType),
        Output::StorageType,
    >,
{
    BinaryNonNullUpdater::update::<Storage1, Storage2, State, _>(
        arrays[0],
        arrays[1],
        selection,
        mapping.iter().copied(),
        states,
    )
}
