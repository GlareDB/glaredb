use core::fmt;
use std::any::Any;
use std::fmt::Debug;
use std::marker::PhantomData;

use iterutil::IntoExactSizeIterator;
use rayexec_error::{RayexecError, Result};

use super::ChunkGroupAddressIter;
use crate::arrays::array::exp::Array;
use crate::arrays::array::selection::Selection;
use crate::arrays::array::{Array2, ArrayData2};
use crate::arrays::buffer::physical_type::{
    MutablePhysicalStorage,
    PhysicalBool,
    PhysicalStorage,
    PhysicalType,
};
use crate::arrays::datatype::DataType;
use crate::arrays::executor::aggregate::{
    AggregateState2,
    BinaryNonNullUpdater2,
    StateCombiner2,
    StateFinalizer,
    UnaryNonNullUpdater2,
};
use crate::arrays::executor::builder::{ArrayBuilder, BooleanBuffer, PrimitiveBuffer};
use crate::arrays::executor::physical_type::PhysicalStorage2;
use crate::arrays::executor_exp::aggregate::binary::BinaryNonNullUpdater;
use crate::arrays::executor_exp::aggregate::unary::UnaryNonNullUpdater;
use crate::arrays::executor_exp::aggregate::{AggregateState, StateCombiner};
use crate::arrays::executor_exp::PutBuffer;
use crate::arrays::storage::{AddressableStorage, PrimitiveStorage};

pub struct TypedAggregateGroupStates<State, Input, Output, StateInit, StateUpdate, StateFinalize> {
    /// States being tracked.
    states: Vec<State>,

    /// Index we should start draining from. Updated after every call to
    /// `drain`.
    drain_idx: usize,

    /// How new states are initialized.
    state_init: StateInit,
    /// How states get updated.
    state_update: StateUpdate,
    /// How to finalize states.
    state_finalize: StateFinalize,

    _input: PhantomData<Input>,
    _output: PhantomData<Output>,
}

impl<State, Input, Output, StateInit, StateUpdate, StateFinalize>
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
            _input: PhantomData,
            _output: PhantomData,
        }
    }
}

impl<State, Input, Output, StateInit, StateUpdate, StateFinalize> AggregateGroupStates
    for TypedAggregateGroupStates<State, Input, Output, StateInit, StateUpdate, StateFinalize>
where
    State: AggregateState<Input, Output> + Sync + Send + 'static,
    Input: Sync + Send,
    Output: Sync + Send,
    StateInit: Fn() -> State + Sync + Send,
    StateUpdate: Fn(&[Array], Selection, &[usize], &mut [State]) -> Result<()> + Sync + Send,
    StateFinalize: Fn(&mut [State], &mut Array) -> Result<()> + Sync + Send,
{
    fn opaque_states_mut(&mut self) -> OpaqueStatesMut<'_> {
        debug_assert_eq!(0, self.drain_idx);
        OpaqueStatesMut(&mut self.states)
    }

    fn new_states(&mut self, count: usize) {
        debug_assert_eq!(0, self.drain_idx);
        self.states.extend((0..count).map(|_| (self.state_init)()))
    }

    fn num_states(&self) -> usize {
        self.states.len()
    }

    fn update_states2(&mut self, inputs: &[&Array2], mapping: ChunkGroupAddressIter) -> Result<()> {
        unimplemented!()
    }

    fn update_states(
        &mut self,
        inputs: &[Array],
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
        mapping: ChunkGroupAddressIter,
    ) -> Result<()> {
        debug_assert_eq!(0, self.drain_idx);
        let consume_states = consume.opaque_states_mut().downcast::<Vec<State>>()?;
        StateCombiner::combine(consume_states, mapping, &mut self.states)
    }

    fn finalize2(&mut self) -> Result<Array2> {
        unimplemented!()
    }

    fn drain(&mut self, output: &mut Array) -> Result<usize> {
        let num_drain = usize::min(self.states.len() - self.drain_idx, output.capacity());
        let drain_states = &mut self.states[self.drain_idx..self.drain_idx + num_drain];

        (self.state_finalize)(drain_states, output)?;
        self.drain_idx += num_drain;

        Ok(num_drain)
    }
}

pub fn drain<S, State, I>(states: &mut [State], output: &mut Array) -> Result<()>
where
    S: MutablePhysicalStorage,
    State: AggregateState<I, S::StorageType>,
{
    let buffer = &mut S::get_addressable_mut(output.data.try_as_mut()?)?;
    let validity = &mut output.validity;

    for (idx, state) in states.iter_mut().enumerate() {
        state.finalize(PutBuffer {
            idx,
            buffer,
            validity,
        })?;
    }

    Ok(())
}

impl<State, Input, Output, StateInit, StateUpdate, StateFinalize> fmt::Debug
    for TypedAggregateGroupStates<State, Input, Output, StateInit, StateUpdate, StateFinalize>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedAggregateGroupedStates")
            .field("num_states", &self.states.len())
            .finish_non_exhaustive()
    }
}

pub struct TypedAggregateGroupStates2<State, Input, Output, StateInit, StateUpdate, StateFinalize> {
    states: Vec<State>,

    state_init: StateInit,
    state_update: StateUpdate,
    state_finalize: StateFinalize,

    _input: PhantomData<Input>,
    _output: PhantomData<Output>,
}

impl<State, Input, Output, StateInit, StateUpdate, StateFinalize>
    TypedAggregateGroupStates2<State, Input, Output, StateInit, StateUpdate, StateFinalize>
{
    pub fn new(
        state_init: StateInit,
        state_update: StateUpdate,
        state_finalize: StateFinalize,
    ) -> Self {
        TypedAggregateGroupStates2 {
            states: Vec::<State>::new(),
            state_init,
            state_update,
            state_finalize,
            _input: PhantomData,
            _output: PhantomData,
        }
    }
}

/// Helper for create an `AggregateGroupStates` that accepts one input.
pub fn new_unary_aggregate_states2<Storage, State, Output, StateInit, StateFinalize>(
    state_init: StateInit,
    state_finalize: StateFinalize,
) -> Box<dyn AggregateGroupStates>
where
    Storage: PhysicalStorage2,
    State: for<'a> AggregateState2<
            <<Storage as PhysicalStorage2>::Storage<'a> as AddressableStorage>::T,
            Output,
        > + Sync
        + Send
        + 'static,
    Output: Sync + Send + 'static,
    StateInit: Fn() -> State + Sync + Send + 'static,
    StateFinalize: Fn(&mut [State]) -> Result<Array2> + Sync + Send + 'static,
{
    Box::new(TypedAggregateGroupStates2 {
        states: Vec::<State>::new(),
        state_init,
        state_update: unary_update2::<State, Storage, Output>,
        state_finalize,
        _input: PhantomData,
        _output: PhantomData,
    })
}

/// Helper for create an `AggregateGroupStates` that accepts two inputs.
pub fn new_binary_aggregate_states2<Storage1, Storage2, State, Output, StateInit, StateFinalize>(
    state_init: StateInit,
    state_finalize: StateFinalize,
) -> Box<dyn AggregateGroupStates>
where
    Storage1: PhysicalStorage2,
    Storage2: PhysicalStorage2,
    State: for<'a> AggregateState2<(Storage1::Type<'a>, Storage2::Type<'a>), Output>
        + Sync
        + Send
        + 'static,
    Output: Sync + Send + 'static,
    StateInit: Fn() -> State + Sync + Send + 'static,
    StateFinalize: Fn(&mut [State]) -> Result<Array2> + Sync + Send + 'static,
{
    Box::new(TypedAggregateGroupStates2 {
        states: Vec::<State>::new(),
        state_init,
        state_update: binary_update2::<State, Storage1, Storage2, Output>,
        state_finalize,
        _input: PhantomData,
        _output: PhantomData,
    })
}

impl<State, Input, Output, StateInit, StateUpdate, StateFinalize> AggregateGroupStates
    for TypedAggregateGroupStates2<State, Input, Output, StateInit, StateUpdate, StateFinalize>
where
    State: AggregateState2<Input, Output> + Sync + Send + 'static,
    Input: Sync + Send,
    Output: Sync + Send,
    StateInit: Fn() -> State + Sync + Send,
    StateUpdate: Fn(&[&Array2], ChunkGroupAddressIter, &mut [State]) -> Result<()> + Sync + Send,
    StateFinalize: Fn(&mut [State]) -> Result<Array2> + Sync + Send,
{
    fn opaque_states_mut(&mut self) -> OpaqueStatesMut<'_> {
        OpaqueStatesMut(&mut self.states)
    }

    fn new_states(&mut self, count: usize) {
        self.states.extend((0..count).map(|_| (self.state_init)()))
    }

    fn num_states(&self) -> usize {
        self.states.len()
    }

    fn update_states2(&mut self, inputs: &[&Array2], mapping: ChunkGroupAddressIter) -> Result<()> {
        (self.state_update)(inputs, mapping, &mut self.states)
    }

    fn combine(
        &mut self,
        consume: &mut Box<dyn AggregateGroupStates>,
        mapping: ChunkGroupAddressIter,
    ) -> Result<()> {
        let consume_states = consume.opaque_states_mut().downcast::<Vec<State>>()?;
        // StateCombiner2::combine(consume_states, mapping, &mut self.states)
        unimplemented!()
    }

    fn finalize2(&mut self) -> Result<Array2> {
        (self.state_finalize)(&mut self.states)
    }
}

impl<State, Input, Output, StateInit, StateUpdate, StateFinalize> fmt::Debug
    for TypedAggregateGroupStates2<State, Input, Output, StateInit, StateUpdate, StateFinalize>
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

    /// Create `count` number of new states.
    fn new_states(&mut self, count: usize);

    /// Returns the number of states being tracked.
    fn num_states(&self) -> usize;

    /// Update states from inputs using some mapping.
    fn update_states2(&mut self, inputs: &[&Array2], mapping: ChunkGroupAddressIter) -> Result<()>;

    fn update_states(
        &mut self,
        inputs: &[Array],
        selection: Selection,
        mapping: &[usize],
    ) -> Result<()> {
        unimplemented!()
    }

    /// Combine states from another partition into self using some mapping.
    fn combine(
        &mut self,
        consume: &mut Box<dyn AggregateGroupStates>,
        mapping: ChunkGroupAddressIter,
    ) -> Result<()>;

    /// Finalize the states and return an array.
    fn finalize2(&mut self) -> Result<Array2>;

    /// Finalize and drain state into `output`.
    ///
    /// Returns the number of states drained. If the number of states drained is
    /// less than the capacity of the output arrays, then draining is finished.
    fn drain(&mut self, output: &mut Array) -> Result<usize> {
        unimplemented!()
    }
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

pub fn unary_update<Storage, Output, State>(
    arrays: &[Array],
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
        &arrays[0],
        selection,
        mapping.iter().copied(),
        states,
    )
}

pub fn binary_update<Storage1, Storage2, Output, State>(
    arrays: &[Array],
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
        &arrays[0],
        &arrays[1],
        selection,
        mapping.iter().copied(),
        states,
    )
}

/// Update function for a unary aggregate.
pub fn unary_update2<State, Storage, Output>(
    arrays: &[&Array2],
    mapping: ChunkGroupAddressIter,
    states: &mut [State],
) -> Result<()>
where
    Storage: PhysicalStorage2,
    State: for<'a> AggregateState2<Storage::Type<'a>, Output>,
{
    unimplemented!()
    // UnaryNonNullUpdater::update::<Storage, _, _, _>(arrays[0], mapping, states)
}

pub fn binary_update2<State, Storage1, Storage2, Output>(
    arrays: &[&Array2],
    mapping: ChunkGroupAddressIter,
    states: &mut [State],
) -> Result<()>
where
    Storage1: PhysicalStorage2,
    Storage2: PhysicalStorage2,
    State: for<'a> AggregateState2<(Storage1::Type<'a>, Storage2::Type<'a>), Output>,
{
    unimplemented!()
    // BinaryNonNullUpdater::update::<Storage1, Storage2, _, _, _>(
    //     arrays[0], arrays[1], mapping, states,
    // )
}

pub fn untyped_null_finalize<State>(states: &mut [State]) -> Result<Array2> {
    Ok(Array2::new_untyped_null_array(states.len()))
}

pub fn boolean_finalize<State, Input>(datatype: DataType, states: &mut [State]) -> Result<Array2>
where
    State: AggregateState2<Input, bool>,
{
    let builder = ArrayBuilder {
        datatype,
        buffer: BooleanBuffer::with_len(states.len()),
    };
    StateFinalizer::finalize(states, builder)
}

pub fn primitive_finalize<State, Input, Output>(
    datatype: DataType,
    states: &mut [State],
) -> Result<Array2>
where
    State: AggregateState2<Input, Output>,
    Output: Copy + Default,
    ArrayData2: From<PrimitiveStorage<Output>>,
{
    let builder = ArrayBuilder {
        datatype,
        buffer: PrimitiveBuffer::with_len(states.len()),
    };
    StateFinalizer::finalize(states, builder)
}
