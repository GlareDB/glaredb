use core::fmt;
use std::any::Any;
use std::fmt::Debug;

use rayexec_error::{RayexecError, Result};
use stdutil::marker::PhantomCovariant;

use super::ChunkGroupAddressIter;
use crate::arrays::array::{Array, ArrayData};
use crate::arrays::datatype::DataType;
use crate::arrays::executor::aggregate::{
    AggregateState,
    BinaryNonNullUpdater,
    StateCombiner,
    StateFinalizer,
    UnaryNonNullUpdater,
};
use crate::arrays::executor::builder::{ArrayBuilder, BooleanBuffer, PrimitiveBuffer};
use crate::arrays::executor::physical_type::PhysicalStorage;
use crate::arrays::storage::{AddressableStorage, PrimitiveStorage};

pub struct TypedAggregateGroupStates<State, Input, Output, StateInit, StateUpdate, StateFinalize> {
    states: Vec<State>,

    state_init: StateInit,
    state_update: StateUpdate,
    state_finalize: StateFinalize,

    // Note that these don't use `PhantomData` since we'll want to allow unsized
    // types for the output type.
    _input: PhantomCovariant<Input>,
    _output: PhantomCovariant<Output>,
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
            state_init,
            state_update,
            state_finalize,
            _input: PhantomCovariant::new(),
            _output: PhantomCovariant::new(),
        }
    }
}

/// Helper for create an `AggregateGroupStates` that accepts one input.
pub fn new_unary_aggregate_states<Storage, State, Output, StateInit, StateFinalize>(
    state_init: StateInit,
    state_finalize: StateFinalize,
) -> Box<dyn AggregateGroupStates>
where
    Storage: PhysicalStorage,
    State: for<'a> AggregateState<
            <<Storage as PhysicalStorage>::Storage<'a> as AddressableStorage>::T,
            Output,
        > + Sync
        + Send
        + 'static,
    Output: Sync + Send + 'static,
    StateInit: Fn() -> State + Sync + Send + 'static,
    StateFinalize: Fn(&mut [State]) -> Result<Array> + Sync + Send + 'static,
{
    Box::new(TypedAggregateGroupStates {
        states: Vec::new(),
        state_init,
        state_update: unary_update::<State, Storage, Output>,
        state_finalize,
        _input: PhantomCovariant::new(),
        _output: PhantomCovariant::new(),
    })
}

/// Helper for create an `AggregateGroupStates` that accepts two inputs.
pub fn new_binary_aggregate_states<Storage1, Storage2, State, Output, StateInit, StateFinalize>(
    state_init: StateInit,
    state_finalize: StateFinalize,
) -> Box<dyn AggregateGroupStates>
where
    Storage1: PhysicalStorage,
    Storage2: PhysicalStorage,
    State: for<'a> AggregateState<(Storage1::Type<'a>, Storage2::Type<'a>), Output>
        + Sync
        + Send
        + 'static,
    Output: Sync + Send + 'static,
    StateInit: Fn() -> State + Sync + Send + 'static,
    StateFinalize: Fn(&mut [State]) -> Result<Array> + Sync + Send + 'static,
{
    Box::new(TypedAggregateGroupStates {
        states: Vec::new(),
        state_init,
        state_update: binary_update::<State, Storage1, Storage2, Output>,
        state_finalize,
        _input: PhantomCovariant::new(),
        _output: PhantomCovariant::new(),
    })
}

impl<State, Input, Output, StateInit, StateUpdate, StateFinalize> AggregateGroupStates
    for TypedAggregateGroupStates<State, Input, Output, StateInit, StateUpdate, StateFinalize>
where
    State: AggregateState<Input, Output> + Sync + Send + 'static,
    Input: Sync + Send,
    Output: Sync + Send,
    StateInit: Fn() -> State + Sync + Send,
    StateUpdate: Fn(&[&Array], ChunkGroupAddressIter, &mut [State]) -> Result<()> + Sync + Send,
    StateFinalize: Fn(&mut [State]) -> Result<Array> + Sync + Send,
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

    fn update_states(&mut self, inputs: &[&Array], mapping: ChunkGroupAddressIter) -> Result<()> {
        (self.state_update)(inputs, mapping, &mut self.states)
    }

    fn combine(
        &mut self,
        consume: &mut Box<dyn AggregateGroupStates>,
        mapping: ChunkGroupAddressIter,
    ) -> Result<()> {
        let consume_states = consume.opaque_states_mut().downcast::<Vec<State>>()?;
        StateCombiner::combine(consume_states, mapping, &mut self.states)
    }

    fn finalize(&mut self) -> Result<Array> {
        (self.state_finalize)(&mut self.states)
    }
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
    fn update_states(&mut self, inputs: &[&Array], mapping: ChunkGroupAddressIter) -> Result<()>;

    /// Combine states from another partition into self using some mapping.
    fn combine(
        &mut self,
        consume: &mut Box<dyn AggregateGroupStates>,
        mapping: ChunkGroupAddressIter,
    ) -> Result<()>;

    /// Finalize the states and return an array.
    fn finalize(&mut self) -> Result<Array>;
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

/// Update function for a unary aggregate.
pub fn unary_update<State, Storage, Output>(
    arrays: &[&Array],
    mapping: ChunkGroupAddressIter,
    states: &mut [State],
) -> Result<()>
where
    Storage: PhysicalStorage,
    State: for<'a> AggregateState<Storage::Type<'a>, Output>,
{
    UnaryNonNullUpdater::update::<Storage, _, _, _>(arrays[0], mapping, states)
}

pub fn binary_update<State, Storage1, Storage2, Output>(
    arrays: &[&Array],
    mapping: ChunkGroupAddressIter,
    states: &mut [State],
) -> Result<()>
where
    Storage1: PhysicalStorage,
    Storage2: PhysicalStorage,
    State: for<'a> AggregateState<(Storage1::Type<'a>, Storage2::Type<'a>), Output>,
{
    BinaryNonNullUpdater::update::<Storage1, Storage2, _, _, _>(
        arrays[0], arrays[1], mapping, states,
    )
}

pub fn untyped_null_finalize<State>(states: &mut [State]) -> Result<Array> {
    Ok(Array::new_untyped_null_array(states.len()))
}

pub fn boolean_finalize<State, Input>(datatype: DataType, states: &mut [State]) -> Result<Array>
where
    State: AggregateState<Input, bool>,
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
) -> Result<Array>
where
    State: AggregateState<Input, Output>,
    Output: Copy + Default,
    ArrayData: From<PrimitiveStorage<Output>>,
{
    let builder = ArrayBuilder {
        datatype,
        buffer: PrimitiveBuffer::with_len(states.len()),
    };
    StateFinalizer::finalize(states, builder)
}
