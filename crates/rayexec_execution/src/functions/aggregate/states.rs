use std::any::Any;
use std::marker::PhantomData;

use rayexec_bullet::array::Array;
use rayexec_bullet::executor::aggregate::AggregateState;
use rayexec_error::{RayexecError, Result, ResultExt};

use super::ChunkGroupAddressIter;

#[derive(Debug)]
pub struct TypedAggregateGroupStates<State, Input, Output, StateInit, StateUpdate, StateFinalize>
where
    State: AggregateState<Input, Output> + 'static,
    StateInit: Fn() -> State,
    StateUpdate: Fn(&[&Array], ChunkGroupAddressIter, &mut [State]) -> Result<()>,
    StateFinalize: Fn(&mut [State]) -> Result<Array>,
{
    states: Vec<State>,

    state_init: StateInit,
    state_update: StateUpdate,
    state_finalize: StateFinalize,

    _input: PhantomData<Input>,
    _output: PhantomData<Output>,
}

impl<State, Input, Output, StateInit, StateUpdate, StateFinalize>
    TypedAggregateGroupStates<State, Input, Output, StateInit, StateUpdate, StateFinalize>
where
    State: AggregateState<Input, Output> + 'static,
    StateInit: Fn() -> State,
    StateUpdate: Fn(&[&Array], ChunkGroupAddressIter, &mut [State]) -> Result<()>,
    StateFinalize: Fn(&mut [State]) -> Result<Array>,
{
    pub fn take_opaque_states(&mut self) -> OpaqueStates {
        OpaqueStates(Box::new(std::mem::take(&mut self.states)))
    }

    pub fn update_states(
        &mut self,
        inputs: &[&Array],
        mapping: ChunkGroupAddressIter,
    ) -> Result<()> {
        (self.state_update)(inputs, mapping, &mut self.states)
    }

    pub fn combine(
        &mut self,
        consume: &mut Box<dyn AggregateGroupStates>,
        mapping: ChunkGroupAddressIter,
    ) -> Result<()> {
        let consume_states = consume.take_opaque_states().downcast::<State>()?;

        unimplemented!()
    }
}

pub trait AggregateGroupStates {
    fn take_opaque_states(&mut self) -> OpaqueStates;
    fn new_groups(&mut self, count: usize);
    fn update_states(&mut self, inputs: &[&Array], mapping: ChunkGroupAddressIter) -> Result<()>;
    fn combine(
        &mut self,
        consume: &mut Box<dyn AggregateGroupStates>,
        mapping: ChunkGroupAddressIter,
    ) -> Result<()>;
}

#[derive(Debug)]
pub struct OpaqueStates(Box<dyn Any>);

impl OpaqueStates {
    fn downcast<State: 'static>(self) -> Result<Vec<State>> {
        let states = self.0.downcast::<Vec<State>>().map_err(|_| {
            RayexecError::new("Attempted to combine aggregate states of different types")
        })?;

        Ok(*states)
    }
}
