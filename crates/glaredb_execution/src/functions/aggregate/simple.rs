//! Helper traits for implementing unary/binary aggregates that don't need
//! custom update, combine, or finalize logic.

use std::fmt::Debug;

use glaredb_error::{RayexecError, Result};

use super::AggregateFunction;
use crate::arrays::array::physical_type::{MutableScalarStorage, ScalarStorage};
use crate::arrays::array::Array;
use crate::arrays::executor::aggregate::{
    AggregateState,
    BinaryNonNullUpdater,
    UnaryNonNullUpdater,
};
use crate::arrays::executor::PutBuffer;
use crate::expr::Expression;
use crate::functions::bind_state::BindState;

pub trait UnaryAggregate: Debug + Copy + Sync + Send + Sized + 'static {
    type Input: ScalarStorage;
    type Output: MutableScalarStorage;

    type BindState: Sync + Send;

    type GroupState: for<'a> AggregateState<
        &'a <Self::Input as ScalarStorage>::StorageType,
        <Self::Output as ScalarStorage>::StorageType,
        BindState = Self::BindState,
    >;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>>;
    fn new_aggregate_state(state: &Self::BindState) -> Self::GroupState;
}

#[derive(Debug, Clone, Copy)]
pub struct SimpleUnaryAggregate<U: UnaryAggregate> {
    unary: &'static U,
}

impl<U> SimpleUnaryAggregate<U>
where
    U: UnaryAggregate,
{
    pub const fn new(unary: &'static U) -> Self {
        SimpleUnaryAggregate { unary }
    }
}

impl<U> AggregateFunction for SimpleUnaryAggregate<U>
where
    U: UnaryAggregate,
{
    type BindState = U::BindState;
    type GroupState = U::GroupState;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        self.unary.bind(inputs)
    }

    fn new_aggregate_state(state: &Self::BindState) -> Self::GroupState {
        U::new_aggregate_state(state)
    }

    fn update(
        bind_state: &Self::BindState,
        inputs: &[Array],
        num_rows: usize,
        states: &mut [*mut Self::GroupState],
    ) -> Result<()> {
        UnaryNonNullUpdater::update::<U::Input, _, _, _>(
            &inputs[0],
            0..num_rows,
            bind_state,
            states,
        )
    }

    fn combine(
        bind_state: &Self::BindState,
        src: &mut [&mut Self::GroupState],
        dest: &mut [&mut Self::GroupState],
    ) -> Result<()> {
        if src.len() != dest.len() {
            return Err(RayexecError::new(
                "Source and destination have different number of states",
            )
            .with_field("source", src.len())
            .with_field("dest", dest.len()));
        }

        for (src, dest) in src.iter_mut().zip(dest) {
            dest.merge(bind_state, src)?;
        }

        Ok(())
    }

    fn finalize(
        bind_state: &Self::BindState,
        states: &mut [&mut Self::GroupState],
        output: &mut Array,
    ) -> Result<()> {
        let buffer = &mut <U::Output>::get_addressable_mut(&mut output.data)?;
        let validity = &mut output.validity;

        for (idx, state) in states.iter_mut().enumerate() {
            state.finalize(bind_state, PutBuffer::new(idx, buffer, validity))?;
        }

        Ok(())
    }
}

pub trait BinaryAggregate: Debug + Copy + Sync + Send + Sized + 'static {
    type Input1: ScalarStorage;
    type Input2: ScalarStorage;
    type Output: MutableScalarStorage;

    type BindState: Sync + Send;

    type GroupState: for<'a> AggregateState<
        (
            &'a <Self::Input1 as ScalarStorage>::StorageType,
            &'a <Self::Input2 as ScalarStorage>::StorageType,
        ),
        <Self::Output as ScalarStorage>::StorageType,
        BindState = Self::BindState,
    >;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>>;
    fn new_aggregate_state(state: &Self::BindState) -> Self::GroupState;
}

#[derive(Debug, Clone, Copy)]
pub struct SimpleBinaryAggregate<B: BinaryAggregate> {
    binary: &'static B,
}

impl<B> SimpleBinaryAggregate<B>
where
    B: BinaryAggregate,
{
    pub const fn new(binary: &'static B) -> Self {
        SimpleBinaryAggregate { binary }
    }
}

impl<B> AggregateFunction for SimpleBinaryAggregate<B>
where
    B: BinaryAggregate,
{
    type BindState = B::BindState;
    type GroupState = B::GroupState;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        self.binary.bind(inputs)
    }

    fn new_aggregate_state(state: &Self::BindState) -> Self::GroupState {
        B::new_aggregate_state(state)
    }

    fn update(
        bind_state: &Self::BindState,
        inputs: &[Array],
        num_rows: usize,
        states: &mut [*mut Self::GroupState],
    ) -> Result<()> {
        BinaryNonNullUpdater::update::<B::Input1, B::Input2, _, _, _>(
            &inputs[0],
            &inputs[1],
            0..num_rows,
            bind_state,
            states,
        )
    }

    fn combine(
        bind_state: &Self::BindState,
        src: &mut [&mut Self::GroupState],
        dest: &mut [&mut Self::GroupState],
    ) -> Result<()> {
        if src.len() != dest.len() {
            return Err(RayexecError::new(
                "Source and destination have different number of states",
            )
            .with_field("source", src.len())
            .with_field("dest", dest.len()));
        }

        for (src, dest) in src.iter_mut().zip(dest) {
            dest.merge(bind_state, src)?;
        }

        Ok(())
    }

    fn finalize(
        bind_state: &Self::BindState,
        states: &mut [&mut Self::GroupState],
        output: &mut Array,
    ) -> Result<()> {
        let buffer = &mut <B::Output>::get_addressable_mut(&mut output.data)?;
        let validity = &mut output.validity;

        for (idx, state) in states.iter_mut().enumerate() {
            state.finalize(bind_state, PutBuffer::new(idx, buffer, validity))?;
        }

        Ok(())
    }
}
