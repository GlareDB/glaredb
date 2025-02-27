use std::fmt::Debug;

use rayexec_error::{RayexecError, Result};

use crate::arrays::array::physical_type::{AddressableMut, MutableScalarStorage, PhysicalI64};
use crate::arrays::array::Array;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::aggregate::AggregateState;
use crate::arrays::executor::PutBuffer;
use crate::expr::Expression;
use crate::functions::aggregate::{AggregateFunction, RawAggregateFunction};
use crate::functions::bind_state::BindState;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::AggregateFunctionSet;
use crate::functions::Signature;

pub const FUNCTION_SET_COUNT: AggregateFunctionSet = AggregateFunctionSet {
    name: "count",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Aggregate,
        description: "Return the count of non-NULL inputs.",
        arguments: &["input"],
        example: None,
    }),
    functions: &[RawAggregateFunction::new(
        &Signature::new(&[DataTypeId::Any], DataTypeId::Int64),
        &Count,
    )],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Count;

impl AggregateFunction for Count {
    type BindState = ();
    type GroupState = CountNonNullState;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Int64,
            inputs,
        })
    }

    fn new_aggregate_state(_state: &Self::BindState) -> Self::GroupState {
        CountNonNullState::default()
    }

    fn update(
        _state: &Self::BindState,
        inputs: &[Array],
        num_rows: usize,
        states: &mut [*mut Self::GroupState],
    ) -> Result<()> {
        let input = &inputs[0];

        if num_rows != states.len() {
            return Err(RayexecError::new(
                "Invalid number of states for selection in count agggregate executor",
            )
            .with_field("num_rows", num_rows)
            .with_field("states_len", states.len()));
        }

        if input.should_flatten_for_execution() {
            let input = input.flatten()?;

            if input.validity.all_valid() {
                for idx in 0..num_rows {
                    let sel_idx = input.selection.get(idx).unwrap();
                    let state = unsafe { &mut *states[sel_idx] };
                    state.count += 1;
                }
            } else {
                for idx in 0..num_rows {
                    if !input.validity.is_valid(idx) {
                        continue;
                    }

                    let sel_idx = input.selection.get(idx).unwrap();
                    let state = unsafe { &mut *states[sel_idx] };
                    state.count += 1;
                }
            }

            return Ok(());
        }

        if input.validity.all_valid() {
            for idx in 0..num_rows {
                let state = unsafe { &mut *states[idx] };
                state.count += 1;
            }
        } else {
            for idx in 0..num_rows {
                if !input.validity.is_valid(idx) {
                    continue;
                }

                let state = unsafe { &mut *states[idx] };
                state.count += 1;
            }
        }

        Ok(())
    }

    fn combine(
        _state: &Self::BindState,
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
            dest.merge(src)?;
        }

        Ok(())
    }

    fn finalize(
        _state: &Self::BindState,
        states: &mut [&mut Self::GroupState],
        output: &mut Array,
    ) -> Result<()> {
        let buffer = &mut PhysicalI64::get_addressable_mut(&mut output.data)?;
        let validity = &mut output.validity;

        for (idx, state) in states.iter_mut().enumerate() {
            state.finalize(PutBuffer::new(idx, buffer, validity))?;
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct CountNonNullState {
    count: i64,
}

impl AggregateState<&(), i64> for CountNonNullState {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.count += other.count;
        Ok(())
    }

    fn update(&mut self, _input: &()) -> Result<()> {
        self.count += 1;
        Ok(())
    }

    fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = i64>,
    {
        output.put(&self.count);
        Ok(())
    }
}
