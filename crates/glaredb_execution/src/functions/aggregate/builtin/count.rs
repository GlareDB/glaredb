use std::fmt::Debug;

use glaredb_error::{RayexecError, Result};

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
                for &mut state_ptr in states {
                    let state = unsafe { &mut *state_ptr };
                    state.count += 1;
                }
            } else {
                for (idx, &mut state_ptr) in states.iter_mut().enumerate() {
                    if !input.validity.is_valid(idx) {
                        continue;
                    }

                    let state = unsafe { &mut *state_ptr };
                    state.count += 1;
                }
            }

            return Ok(());
        }

        if input.validity.all_valid() {
            for &mut state_ptr in states {
                let state = unsafe { &mut *state_ptr };
                state.count += 1;
            }
        } else {
            for (idx, &mut state_ptr) in states.iter_mut().enumerate() {
                if !input.validity.is_valid(idx) {
                    continue;
                }

                let state = unsafe { &mut *state_ptr };
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
            dest.merge(&(), src)?;
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
            state.finalize(&(), PutBuffer::new(idx, buffer, validity))?;
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct CountNonNullState {
    count: i64,
}

impl AggregateState<&(), i64> for CountNonNullState {
    type BindState = ();

    fn merge(&mut self, _state: &(), other: &mut Self) -> Result<()> {
        self.count += other.count;
        Ok(())
    }

    fn update(&mut self, _state: &(), _input: &()) -> Result<()> {
        self.count += 1;
        Ok(())
    }

    fn finalize<M>(&mut self, _state: &(), output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = i64>,
    {
        output.put(&self.count);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_manager::NopBufferManager;
    use crate::util::iter::TryFromExactSizeIterator;

    #[test]
    fn count_single_state() {
        let mut state = CountNonNullState::default();
        let state_ptr: *mut CountNonNullState = &mut state;
        let mut states = vec![state_ptr; 4];

        let array = Array::try_from_iter(["a", "b", "c", "d"]).unwrap();

        Count::update(&(), &[array], 4, &mut states).unwrap();

        assert_eq!(4, state.count);
    }

    #[test]
    fn count_multiple_states() {
        let mut state1 = CountNonNullState::default();
        let mut state2 = CountNonNullState::default();
        let state_ptr1: *mut CountNonNullState = &mut state1;
        let state_ptr2: *mut CountNonNullState = &mut state2;

        let mut states = vec![state_ptr1, state_ptr1, state_ptr2, state_ptr1];

        let array = Array::try_from_iter(["a", "b", "c", "d"]).unwrap();
        Count::update(&(), &[array], 4, &mut states).unwrap();

        assert_eq!(3, state1.count);
        assert_eq!(1, state2.count);
    }

    #[test]
    fn count_multiple_states_with_constant() {
        let mut state1 = CountNonNullState::default();
        let mut state2 = CountNonNullState::default();
        let state_ptr1: *mut CountNonNullState = &mut state1;
        let state_ptr2: *mut CountNonNullState = &mut state2;

        let mut states = vec![state_ptr1, state_ptr1, state_ptr2, state_ptr1];

        let array = Array::new_constant(&NopBufferManager, &"a".into(), 4).unwrap();
        Count::update(&(), &[array], 4, &mut states).unwrap();

        assert_eq!(3, state1.count);
        assert_eq!(1, state2.count);
    }
}
