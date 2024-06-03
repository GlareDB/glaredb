use rayexec_bullet::{
    array::{Array, PrimitiveArrayBuilder, UnitArrayAccessor},
    bitmap::Bitmap,
    executor::aggregate::{AggregateState, StateCombiner, StateFinalizer, UnaryNonNullUpdater},
    field::DataType,
};
use rayexec_error::{RayexecError, Result};
use std::vec;

use crate::functions::{InputTypes, ReturnType, Signature};

use super::{
    DefaultGroupedStates, GenericAggregateFunction, GroupedStates, SpecializedAggregateFunction,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Count;

impl GenericAggregateFunction for Count {
    fn name(&self) -> &str {
        "count"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: InputTypes::Dynamic,
            return_type: ReturnType::Static(DataType::Int64),
        }]
    }

    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedAggregateFunction>> {
        if inputs.len() != 1 {
            return Err(RayexecError::new("Expected 1 input"));
        }
        Ok(Box::new(CountNonNull))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CountNonNull;

impl SpecializedAggregateFunction for CountNonNull {
    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        let update_fn = |row_selection: &Bitmap,
                         arrays: &[&Array],
                         mapping: &[usize],
                         states: &mut [CountNonNullState]| {
            let unit_arr = UnitArrayAccessor::new(arrays[0]);
            UnaryNonNullUpdater::update(row_selection, unit_arr, mapping, states)
        };

        let finalize_fn = |states: vec::Drain<'_, _>| {
            let mut builder = PrimitiveArrayBuilder::with_capacity(states.len());
            StateFinalizer::finalize(states, &mut builder)?;
            Ok(Array::Int64(builder.into_typed_array()))
        };

        Box::new(DefaultGroupedStates::new(
            update_fn,
            StateCombiner::combine,
            finalize_fn,
        ))
    }
}

#[derive(Debug, Default)]
pub struct CountNonNullState {
    count: i64,
}

impl AggregateState<(), i64> for CountNonNullState {
    fn merge(&mut self, other: Self) -> Result<()> {
        self.count += other.count;
        Ok(())
    }

    fn update(&mut self, _input: ()) -> Result<()> {
        self.count += 1;
        Ok(())
    }

    fn finalize(self) -> Result<i64> {
        Ok(self.count)
    }
}
