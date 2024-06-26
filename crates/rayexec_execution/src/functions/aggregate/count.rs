use rayexec_bullet::{
    array::{Array, PrimitiveArray, UnitArrayAccessor},
    bitmap::Bitmap,
    datatype::{DataType, DataTypeId},
    executor::aggregate::{AggregateState, StateFinalizer, UnaryNonNullUpdater},
};
use rayexec_error::{RayexecError, Result};
use std::vec;

use crate::functions::{FunctionInfo, Signature};

use super::{
    DefaultGroupedStates, GenericAggregateFunction, GroupedStates, SpecializedAggregateFunction,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Count;

impl FunctionInfo for Count {
    fn name(&self) -> &'static str {
        "count"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Any],
            return_type: DataTypeId::Int64,
        }]
    }
}

impl GenericAggregateFunction for Count {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedAggregateFunction>> {
        if inputs.len() != 1 {
            return Err(RayexecError::new("Expected 1 input"));
        }
        Ok(Box::new(CountNonNull))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CountNonNull;

impl CountNonNull {
    fn update(
        row_selection: &Bitmap,
        arrays: &[&Array],
        mapping: &[usize],
        states: &mut [CountNonNullState],
    ) -> Result<()> {
        let unit_arr = UnitArrayAccessor::new(arrays[0]);
        UnaryNonNullUpdater::update(row_selection, unit_arr, mapping, states)
    }

    fn finalize(states: vec::Drain<CountNonNullState>) -> Result<Array> {
        let mut buffer = Vec::with_capacity(states.len());
        let mut bitmap = Bitmap::with_capacity(states.len());
        StateFinalizer::finalize(states, &mut buffer, &mut bitmap)?;
        Ok(Array::Int64(PrimitiveArray::new(buffer, Some(bitmap))))
    }
}

impl SpecializedAggregateFunction for CountNonNull {
    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        Box::new(DefaultGroupedStates::new(Self::update, Self::finalize))
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

    fn finalize(self) -> Result<(i64, bool)> {
        // Always valid, even when count is 0
        Ok((self.count, true))
    }
}
