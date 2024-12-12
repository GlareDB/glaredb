use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::aggregate::{AggregateState, StateFinalizer, UnaryNonNullUpdater};
use rayexec_bullet::executor::builder::{ArrayBuilder, PrimitiveBuffer};
use rayexec_bullet::executor::physical_type::PhysicalAny;
use rayexec_error::{RayexecError, Result};
use serde::{Deserialize, Serialize};

use crate::functions::aggregate::{
    AggregateFunction,
    ChunkGroupAddressIter,
    DefaultGroupedStates,
    GroupedStates,
    PlannedAggregateFunction2,
};
use crate::functions::{FunctionInfo, Signature};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Count;

impl FunctionInfo for Count {
    fn name(&self) -> &'static str {
        "count"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Any],
            variadic: None,
            return_type: DataTypeId::Int64,
        }]
    }
}

impl AggregateFunction for Count {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedAggregateFunction2>> {
        Ok(Box::new(CountNonNullImpl))
    }

    fn plan_from_datatypes(
        &self,
        inputs: &[DataType],
    ) -> Result<Box<dyn PlannedAggregateFunction2>> {
        if inputs.len() != 1 {
            return Err(RayexecError::new("Expected 1 input"));
        }
        Ok(Box::new(CountNonNullImpl))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CountNonNullImpl;

impl CountNonNullImpl {
    fn update(
        arrays: &[&Array],
        mapping: ChunkGroupAddressIter,
        states: &mut [CountNonNullState],
    ) -> Result<()> {
        UnaryNonNullUpdater::update::<PhysicalAny, _, _, _>(arrays[0], mapping, states)
    }

    fn finalize(states: &mut [CountNonNullState]) -> Result<Array> {
        let builder = ArrayBuilder {
            datatype: DataType::Int64,
            buffer: PrimitiveBuffer::<i64>::with_len(states.len()),
        };
        StateFinalizer::finalize(states, builder)
    }
}

impl PlannedAggregateFunction2 for CountNonNullImpl {
    fn aggregate_function(&self) -> &dyn AggregateFunction {
        &Count
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Int64
    }

    fn new_grouped_state(&self) -> Result<Box<dyn GroupedStates>> {
        Ok(Box::new(DefaultGroupedStates::new(
            CountNonNullState::default,
            Self::update,
            Self::finalize,
        )))
    }
}

#[derive(Debug, Default)]
pub struct CountNonNullState {
    count: i64,
}

impl AggregateState<(), i64> for CountNonNullState {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.count += other.count;
        Ok(())
    }

    fn update(&mut self, _input: ()) -> Result<()> {
        self.count += 1;
        Ok(())
    }

    fn finalize(&mut self) -> Result<(i64, bool)> {
        // Always valid, even when count is 0
        Ok((self.count, true))
    }
}
