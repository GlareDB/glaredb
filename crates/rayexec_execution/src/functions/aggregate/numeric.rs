use rayexec_bullet::{
    array::{Array, PrimitiveArrayBuilder},
    executor::aggregate::{AggregateState, StateFinalizer, UnaryUpdater},
    field::DataType,
};

use super::{
    DefaultGroupedStates, GenericAggregateFunction, GroupedStates, SpecializedAggregateFunction,
};
use crate::functions::{InputTypes, ReturnType, Signature};
use rayexec_error::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sum;

impl GenericAggregateFunction for Sum {
    fn name(&self) -> &str {
        "sum"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: InputTypes::Exact(&[DataType::Int64]),
            return_type: ReturnType::Static(DataType::Int64), // TODO: Should be big num
        }]
    }

    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedAggregateFunction>> {
        match &inputs[0] {
            DataType::Int64 => Ok(Box::new(SumI64)),
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SumI64;

impl SpecializedAggregateFunction for SumI64 {
    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        let update_fn = |arrays: &[Array], mapping: &[usize], states: &mut [SumI64State]| {
            let inputs = match &arrays[0] {
                Array::Int32(arr) => arr,
                other => panic!("unexpected array type: {other:?}"),
            };
            UnaryUpdater::update(inputs, mapping, states)
        };

        let finalize_fn = |states: Vec<_>| {
            let mut builder = PrimitiveArrayBuilder::with_capacity(states.len());
            StateFinalizer::finalize(states, &mut builder)?;
            Ok(Array::Int64(builder.into_typed_array()))
        };

        Box::new(DefaultGroupedStates::new(update_fn, finalize_fn))
    }
}

#[derive(Debug, Default)]
pub struct SumI64State {
    sum: i64,
}

impl AggregateState<i32, i64> for SumI64State {
    fn merge(&mut self, other: Self) -> Result<()> {
        self.sum = other.sum;
        Ok(())
    }

    fn update(&mut self, input: i32) -> Result<()> {
        self.sum += input as i64;
        Ok(())
    }

    fn finalize(self) -> Result<i64> {
        Ok(self.sum)
    }
}
