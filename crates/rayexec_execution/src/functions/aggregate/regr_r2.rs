use std::fmt::Debug;

use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::aggregate::{AggregateState, BinaryNonNullUpdater};
use rayexec_bullet::executor::physical_type::PhysicalF64;
use rayexec_error::Result;

use super::corr::CorrelationState;
use super::{
    primitive_finalize,
    AggregateFunction,
    DefaultGroupedStates,
    PlannedAggregateFunction,
};
use crate::functions::aggregate::ChunkGroupAddressIter;
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegrR2;

impl FunctionInfo for RegrR2 {
    fn name(&self) -> &'static str {
        "regr_r2"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Float64, DataTypeId::Float64],
            variadic: None,
            return_type: DataTypeId::Float64,
        }]
    }
}

impl AggregateFunction for RegrR2 {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedAggregateFunction>> {
        Ok(Box::new(RegrR2Impl))
    }

    fn plan_from_datatypes(
        &self,
        inputs: &[DataType],
    ) -> Result<Box<dyn PlannedAggregateFunction>> {
        plan_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float64, DataType::Float64) => Ok(Box::new(RegrR2Impl)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegrR2Impl;

impl PlannedAggregateFunction for RegrR2Impl {
    fn aggregate_function(&self) -> &dyn AggregateFunction {
        &RegrR2
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Float64
    }

    fn new_grouped_state(&self) -> Result<Box<dyn super::GroupedStates>> {
        let datatype = self.return_type();

        fn update(
            arrays: &[&Array],
            mapping: ChunkGroupAddressIter,
            states: &mut [RegrCountState],
        ) -> Result<()> {
            BinaryNonNullUpdater::update::<PhysicalF64, PhysicalF64, _, _, _>(
                arrays[0], arrays[1], mapping, states,
            )
        }

        Ok(Box::new(DefaultGroupedStates::new(update, move |states| {
            primitive_finalize(datatype.clone(), states)
        })))
    }
}

#[derive(Debug, Default)]
pub struct RegrCountState {
    corr: CorrelationState,
}

impl AggregateState<(f64, f64), f64> for RegrCountState {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.corr.merge(&mut other.corr)?;
        Ok(())
    }

    fn update(&mut self, input: (f64, f64)) -> Result<()> {
        self.corr.update(input)?;
        Ok(())
    }

    fn finalize(&mut self) -> Result<(f64, bool)> {
        let (v, valid) = self.corr.finalize()?;
        if valid {
            Ok((v.powi(2), true))
        } else {
            Ok((0.0, false))
        }
    }
}
