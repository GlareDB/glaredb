use std::fmt::Debug;

use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::aggregate::{AggregateState, BinaryNonNullUpdater};
use rayexec_bullet::executor::physical_type::PhysicalF64;
use rayexec_error::Result;

use super::covar::{CovarPopFinalize, CovarState};
use super::stddev::{VariancePopFinalize, VarianceState};
use crate::functions::aggregate::{
    primitive_finalize,
    AggregateFunction,
    ChunkGroupAddressIter,
    DefaultGroupedStates,
    GroupedStates,
    PlannedAggregateFunction2,
};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegrSlope;

impl FunctionInfo for RegrSlope {
    fn name(&self) -> &'static str {
        "regr_slope"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Float64, DataTypeId::Float64],
            variadic: None,
            return_type: DataTypeId::Float64,
        }]
    }
}

impl AggregateFunction for RegrSlope {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedAggregateFunction2>> {
        Ok(Box::new(RegrSlopeImpl))
    }

    fn plan_from_datatypes(
        &self,
        inputs: &[DataType],
    ) -> Result<Box<dyn PlannedAggregateFunction2>> {
        plan_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float64, DataType::Float64) => Ok(Box::new(RegrSlopeImpl)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegrSlopeImpl;

impl PlannedAggregateFunction2 for RegrSlopeImpl {
    fn aggregate_function(&self) -> &dyn AggregateFunction {
        &RegrSlope
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Float64
    }

    fn new_grouped_state(&self) -> Result<Box<dyn GroupedStates>> {
        let datatype = self.return_type();

        fn update(
            arrays: &[&Array],
            mapping: ChunkGroupAddressIter,
            states: &mut [RegrSlopeState],
        ) -> Result<()> {
            BinaryNonNullUpdater::update::<PhysicalF64, PhysicalF64, _, _, _>(
                arrays[0], arrays[1], mapping, states,
            )
        }

        Ok(Box::new(DefaultGroupedStates::new(
            RegrSlopeState::default,
            update,
            move |states| primitive_finalize(datatype.clone(), states),
        )))
    }
}

#[derive(Debug, Default)]
pub struct RegrSlopeState {
    cov: CovarState<CovarPopFinalize>,
    var: VarianceState<VariancePopFinalize>,
}

impl AggregateState<(f64, f64), f64> for RegrSlopeState {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.cov.merge(&mut other.cov)?;
        self.var.merge(&mut other.var)?;
        Ok(())
    }

    fn update(&mut self, input: (f64, f64)) -> Result<()> {
        self.cov.update(input)?;
        self.var.update(input.1)?; // Update with 'x'
        Ok(())
    }

    fn finalize(&mut self) -> Result<(f64, bool)> {
        let (cov, cov_valid) = self.cov.finalize()?;
        let (var, var_valid) = self.var.finalize()?;

        if cov_valid && var_valid {
            if var == 0.0 {
                return Ok((0.0, false));
            }
            let v = cov / var;
            Ok((v, true))
        } else {
            Ok((0.0, false))
        }
    }
}
