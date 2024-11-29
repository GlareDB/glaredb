use std::fmt::Debug;

use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::aggregate::{AggregateState, BinaryNonNullUpdater};
use rayexec_bullet::executor::physical_type::PhysicalF64;
use rayexec_error::Result;

use super::covar::{CovarPopFinalize, CovarState};
use super::stddev::{StddevPopFinalize, VarianceState};
use super::{
    primitive_finalize,
    AggregateFunction,
    DefaultGroupedStates,
    PlannedAggregateFunction,
};
use crate::functions::aggregate::ChunkGroupAddressIter;
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};

/// Pearson coefficient, population.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Corr;

impl FunctionInfo for Corr {
    fn name(&self) -> &'static str {
        "corr"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Float64, DataTypeId::Float64],
            variadic: None,
            return_type: DataTypeId::Float64,
        }]
    }
}

impl AggregateFunction for Corr {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedAggregateFunction>> {
        Ok(Box::new(CorrImpl))
    }

    fn plan_from_datatypes(
        &self,
        inputs: &[DataType],
    ) -> Result<Box<dyn PlannedAggregateFunction>> {
        plan_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float64, DataType::Float64) => Ok(Box::new(CorrImpl)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CorrImpl;

impl PlannedAggregateFunction for CorrImpl {
    fn aggregate_function(&self) -> &dyn AggregateFunction {
        &Corr
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
            states: &mut [CorrelationState],
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
pub struct CorrelationState {
    covar: CovarState<CovarPopFinalize>,
    stddev_x: VarianceState<StddevPopFinalize>,
    stddev_y: VarianceState<StddevPopFinalize>,
}

impl AggregateState<(f64, f64), f64> for CorrelationState {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.covar.merge(&mut other.covar)?;
        self.stddev_x.merge(&mut other.stddev_x)?;
        self.stddev_y.merge(&mut other.stddev_y)?;
        Ok(())
    }

    fn update(&mut self, input: (f64, f64)) -> Result<()> {
        self.covar.update(input)?;

        // Note input is passed in as (y, x)
        self.stddev_x.update(input.1)?;
        self.stddev_y.update(input.0)?;

        Ok(())
    }

    fn finalize(&mut self) -> Result<(f64, bool)> {
        let (cov, cov_valid) = self.covar.finalize()?;
        let (stddev_x, stddev_x_valid) = self.stddev_x.finalize()?;
        let (stddev_y, stddev_y_valid) = self.stddev_y.finalize()?;

        if cov_valid && stddev_x_valid && stddev_y_valid {
            let div = stddev_x * stddev_y;
            if div == 0.0 {
                // TODO: Could return NULL. Postgres returns NULL in this case,
                // duckdb (recently) changed to return NaN.
                return Ok((f64::NAN, true));
            }
            Ok((cov / div, true))
        } else {
            Ok((0.0, false))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correlation_state_single_input() {
        let mut state = CorrelationState::default();
        state.update((1.0, 1.0)).unwrap();

        let (v, valid) = state.finalize().unwrap();
        assert!(valid);
        assert!(v.is_nan());
    }
}
