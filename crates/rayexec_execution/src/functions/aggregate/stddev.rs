use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::aggregate::AggregateState;
use rayexec_bullet::executor::physical_type::PhysicalF64;
use rayexec_error::Result;

use super::{
    primitive_finalize,
    unary_update,
    AggregateFunction,
    DefaultGroupedStates,
    PlannedAggregateFunction,
};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StddevPop;

impl FunctionInfo for StddevPop {
    fn name(&self) -> &'static str {
        "stddev_pop"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Float64],
            variadic: None,
            return_type: DataTypeId::Float64,
        }]
    }
}

impl AggregateFunction for StddevPop {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedAggregateFunction>> {
        Ok(Box::new(StddevPopImpl))
    }

    fn plan_from_datatypes(
        &self,
        inputs: &[DataType],
    ) -> Result<Box<dyn PlannedAggregateFunction>> {
        plan_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Float64 => Ok(Box::new(StddevPopImpl)),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StddevPopImpl;

impl PlannedAggregateFunction for StddevPopImpl {
    fn aggregate_function(&self) -> &dyn AggregateFunction {
        &StddevPop
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Float64
    }

    fn new_grouped_state(&self) -> Result<Box<dyn super::GroupedStates>> {
        let datatype = self.return_type();
        Ok(Box::new(DefaultGroupedStates::new(
            unary_update::<StddevPopState, PhysicalF64, f64>,
            move |states| primitive_finalize(datatype.clone(), states),
        )))
    }
}

#[derive(Debug, Default)]
pub struct StddevPopState {
    count: i64,
    mean: f64,
    m2: f64,
}

impl AggregateState<f64, f64> for StddevPopState {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        if self.count == 0 {
            std::mem::swap(self, other);
            return Ok(());
        }

        let self_count = self.count as f64;
        let other_count = other.count as f64;
        let total_count = self_count + other_count;

        let new_mean = (self_count as f64 * self.mean + other_count * other.mean) / total_count;
        let delta = self.mean - other.mean;

        self.m2 = self.m2 + other.m2 + delta * delta * self_count * other_count / total_count;
        self.mean = new_mean;
        self.count += other.count;

        Ok(())
    }

    fn update(&mut self, input: f64) -> Result<()> {
        self.count += 1;
        let delta = input - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = input - self.mean;
        self.m2 += delta * delta2;

        Ok(())
    }

    fn finalize(&mut self) -> Result<(f64, bool)> {
        match self.count {
            0 => Ok((0.0, false)),
            1 => Ok((0.0, true)),
            _ => {
                let v = f64::sqrt(self.m2 / self.count as f64);
                Ok((v, true))
            }
        }
    }
}
