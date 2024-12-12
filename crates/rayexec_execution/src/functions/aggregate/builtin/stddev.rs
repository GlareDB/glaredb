use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::aggregate::AggregateState;
use rayexec_bullet::executor::physical_type::PhysicalF64;
use rayexec_error::Result;

use crate::expr::Expression;
use crate::functions::aggregate::states::{new_unary_aggregate_states, AggregateGroupStates};
use crate::functions::aggregate::{
    primitive_finalize,
    AggregateFunction,
    AggregateFunctionImpl,
    PlannedAggregateFunction,
};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

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
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction> {
        plan_check_num_args(self, &inputs, 1)?;

        match inputs[0].datatype(table_list)? {
            DataType::Float64 => Ok(PlannedAggregateFunction {
                function: Box::new(*self),
                return_type: DataType::Float64,
                inputs,
                function_impl: Box::new(StddevPopImpl),
            }),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StddevPopImpl;

impl AggregateFunctionImpl for StddevPopImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        new_unary_aggregate_states::<PhysicalF64, _, _, _, _>(
            VarianceState::<StddevPopFinalize>::default,
            move |states| primitive_finalize(DataType::Float64, states),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StddevSamp;

impl FunctionInfo for StddevSamp {
    fn name(&self) -> &'static str {
        "stddev_samp"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["stddev"]
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Float64],
            variadic: None,
            return_type: DataTypeId::Float64,
        }]
    }
}

impl AggregateFunction for StddevSamp {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction> {
        plan_check_num_args(self, &inputs, 1)?;

        match inputs[0].datatype(table_list)? {
            DataType::Float64 => Ok(PlannedAggregateFunction {
                function: Box::new(*self),
                return_type: DataType::Float64,
                inputs,
                function_impl: Box::new(StddevSampImpl),
            }),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StddevSampImpl;

impl AggregateFunctionImpl for StddevSampImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        new_unary_aggregate_states::<PhysicalF64, _, _, _, _>(
            VarianceState::<StddevSampFinalize>::default,
            move |states| primitive_finalize(DataType::Float64, states),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VarPop;

impl FunctionInfo for VarPop {
    fn name(&self) -> &'static str {
        "var_pop"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Float64],
            variadic: None,
            return_type: DataTypeId::Float64,
        }]
    }
}

impl AggregateFunction for VarPop {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction> {
        plan_check_num_args(self, &inputs, 1)?;

        match inputs[0].datatype(table_list)? {
            DataType::Float64 => Ok(PlannedAggregateFunction {
                function: Box::new(*self),
                return_type: DataType::Float64,
                inputs,
                function_impl: Box::new(VarPopImpl),
            }),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VarPopImpl;

impl AggregateFunctionImpl for VarPopImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        new_unary_aggregate_states::<PhysicalF64, _, _, _, _>(
            VarianceState::<VariancePopFinalize>::default,
            move |states| primitive_finalize(DataType::Float64, states),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VarSamp;

impl FunctionInfo for VarSamp {
    fn name(&self) -> &'static str {
        "var_samp"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Float64],
            variadic: None,
            return_type: DataTypeId::Float64,
        }]
    }
}

impl AggregateFunction for VarSamp {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction> {
        plan_check_num_args(self, &inputs, 1)?;

        match inputs[0].datatype(table_list)? {
            DataType::Float64 => Ok(PlannedAggregateFunction {
                function: Box::new(*self),
                return_type: DataType::Float64,
                inputs,
                function_impl: Box::new(VarSampImpl),
            }),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VarSampImpl;

impl AggregateFunctionImpl for VarSampImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        new_unary_aggregate_states::<PhysicalF64, _, _, _, _>(
            VarianceState::<VarianceSampFinalize>::default,
            move |states| primitive_finalize(DataType::Float64, states),
        )
    }
}

pub trait VarianceFinalize: Sync + Send + Debug + Default + 'static {
    fn finalize(count: i64, mean: f64, m2: f64) -> (f64, bool);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StddevPopFinalize;

impl VarianceFinalize for StddevPopFinalize {
    fn finalize(count: i64, _mean: f64, m2: f64) -> (f64, bool) {
        match count {
            0 => (0.0, false),
            1 => (0.0, true),
            _ => {
                let v = f64::sqrt(m2 / count as f64);
                (v, true)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StddevSampFinalize;

impl VarianceFinalize for StddevSampFinalize {
    fn finalize(count: i64, _mean: f64, m2: f64) -> (f64, bool) {
        match count {
            0 | 1 => (0.0, false),
            _ => {
                let v = f64::sqrt(m2 / (count - 1) as f64);
                (v, true)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct VarianceSampFinalize;

impl VarianceFinalize for VarianceSampFinalize {
    fn finalize(count: i64, _mean: f64, m2: f64) -> (f64, bool) {
        match count {
            0 | 1 => (0.0, false),
            _ => {
                let v = m2 / (count - 1) as f64;
                (v, true)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct VariancePopFinalize;

impl VarianceFinalize for VariancePopFinalize {
    fn finalize(count: i64, _mean: f64, m2: f64) -> (f64, bool) {
        match count {
            0 => (0.0, false),
            1 => (0.0, true),
            _ => {
                let v = m2 / count as f64;
                (v, true)
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct VarianceState<F: VarianceFinalize> {
    pub count: i64,
    pub mean: f64,
    pub m2: f64,
    _finalize: PhantomData<F>,
}

impl<F> AggregateState<f64, f64> for VarianceState<F>
where
    F: VarianceFinalize,
{
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        if self.count == 0 {
            std::mem::swap(self, other);
            return Ok(());
        }

        let self_count = self.count as f64;
        let other_count = other.count as f64;
        let total_count = self_count + other_count;

        let new_mean = (self_count * self.mean + other_count * other.mean) / total_count;
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
        Ok(F::finalize(self.count, self.mean, self.m2))
    }
}
