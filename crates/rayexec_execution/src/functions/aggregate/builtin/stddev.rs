use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_error::Result;

use crate::arrays::array::buffer_manager::BufferManager;
use crate::arrays::array::physical_type::{AddressableMut, PhysicalF64};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::aggregate::AggregateState;
use crate::arrays::executor::PutBuffer;
use crate::expr::Expression;
use crate::functions::aggregate::states::{
    drain,
    unary_update2,
    AggregateFunctionImpl,
    AggregateGroupStates,
    TypedAggregateGroupStates,
    UnaryStateLogic,
};
use crate::functions::aggregate::{
    AggregateFunction,
    AggregateFunctionImpl2,
    PlannedAggregateFunction,
};
use crate::functions::documentation::{Category, Documentation};
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
            positional_args: &[DataTypeId::Float64],
            variadic_arg: None,
            return_type: DataTypeId::Float64,
            doc: Some(&Documentation {
                category: Category::Aggregate,
                description: "Compute the population standard deviation.",
                arguments: &["inputs"],
                example: None,
            }),
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
                function_impl: AggregateFunctionImpl::new::<
                    UnaryStateLogic<VarianceState<StddevPopFinalize>, PhysicalF64, PhysicalF64>,
                >(None),
            }),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StddevPopImpl;

impl AggregateFunctionImpl2 for StddevPopImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        Box::new(TypedAggregateGroupStates::new(
            VarianceState::<StddevPopFinalize>::default,
            unary_update2::<PhysicalF64, PhysicalF64, _>,
            drain::<PhysicalF64, _, _>,
        ))
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
            positional_args: &[DataTypeId::Float64],
            variadic_arg: None,
            return_type: DataTypeId::Float64,
            doc: Some(&Documentation {
                category: Category::Aggregate,
                description: "Compute the sample standard deviation.",
                arguments: &["inputs"],
                example: None,
            }),
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
                function_impl: AggregateFunctionImpl::new::<
                    UnaryStateLogic<VarianceState<StddevSampFinalize>, PhysicalF64, PhysicalF64>,
                >(None),
            }),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StddevSampImpl;

impl AggregateFunctionImpl2 for StddevSampImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        Box::new(TypedAggregateGroupStates::new(
            VarianceState::<StddevSampFinalize>::default,
            unary_update2::<PhysicalF64, PhysicalF64, _>,
            drain::<PhysicalF64, _, _>,
        ))
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
            positional_args: &[DataTypeId::Float64],
            variadic_arg: None,
            return_type: DataTypeId::Float64,
            doc: Some(&Documentation {
                category: Category::Aggregate,
                description: "Compute the population variance.",
                arguments: &["inputs"],
                example: None,
            }),
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
                function_impl: AggregateFunctionImpl::new::<
                    UnaryStateLogic<VarianceState<VariancePopFinalize>, PhysicalF64, PhysicalF64>,
                >(None),
            }),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VarPopImpl;

impl AggregateFunctionImpl2 for VarPopImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        Box::new(TypedAggregateGroupStates::new(
            VarianceState::<VariancePopFinalize>::default,
            unary_update2::<PhysicalF64, PhysicalF64, _>,
            drain::<PhysicalF64, _, _>,
        ))
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
            positional_args: &[DataTypeId::Float64],
            variadic_arg: None,
            return_type: DataTypeId::Float64,
            doc: Some(&Documentation {
                category: Category::Aggregate,
                description: "Compute the sample variance.",
                arguments: &["inputs"],
                example: None,
            }),
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
                function_impl: AggregateFunctionImpl::new::<
                    UnaryStateLogic<VarianceState<VarianceSampFinalize>, PhysicalF64, PhysicalF64>,
                >(None),
            }),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VarSampImpl;

impl AggregateFunctionImpl2 for VarSampImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        Box::new(TypedAggregateGroupStates::new(
            VarianceState::<VarianceSampFinalize>::default,
            unary_update2::<PhysicalF64, PhysicalF64, _>,
            drain::<PhysicalF64, _, _>,
        ))
    }
}

pub trait VarianceFinalize: Sync + Send + Debug + Default + 'static {
    fn finalize(count: i64, mean: f64, m2: f64) -> Option<f64>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StddevPopFinalize;

impl VarianceFinalize for StddevPopFinalize {
    fn finalize(count: i64, _mean: f64, m2: f64) -> Option<f64> {
        match count {
            0 => None,
            1 => Some(0.0),
            _ => {
                let v = f64::sqrt(m2 / count as f64);
                Some(v)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StddevSampFinalize;

impl VarianceFinalize for StddevSampFinalize {
    fn finalize(count: i64, _mean: f64, m2: f64) -> Option<f64> {
        match count {
            0 | 1 => None,
            _ => {
                let v = f64::sqrt(m2 / (count - 1) as f64);
                Some(v)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct VarianceSampFinalize;

impl VarianceFinalize for VarianceSampFinalize {
    fn finalize(count: i64, _mean: f64, m2: f64) -> Option<f64> {
        match count {
            0 | 1 => None,
            _ => {
                let v = m2 / (count - 1) as f64;
                Some(v)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct VariancePopFinalize;

impl VarianceFinalize for VariancePopFinalize {
    fn finalize(count: i64, _mean: f64, m2: f64) -> Option<f64> {
        match count {
            0 => None,
            1 => Some(0.0),
            _ => {
                let v = m2 / count as f64;
                Some(v)
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

impl<F> VarianceState<F>
where
    F: VarianceFinalize,
{
    pub fn finalize_value(&self) -> Option<f64> {
        F::finalize(self.count, self.mean, self.m2)
    }
}

impl<F> AggregateState<&f64, f64> for VarianceState<F>
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

    fn update(&mut self, &input: &f64) -> Result<()> {
        self.count += 1;
        let delta = input - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = input - self.mean;
        self.m2 += delta * delta2;

        Ok(())
    }

    fn finalize<M, B>(&mut self, output: PutBuffer<M, B>) -> Result<()>
    where
        M: AddressableMut<B, T = f64>,
        B: BufferManager,
    {
        match F::finalize(self.count, self.mean, self.m2) {
            Some(val) => output.put(&val),
            None => output.put_null(),
        }
        Ok(())
    }
}
