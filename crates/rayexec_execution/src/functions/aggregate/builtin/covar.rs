use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_error::Result;

use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::aggregate::AggregateState;
use crate::arrays::executor::physical_type::PhysicalF64_2;
use crate::expr::Expression;
use crate::functions::aggregate::states::{
    new_binary_aggregate_states,
    primitive_finalize,
    AggregateGroupStates,
};
use crate::functions::aggregate::{
    AggregateFunction,
    AggregateFunctionImpl,
    PlannedAggregateFunction,
};
use crate::functions::documentation::{Category, Documentation};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CovarPop;

impl FunctionInfo for CovarPop {
    fn name(&self) -> &'static str {
        "covar_pop"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Float64, DataTypeId::Float64],
            variadic_arg: None,
            return_type: DataTypeId::Float64,
            doc: Some(&Documentation {
                category: Category::Aggregate,
                description: "Compute population covariance.",
                arguments: &["y", "x"],
                example: None,
            }),
        }]
    }
}

impl AggregateFunction for CovarPop {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction> {
        plan_check_num_args(self, &inputs, 2)?;

        match (
            inputs[0].datatype(table_list)?,
            inputs[1].datatype(table_list)?,
        ) {
            (DataType::Float64, DataType::Float64) => Ok(PlannedAggregateFunction {
                function: Box::new(*self),
                return_type: DataType::Float64,
                inputs,
                function_impl: Box::new(CovarPopImpl),
            }),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CovarPopImpl;

impl AggregateFunctionImpl for CovarPopImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        new_binary_aggregate_states::<PhysicalF64_2, PhysicalF64_2, _, _, _, _>(
            CovarState::<CovarPopFinalize>::default,
            move |states| primitive_finalize(DataType::Float64, states),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CovarSamp;

impl FunctionInfo for CovarSamp {
    fn name(&self) -> &'static str {
        "covar_samp"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Float64, DataTypeId::Float64],
            variadic_arg: None,
            return_type: DataTypeId::Float64,
            doc: Some(&Documentation {
                category: Category::Aggregate,
                description: "Compute sample covariance.",
                arguments: &["y", "x"],
                example: None,
            }),
        }]
    }
}

impl AggregateFunction for CovarSamp {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction> {
        plan_check_num_args(self, &inputs, 2)?;

        match (
            inputs[0].datatype(table_list)?,
            inputs[1].datatype(table_list)?,
        ) {
            (DataType::Float64, DataType::Float64) => Ok(PlannedAggregateFunction {
                function: Box::new(*self),
                return_type: DataType::Float64,
                inputs,
                function_impl: Box::new(CovarSampImpl),
            }),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CovarSampImpl;

impl AggregateFunctionImpl for CovarSampImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        new_binary_aggregate_states::<PhysicalF64_2, PhysicalF64_2, _, _, _, _>(
            CovarState::<CovarSampFinalize>::default,
            move |states| primitive_finalize(DataType::Float64, states),
        )
    }
}

pub trait CovarFinalize: Sync + Send + Debug + Default + 'static {
    fn finalize(co_moment: f64, count: i64) -> (f64, bool);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CovarSampFinalize;

impl CovarFinalize for CovarSampFinalize {
    fn finalize(co_moment: f64, count: i64) -> (f64, bool) {
        match count {
            0 | 1 => (0.0, false),
            _ => (co_moment / (count - 1) as f64, true),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CovarPopFinalize;

impl CovarFinalize for CovarPopFinalize {
    fn finalize(co_moment: f64, count: i64) -> (f64, bool) {
        match count {
            0 => (0.0, false),
            _ => (co_moment / count as f64, true),
        }
    }
}

#[derive(Debug, Default)]
pub struct CovarState<F: CovarFinalize> {
    pub count: i64,
    pub meanx: f64,
    pub meany: f64,
    pub co_moment: f64,
    _finalize: PhantomData<F>,
}

impl<F> AggregateState<(f64, f64), f64> for CovarState<F>
where
    F: CovarFinalize,
{
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        if self.count == 0 {
            std::mem::swap(self, other);
            return Ok(());
        }
        if other.count == 0 {
            return Ok(());
        }

        let count = self.count + other.count;
        let meanx =
            (other.count as f64 * other.meanx + self.count as f64 * self.meanx) / count as f64;
        let meany =
            (other.count as f64 * other.meany + self.count as f64 * self.meany) / count as f64;

        let deltax = self.meanx - other.meanx;
        let deltay = self.meany - other.meany;

        self.co_moment = other.co_moment
            + self.co_moment
            + deltax * deltay * other.count as f64 * self.count as f64 / count as f64;
        self.meanx = meanx;
        self.meany = meany;
        self.count = count;

        Ok(())
    }

    fn update(&mut self, input: (f64, f64)) -> Result<()> {
        // Note that 'y' comes first, covariance functions are call like `COVAR_SAMP(y, x)`.
        let x = input.1;
        let y = input.0;

        self.count += 1;
        let n = self.count as f64;

        let dx = x - self.meanx;
        let meanx = self.meanx + dx / n;

        let dy = y - self.meany;
        let meany = self.meany + dy / n;

        let co_moment = self.co_moment + dx * (y - meany);

        self.meanx = meanx;
        self.meany = meany;
        self.co_moment = co_moment;

        Ok(())
    }

    fn finalize(&mut self) -> Result<(f64, bool)> {
        Ok(F::finalize(self.co_moment, self.count))
    }
}
