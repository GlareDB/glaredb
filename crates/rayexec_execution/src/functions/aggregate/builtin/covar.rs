use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_error::Result;

use crate::arrays::array::physical_type::{AddressableMut, PhysicalF64};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::aggregate::AggregateState;
use crate::arrays::executor::PutBuffer;
use crate::buffer::buffer_manager::BufferManager;
use crate::expr::Expression;
use crate::functions::aggregate::states::{AggregateFunctionImpl, BinaryStateLogic};
use crate::functions::aggregate::{AggregateFunction2, PlannedAggregateFunction2};
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

impl AggregateFunction2 for CovarPop {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction2> {
        plan_check_num_args(self, &inputs, 2)?;

        match (inputs[0].datatype()?, inputs[1].datatype()?) {
            (DataType::Float64, DataType::Float64) => Ok(PlannedAggregateFunction2 {
                function: Box::new(*self),
                return_type: DataType::Float64,
                inputs,
                function_impl: AggregateFunctionImpl::new::<
                    BinaryStateLogic<
                        CovarState<CovarPopFinalize>,
                        PhysicalF64,
                        PhysicalF64,
                        PhysicalF64,
                    >,
                >(None),
            }),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
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

impl AggregateFunction2 for CovarSamp {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction2> {
        plan_check_num_args(self, &inputs, 2)?;

        match (inputs[0].datatype()?, inputs[1].datatype()?) {
            (DataType::Float64, DataType::Float64) => Ok(PlannedAggregateFunction2 {
                function: Box::new(*self),
                return_type: DataType::Float64,
                inputs,
                function_impl: AggregateFunctionImpl::new::<
                    BinaryStateLogic<
                        CovarState<CovarSampFinalize>,
                        PhysicalF64,
                        PhysicalF64,
                        PhysicalF64,
                    >,
                >(None),
            }),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

pub trait CovarFinalize: Sync + Send + Debug + Default + 'static {
    fn finalize(co_moment: f64, count: i64) -> Option<f64>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CovarSampFinalize;

impl CovarFinalize for CovarSampFinalize {
    fn finalize(co_moment: f64, count: i64) -> Option<f64> {
        match count {
            0 | 1 => None,
            _ => Some(co_moment / (count - 1) as f64),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CovarPopFinalize;

impl CovarFinalize for CovarPopFinalize {
    fn finalize(co_moment: f64, count: i64) -> Option<f64> {
        match count {
            0 => None,
            _ => Some(co_moment / count as f64),
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

impl<F> CovarState<F>
where
    F: CovarFinalize,
{
    pub fn finalize_value(&self) -> Option<f64> {
        F::finalize(self.co_moment, self.count)
    }
}

impl<F> AggregateState<(&f64, &f64), f64> for CovarState<F>
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

    // Note that 'y' comes first, covariance functions are call like `COVAR_SAMP(y, x)`.
    fn update(&mut self, (&y, &x): (&f64, &f64)) -> Result<()> {
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

    fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = f64>,
    {
        match F::finalize(self.co_moment, self.count) {
            Some(val) => output.put(&val),
            None => output.put_null(),
        }
        Ok(())
    }
}
