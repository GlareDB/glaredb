use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_error::Result;

use crate::arrays::array::physical_type::{AddressableMut, PhysicalF64};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::aggregate::AggregateState;
use crate::arrays::executor::PutBuffer;
use crate::expr::Expression;
use crate::functions::aggregate::simple::{BinaryAggregate, SimpleBinaryAggregate};
use crate::functions::aggregate::RawAggregateFunction;
use crate::functions::bind_state::BindState;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::AggregateFunctionSet;
use crate::functions::Signature;

pub const FUNCTION_SET_REGR_AVG_Y: AggregateFunctionSet = AggregateFunctionSet {
    name: "regr_avgy",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Aggregate,
        description: "Compute the average of the dependent variable ('y').",
        arguments: &["y", "x"],
        example: None,
    }),
    functions: &[RawAggregateFunction::new(
        &Signature::new(
            &[DataTypeId::Float64, DataTypeId::Float64],
            DataTypeId::Float64,
        ),
        &SimpleBinaryAggregate::new(&RegrAvgY),
    )],
};

pub const FUNCTION_SET_REGR_AVG_X: AggregateFunctionSet = AggregateFunctionSet {
    name: "regr_avgx",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Aggregate,
        description: "Compute the average of the independent variable ('x').",
        arguments: &["y", "x"],
        example: None,
    }),
    functions: &[RawAggregateFunction::new(
        &Signature::new(
            &[DataTypeId::Float64, DataTypeId::Float64],
            DataTypeId::Float64,
        ),
        &SimpleBinaryAggregate::new(&RegrAvgX),
    )],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegrAvgY;

impl BinaryAggregate for RegrAvgY {
    type Input1 = PhysicalF64;
    type Input2 = PhysicalF64;
    type Output = PhysicalF64;

    type BindState = ();
    type GroupState = RegrAvgState<Self>;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Float64,
            inputs,
        })
    }

    fn new_aggregate_state(_state: &Self::BindState) -> Self::GroupState {
        Default::default()
    }
}

impl RegrAvgInput for RegrAvgY {
    fn input(vals: (f64, f64)) -> f64 {
        // 'y' in (y, x)
        vals.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegrAvgX;

impl BinaryAggregate for RegrAvgX {
    type Input1 = PhysicalF64;
    type Input2 = PhysicalF64;
    type Output = PhysicalF64;

    type BindState = ();
    type GroupState = RegrAvgState<Self>;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Float64,
            inputs,
        })
    }

    fn new_aggregate_state(_state: &Self::BindState) -> Self::GroupState {
        Default::default()
    }
}

impl RegrAvgInput for RegrAvgX {
    fn input(vals: (f64, f64)) -> f64 {
        // 'x' in (y, x)
        vals.1
    }
}

pub trait RegrAvgInput: Sync + Send + Debug + 'static {
    fn input(vals: (f64, f64)) -> f64;
}

#[derive(Debug, Clone, Copy)]
pub struct RegrAvgState<F>
where
    F: RegrAvgInput,
{
    sum: f64,
    count: i64,
    _input: PhantomData<F>,
}

impl<F> Default for RegrAvgState<F>
where
    F: RegrAvgInput,
{
    fn default() -> Self {
        RegrAvgState {
            sum: 0.0,
            count: 0,
            _input: PhantomData,
        }
    }
}

impl<F> AggregateState<(&f64, &f64), f64> for RegrAvgState<F>
where
    F: RegrAvgInput,
{
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.count += other.count;
        self.sum += other.sum;
        Ok(())
    }

    fn update(&mut self, (&y, &x): (&f64, &f64)) -> Result<()> {
        self.sum += F::input((y, x));
        self.count += 1;
        Ok(())
    }

    fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = f64>,
    {
        if self.count == 0 {
            output.put_null();
        } else {
            let v = self.sum / self.count as f64;
            output.put(&v);
        }
        Ok(())
    }
}
