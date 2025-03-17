use std::fmt::Debug;
use std::marker::PhantomData;

use glaredb_error::Result;

use crate::arrays::array::physical_type::{AddressableMut, PhysicalF64};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::PutBuffer;
use crate::arrays::executor::aggregate::AggregateState;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::aggregate::RawAggregateFunction;
use crate::functions::aggregate::simple::{SimpleUnaryAggregate, UnaryAggregate};
use crate::functions::bind_state::BindState;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::AggregateFunctionSet;

pub const FUNCTION_SET_STDDEV_POP: AggregateFunctionSet = AggregateFunctionSet {
    name: "stddev_pop",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Aggregate,
        description: "Compute the population standard deviation.",
        arguments: &["inputs"],
        example: None,
    }),
    functions: &[RawAggregateFunction::new(
        &Signature::new(&[DataTypeId::Float64], DataTypeId::Float64),
        &SimpleUnaryAggregate::new(&Variance::<StddevPopFinalize>::new()),
    )],
};

pub const FUNCTION_SET_STDDEV_SAMP: AggregateFunctionSet = AggregateFunctionSet {
    name: "stddev_samp",
    aliases: &["stddev"],
    doc: Some(&Documentation {
        category: Category::Aggregate,
        description: "Compute the sample standard deviation.",
        arguments: &["inputs"],
        example: None,
    }),
    functions: &[RawAggregateFunction::new(
        &Signature::new(&[DataTypeId::Float64], DataTypeId::Float64),
        &SimpleUnaryAggregate::new(&Variance::<StddevSampFinalize>::new()),
    )],
};

pub const FUNCTION_SET_VAR_POP: AggregateFunctionSet = AggregateFunctionSet {
    name: "var_pop",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Aggregate,
        description: "Compute the population variance.",
        arguments: &["inputs"],
        example: None,
    }),
    functions: &[RawAggregateFunction::new(
        &Signature::new(&[DataTypeId::Float64], DataTypeId::Float64),
        &SimpleUnaryAggregate::new(&Variance::<VariancePopFinalize>::new()),
    )],
};

pub const FUNCTION_SET_VAR_SAMP: AggregateFunctionSet = AggregateFunctionSet {
    name: "var_samp",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Aggregate,
        description: "Compute the sample variance.",
        arguments: &["inputs"],
        example: None,
    }),
    functions: &[RawAggregateFunction::new(
        &Signature::new(&[DataTypeId::Float64], DataTypeId::Float64),
        &SimpleUnaryAggregate::new(&Variance::<VarianceSampFinalize>::new()),
    )],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Variance<F> {
    _f: PhantomData<F>,
}

impl<F> Variance<F> {
    pub const fn new() -> Self {
        Variance { _f: PhantomData }
    }
}

impl<F> UnaryAggregate for Variance<F>
where
    F: VarianceFinalize,
{
    type Input = PhysicalF64;
    type Output = PhysicalF64;

    type BindState = ();
    type GroupState = VarianceState<F>;

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

pub trait VarianceFinalize: Sync + Send + Copy + Debug + Default + 'static {
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
    type BindState = ();

    fn merge(&mut self, _state: &(), other: &mut Self) -> Result<()> {
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

    fn update(&mut self, _state: &(), &input: &f64) -> Result<()> {
        self.count += 1;
        let delta = input - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = input - self.mean;
        self.m2 += delta * delta2;

        Ok(())
    }

    fn finalize<M>(&mut self, _state: &(), output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = f64>,
    {
        match F::finalize(self.count, self.mean, self.m2) {
            Some(val) => output.put(&val),
            None => output.put_null(),
        }
        Ok(())
    }
}
