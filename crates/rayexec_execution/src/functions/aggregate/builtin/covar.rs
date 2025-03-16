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

pub const FUNCTION_SET_COVAR_POP: AggregateFunctionSet = AggregateFunctionSet {
    name: "covar_pop",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Aggregate,
        description: "Compute population covariance.",
        arguments: &["y", "x"],
        example: None,
    }),
    functions: &[RawAggregateFunction::new(
        &Signature::new(
            &[DataTypeId::Float64, DataTypeId::Float64],
            DataTypeId::Float64,
        ),
        &SimpleBinaryAggregate::new(&CovarPop),
    )],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CovarPop;

impl BinaryAggregate for CovarPop {
    type Input1 = PhysicalF64;
    type Input2 = PhysicalF64;
    type Output = PhysicalF64;

    type BindState = ();
    type GroupState = CovarState<CovarPopFinalize>;

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

pub const FUNCTION_SET_COVAR_SAMP: AggregateFunctionSet = AggregateFunctionSet {
    name: "covar_samp",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Aggregate,
        description: "Compute sample covariance.",
        arguments: &["y", "x"],
        example: None,
    }),
    functions: &[RawAggregateFunction::new(
        &Signature::new(
            &[DataTypeId::Float64, DataTypeId::Float64],
            DataTypeId::Float64,
        ),
        &SimpleBinaryAggregate::new(&CovarSamp),
    )],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CovarSamp;

impl BinaryAggregate for CovarSamp {
    type Input1 = PhysicalF64;
    type Input2 = PhysicalF64;
    type Output = PhysicalF64;

    type BindState = ();
    type GroupState = CovarState<CovarSampFinalize>;

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
    type BindState = ();

    fn merge(&mut self, _state: &(), other: &mut Self) -> Result<()> {
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
    fn update(&mut self, _state: &(), (&y, &x): (&f64, &f64)) -> Result<()> {
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

    fn finalize<M>(&mut self, _state: &(), output: PutBuffer<M>) -> Result<()>
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
