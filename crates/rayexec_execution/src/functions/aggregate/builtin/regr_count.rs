use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_error::Result;

use crate::arrays::array::physical_type::{
    AddressableMut,
    PhysicalF64,
    PhysicalI64,
    ScalarStorage,
};
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

pub const FUNCTION_SET_REGR_COUNT: AggregateFunctionSet = AggregateFunctionSet {
    name: "regr_count",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Aggregate,
        description: "Compute the count where both inputs are not NULL.",
        arguments: &["y", "x"],
        example: None,
    }),
    functions: &[RawAggregateFunction::new(
        &Signature::new(
            &[DataTypeId::Float64, DataTypeId::Float64],
            DataTypeId::Int64,
        ),
        &SimpleBinaryAggregate::new(&RegrCount),
    )],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegrCount;

impl BinaryAggregate for RegrCount {
    type Input1 = PhysicalF64;
    type Input2 = PhysicalF64;
    type Output = PhysicalI64;

    type BindState = ();
    type GroupState = RegrCountState<PhysicalF64>;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Int64,
            inputs,
        })
    }

    fn new_aggregate_state(_state: &Self::BindState) -> Self::GroupState {
        Default::default()
    }
}

/// State for `regr_count`.
///
/// Note that this can be used for any input type, but the sql function we
/// expose only accepts f64 (to match Postgres).
#[derive(Debug, Clone, Copy, Default)]
pub struct RegrCountState<S> {
    count: i64,
    _s: PhantomData<S>,
}

impl<S> AggregateState<(&S::StorageType, &S::StorageType), i64> for RegrCountState<S>
where
    S: ScalarStorage,
{
    type BindState = ();

    fn merge(&mut self, _state: &(), other: &mut Self) -> Result<()> {
        self.count += other.count;
        Ok(())
    }

    fn update(&mut self, _state: &(), _input: (&S::StorageType, &S::StorageType)) -> Result<()> {
        self.count += 1;
        Ok(())
    }

    fn finalize<M>(&mut self, _state: &(), output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = i64>,
    {
        output.put(&self.count);
        Ok(())
    }
}
