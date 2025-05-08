use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::aggregate::{AggregateFunction, RawAggregateFunction};
use crate::functions::bind_state::BindState;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::AggregateFunctionSet;

pub const FUNCTION_SET_APPROX_COUNT_DISTINCT: AggregateFunctionSet = AggregateFunctionSet {
    name: "approx_count_distinct",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Aggregate,
        description: "Return an estimated number of distinct value in the input.",
        arguments: &["input"],
        example: None,
    }],
    functions: &[RawAggregateFunction::new(
        &Signature::new(&[DataTypeId::Any], DataTypeId::Int64),
        &ApproxCountDistinct,
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct ApproxCountDistinct;

impl AggregateFunction for ApproxCountDistinct {
    type BindState = ();
    type GroupState = ApproxDistinctState;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        Ok(BindState {
            state: (),
            return_type: DataType::int64(),
            inputs,
        })
    }

    fn new_aggregate_state(state: &Self::BindState) -> Self::GroupState {
        unimplemented!()
    }

    fn update(
        state: &Self::BindState,
        inputs: &[Array],
        num_rows: usize,
        states: &mut [*mut Self::GroupState],
    ) -> Result<()> {
        unimplemented!()
    }

    fn combine(
        state: &Self::BindState,
        src: &mut [&mut Self::GroupState],
        dest: &mut [&mut Self::GroupState],
    ) -> Result<()> {
        unimplemented!()
    }

    fn finalize(
        state: &Self::BindState,
        states: &mut [&mut Self::GroupState],
        output: &mut Array,
    ) -> Result<()> {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct ApproxDistinctState {}
