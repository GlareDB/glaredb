use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_error::Result;

use crate::arrays::array::buffer_manager::BufferManager;
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
use crate::functions::aggregate::states::{
    drain,
    unary_update2,
    AggregateGroupStates,
    TypedAggregateGroupStates,
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
pub struct RegrCount;

impl FunctionInfo for RegrCount {
    fn name(&self) -> &'static str {
        "regr_count"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Float64, DataTypeId::Float64],
            variadic_arg: None,
            return_type: DataTypeId::Int64,
            doc: Some(&Documentation {
                category: Category::Aggregate,
                description: "Compute the count where both inputs are not NULL.",
                arguments: &["y", "x"],
                example: None,
            }),
        }]
    }
}

impl AggregateFunction for RegrCount {
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
                function_impl: Box::new(RegrCountImpl::<PhysicalF64>::new()),
            }),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegrCountImpl<S> {
    _s: PhantomData<S>,
}

impl<S> RegrCountImpl<S> {
    const fn new() -> Self {
        RegrCountImpl { _s: PhantomData }
    }
}

impl<S> AggregateFunctionImpl2 for RegrCountImpl<S>
where
    S: ScalarStorage,
{
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        Box::new(TypedAggregateGroupStates::new(
            RegrCountState::<S>::default,
            unary_update2::<S, PhysicalI64, _>,
            drain::<PhysicalI64, _, _>,
        ))
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

impl<S> AggregateState<&S::StorageType, i64> for RegrCountState<S>
where
    S: ScalarStorage,
{
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.count += other.count;
        Ok(())
    }

    fn update(&mut self, _input: &S::StorageType) -> Result<()> {
        self.count += 1;
        Ok(())
    }

    fn finalize<M, B>(&mut self, output: PutBuffer<M, B>) -> Result<()>
    where
        M: AddressableMut<B, T = i64>,
        B: BufferManager,
    {
        output.put(&self.count);
        Ok(())
    }
}
