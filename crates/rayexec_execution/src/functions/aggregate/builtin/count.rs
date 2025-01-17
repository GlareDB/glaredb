use std::marker::PhantomData;

use rayexec_error::{not_implemented, Result};

use crate::arrays::array::physical_type::{
    AddressableMut,
    PhysicalBinary,
    PhysicalBool,
    PhysicalConstant,
    PhysicalDictionary,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalInterval,
    PhysicalList,
    PhysicalStorage,
    PhysicalType,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUntypedNull,
    PhysicalUtf8,
};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::aggregate::AggregateState;
use crate::arrays::executor::PutBuffer;
use crate::expr::{self, Expression};
use crate::functions::aggregate::states::{
    drain,
    unary_update,
    AggregateGroupStates,
    TypedAggregateGroupStates,
};
use crate::functions::aggregate::{
    AggregateFunction,
    AggregateFunctionImpl,
    PlannedAggregateFunction,
};
use crate::functions::documentation::{Category, Documentation};
use crate::functions::{plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Count;

impl Count {
    /// Returns a planned aggregate function representing `COUNT(*)`.
    pub fn count_star(&self) -> PlannedAggregateFunction {
        PlannedAggregateFunction {
            function: Box::new(*self),
            return_type: DataType::Int64,
            inputs: vec![expr::lit(true)],
            function_impl: Box::new(CountNonNullImpl::<PhysicalBool>::new()),
        }
    }
}

impl FunctionInfo for Count {
    fn name(&self) -> &'static str {
        "count"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Any],
            variadic_arg: None,
            return_type: DataTypeId::Int64,
            doc: Some(&Documentation {
                category: Category::Aggregate,
                description: "Return the count of non-NULL inputs.",
                arguments: &["input"],
                example: None,
            }),
        }]
    }
}

impl AggregateFunction for Count {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction> {
        plan_check_num_args(self, &inputs, 1)?;

        let function_impl: Box<dyn AggregateFunctionImpl> = match inputs[0]
            .datatype(table_list)?
            .physical_type()
        {
            PhysicalType::UntypedNull => Box::new(CountNonNullImpl::<PhysicalUntypedNull>::new()),
            PhysicalType::Boolean => Box::new(CountNonNullImpl::<PhysicalBool>::new()),
            PhysicalType::Int8 => Box::new(CountNonNullImpl::<PhysicalI8>::new()),
            PhysicalType::Int16 => Box::new(CountNonNullImpl::<PhysicalI16>::new()),
            PhysicalType::Int32 => Box::new(CountNonNullImpl::<PhysicalI32>::new()),
            PhysicalType::Int64 => Box::new(CountNonNullImpl::<PhysicalI64>::new()),
            PhysicalType::Int128 => Box::new(CountNonNullImpl::<PhysicalI128>::new()),
            PhysicalType::UInt8 => Box::new(CountNonNullImpl::<PhysicalU8>::new()),
            PhysicalType::UInt16 => Box::new(CountNonNullImpl::<PhysicalU16>::new()),
            PhysicalType::UInt32 => Box::new(CountNonNullImpl::<PhysicalU32>::new()),
            PhysicalType::UInt64 => Box::new(CountNonNullImpl::<PhysicalU64>::new()),
            PhysicalType::UInt128 => Box::new(CountNonNullImpl::<PhysicalU128>::new()),
            PhysicalType::Float16 => Box::new(CountNonNullImpl::<PhysicalF16>::new()),
            PhysicalType::Float32 => Box::new(CountNonNullImpl::<PhysicalF32>::new()),
            PhysicalType::Float64 => Box::new(CountNonNullImpl::<PhysicalF64>::new()),
            PhysicalType::Interval => Box::new(CountNonNullImpl::<PhysicalInterval>::new()),
            PhysicalType::Utf8 => Box::new(CountNonNullImpl::<PhysicalUtf8>::new()),
            PhysicalType::Binary => Box::new(CountNonNullImpl::<PhysicalBinary>::new()),
            PhysicalType::Dictionary => Box::new(CountNonNullImpl::<PhysicalDictionary>::new()),
            PhysicalType::Constant => Box::new(CountNonNullImpl::<PhysicalConstant>::new()),
            PhysicalType::List => Box::new(CountNonNullImpl::<PhysicalList>::new()),
            PhysicalType::Struct => not_implemented!("count struct"),
        };

        Ok(PlannedAggregateFunction {
            function: Box::new(*self),
            return_type: DataType::Int64,
            inputs,
            function_impl,
        })
    }
}

#[derive(Debug, Clone)]
pub struct CountNonNullImpl<S> {
    _s: PhantomData<S>,
}

impl<S> CountNonNullImpl<S> {
    const fn new() -> Self {
        CountNonNullImpl { _s: PhantomData }
    }
}

impl<S> AggregateFunctionImpl for CountNonNullImpl<S>
where
    S: PhysicalStorage,
{
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        Box::new(TypedAggregateGroupStates::new(
            CountNonNullState::<S>::default,
            unary_update::<S, PhysicalI64, _>,
            drain::<PhysicalI64, _, _>,
        ))
    }
}

#[derive(Debug, Default)]
pub struct CountNonNullState<S: PhysicalStorage> {
    count: i64,
    _s: PhantomData<S>,
}

impl<S> AggregateState<&S::StorageType, i64> for CountNonNullState<S>
where
    S: PhysicalStorage,
{
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.count += other.count;
        Ok(())
    }

    fn update(&mut self, _input: &S::StorageType) -> Result<()> {
        self.count += 1;
        Ok(())
    }

    fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = i64>,
    {
        output.put(&self.count);
        Ok(())
    }
}
