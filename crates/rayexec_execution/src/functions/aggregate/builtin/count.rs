use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_error::{not_implemented, Result};

use crate::arrays::array::physical_type::{
    AddressableMut,
    PhysicalBinary,
    PhysicalBool,
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
    PhysicalType,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUntypedNull,
    PhysicalUtf8,
    ScalarStorage,
};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::aggregate::AggregateState;
use crate::arrays::executor::PutBuffer;
use crate::buffer::buffer_manager::BufferManager;
use crate::expr::{self, Expression};
use crate::functions::aggregate::simple::{SimpleUnaryAggregate, UnaryAggregate};
use crate::functions::aggregate::states::{AggregateFunctionImpl, UnaryStateLogic};
use crate::functions::aggregate::{
    AggregateFunction2,
    PlannedAggregateFunction2,
    RawAggregateFunction,
};
use crate::functions::bind_state::BindState;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::AggregateFunctionSet;
use crate::functions::{plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Count;

impl Count {
    /// Returns a planned aggregate function representing `COUNT(*)`.
    pub fn count_star(&self) -> PlannedAggregateFunction2 {
        PlannedAggregateFunction2 {
            function: Box::new(*self),
            return_type: DataType::Int64,
            inputs: vec![expr::lit(true).into()],
            function_impl: create_impl::<PhysicalBool>(),
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

impl AggregateFunction2 for Count {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction2> {
        plan_check_num_args(self, &inputs, 1)?;

        let function_impl = match inputs[0].datatype()?.physical_type() {
            PhysicalType::UntypedNull => create_impl::<PhysicalUntypedNull>(),
            PhysicalType::Boolean => create_impl::<PhysicalBool>(),
            PhysicalType::Int8 => create_impl::<PhysicalI8>(),
            PhysicalType::Int16 => create_impl::<PhysicalI16>(),
            PhysicalType::Int32 => create_impl::<PhysicalI32>(),
            PhysicalType::Int64 => create_impl::<PhysicalI64>(),
            PhysicalType::Int128 => create_impl::<PhysicalI128>(),
            PhysicalType::UInt8 => create_impl::<PhysicalU8>(),
            PhysicalType::UInt16 => create_impl::<PhysicalU16>(),
            PhysicalType::UInt32 => create_impl::<PhysicalU32>(),
            PhysicalType::UInt64 => create_impl::<PhysicalU64>(),
            PhysicalType::UInt128 => create_impl::<PhysicalU128>(),
            PhysicalType::Float16 => create_impl::<PhysicalF16>(),
            PhysicalType::Float32 => create_impl::<PhysicalF32>(),
            PhysicalType::Float64 => create_impl::<PhysicalF64>(),
            PhysicalType::Interval => create_impl::<PhysicalInterval>(),
            PhysicalType::Utf8 => create_impl::<PhysicalUtf8>(),
            PhysicalType::Binary => create_impl::<PhysicalBinary>(),
            PhysicalType::List => create_impl::<PhysicalList>(),
            PhysicalType::Struct => not_implemented!("count struct"),
        };

        Ok(PlannedAggregateFunction2 {
            function: Box::new(*self),
            return_type: DataType::Int64,
            inputs,
            function_impl,
        })
    }
}

fn create_impl<S>() -> AggregateFunctionImpl
where
    S: ScalarStorage,
{
    AggregateFunctionImpl::new::<UnaryStateLogic<CountNonNullState<S>, S, PhysicalI64>>(None)
}

pub const FUNCTION_SET_COUNT: AggregateFunctionSet = AggregateFunctionSet {
    name: "count",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Aggregate,
        description: "Return the count of non-NULL inputs.",
        arguments: &["input"],
        example: None,
    }),
    functions: &[RawAggregateFunction::new(
        &Signature::new(&[DataTypeId::Boolean], DataTypeId::Int64),
        &SimpleUnaryAggregate::new(&CountTest::<PhysicalBool>::new()),
    )],
};

#[derive(Debug)]
pub struct CountTest<S: ScalarStorage> {
    _s: PhantomData<S>,
}

impl<S> CountTest<S>
where
    S: ScalarStorage,
{
    pub const fn new() -> Self {
        CountTest { _s: PhantomData }
    }
}

impl<S> UnaryAggregate for CountTest<S>
where
    S: ScalarStorage,
{
    type Input = S;
    type Output = PhysicalI64;

    type BindState = ();
    type GroupState = CountNonNullState<S>;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Int64,
            inputs,
        })
    }

    fn new_aggregate_state(_state: &Self::BindState) -> Self::GroupState {
        CountNonNullState::default()
    }
}

#[derive(Debug, Default)]
pub struct CountNonNullState<S: ScalarStorage> {
    count: i64,
    _s: PhantomData<S>,
}

impl<S> AggregateState<&S::StorageType, i64> for CountNonNullState<S>
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

    fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = i64>,
    {
        output.put(&self.count);
        Ok(())
    }
}
