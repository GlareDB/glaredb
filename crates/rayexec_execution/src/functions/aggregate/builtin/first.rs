use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_error::{not_implemented, Result};

use crate::arrays::array::buffer_manager::BufferManager;
use crate::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
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
    PhysicalType,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUntypedNull,
    PhysicalUtf8,
};
use crate::arrays::datatype::DataTypeId;
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
use crate::functions::{plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct First;

impl FunctionInfo for First {
    fn name(&self) -> &'static str {
        "first"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Any],
            variadic_arg: None,
            return_type: DataTypeId::Any,
            doc: Some(&Documentation {
                category: Category::Aggregate,
                description: "Return the first non-NULL value.",
                arguments: &["input"],
                example: None,
            }),
        }]
    }
}

impl AggregateFunction for First {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction> {
        plan_check_num_args(self, &inputs, 1)?;

        let datatype = inputs[0].datatype(table_list)?;

        let function_impl: Box<dyn AggregateFunctionImpl2> = match datatype.physical_type() {
            PhysicalType::UntypedNull => Box::new(FirstPrimitiveImpl::<PhysicalUntypedNull>::new()),
            PhysicalType::Boolean => Box::new(FirstPrimitiveImpl::<PhysicalBool>::new()),
            PhysicalType::Int8 => Box::new(FirstPrimitiveImpl::<PhysicalI8>::new()),
            PhysicalType::Int16 => Box::new(FirstPrimitiveImpl::<PhysicalI16>::new()),
            PhysicalType::Int32 => Box::new(FirstPrimitiveImpl::<PhysicalI32>::new()),
            PhysicalType::Int64 => Box::new(FirstPrimitiveImpl::<PhysicalI64>::new()),
            PhysicalType::Int128 => Box::new(FirstPrimitiveImpl::<PhysicalI128>::new()),
            PhysicalType::UInt8 => Box::new(FirstPrimitiveImpl::<PhysicalU8>::new()),
            PhysicalType::UInt16 => Box::new(FirstPrimitiveImpl::<PhysicalU16>::new()),
            PhysicalType::UInt32 => Box::new(FirstPrimitiveImpl::<PhysicalU32>::new()),
            PhysicalType::UInt64 => Box::new(FirstPrimitiveImpl::<PhysicalU64>::new()),
            PhysicalType::UInt128 => Box::new(FirstPrimitiveImpl::<PhysicalU128>::new()),
            PhysicalType::Float16 => Box::new(FirstPrimitiveImpl::<PhysicalF16>::new()),
            PhysicalType::Float32 => Box::new(FirstPrimitiveImpl::<PhysicalF32>::new()),
            PhysicalType::Float64 => Box::new(FirstPrimitiveImpl::<PhysicalF64>::new()),
            PhysicalType::Interval => Box::new(FirstPrimitiveImpl::<PhysicalInterval>::new()),
            PhysicalType::Utf8 => Box::new(FirstStringImpl),
            PhysicalType::Binary => Box::new(FirstBinaryImpl),
            other => not_implemented!("FIRST for physical type: {other}"),
        };

        Ok(PlannedAggregateFunction {
            function: Box::new(*self),
            return_type: datatype,
            inputs,
            function_impl,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FirstPrimitiveImpl<S> {
    _s: PhantomData<S>,
}

impl<S> FirstPrimitiveImpl<S> {
    const fn new() -> Self {
        FirstPrimitiveImpl { _s: PhantomData }
    }
}

impl<S> AggregateFunctionImpl2 for FirstPrimitiveImpl<S>
where
    S: MutableScalarStorage,
    S::StorageType: Debug + Default + Copy,
{
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        Box::new(TypedAggregateGroupStates::new(
            FirstPrimitiveState::<S::StorageType>::default,
            unary_update2::<S, S, _>,
            drain::<S, _, _>,
        ))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FirstBinaryImpl;

impl AggregateFunctionImpl2 for FirstBinaryImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        Box::new(TypedAggregateGroupStates::new(
            FirstBinaryState::default,
            unary_update2::<PhysicalBinary, PhysicalBinary, _>,
            drain::<PhysicalBinary, _, _>,
        ))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FirstStringImpl;

impl AggregateFunctionImpl2 for FirstStringImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        Box::new(TypedAggregateGroupStates::new(
            FirstStringState::default,
            unary_update2::<PhysicalUtf8, PhysicalUtf8, _>,
            drain::<PhysicalUtf8, _, _>,
        ))
    }
}

#[derive(Debug, Default)]
pub struct FirstPrimitiveState<T> {
    value: Option<T>,
}

impl<T> AggregateState<&T, T> for FirstPrimitiveState<T>
where
    T: Debug + Default + Copy,
{
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        if self.value.is_none() {
            std::mem::swap(&mut self.value, &mut other.value);
        }
        Ok(())
    }

    fn update(&mut self, &input: &T) -> Result<()> {
        if self.value.is_none() {
            self.value = Some(input);
        }
        Ok(())
    }

    fn finalize<M, B>(&mut self, output: PutBuffer<M, B>) -> Result<()>
    where
        M: AddressableMut<B, T = T>,
        B: BufferManager,
    {
        match &self.value {
            Some(val) => output.put(val),
            None => output.put_null(),
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct FirstBinaryState {
    value: Option<Vec<u8>>,
}

impl AggregateState<&[u8], [u8]> for FirstBinaryState {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        if self.value.is_none() {
            std::mem::swap(&mut self.value, &mut other.value);
        }
        Ok(())
    }

    fn update(&mut self, input: &[u8]) -> Result<()> {
        if self.value.is_none() {
            self.value = Some(input.to_vec());
        }
        Ok(())
    }

    fn finalize<M, B>(&mut self, output: PutBuffer<M, B>) -> Result<()>
    where
        M: AddressableMut<B, T = [u8]>,
        B: BufferManager,
    {
        match &self.value {
            Some(val) => output.put(val),
            None => output.put_null(),
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct FirstStringState {
    value: Option<String>,
}

impl AggregateState<&str, str> for FirstStringState {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        if self.value.is_none() {
            std::mem::swap(&mut self.value, &mut other.value);
        }
        Ok(())
    }

    fn update(&mut self, input: &str) -> Result<()> {
        if self.value.is_none() {
            self.value = Some(input.to_string());
        }
        Ok(())
    }

    fn finalize<M, B>(&mut self, output: PutBuffer<M, B>) -> Result<()>
    where
        M: AddressableMut<B, T = str>,
        B: BufferManager,
    {
        match &self.value {
            Some(val) => output.put(val),
            None => output.put_null(),
        }
        Ok(())
    }
}
