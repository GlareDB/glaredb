use std::fmt::Debug;

use rayexec_error::{not_implemented, Result};

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
use crate::buffer::buffer_manager::BufferManager;
use crate::expr::Expression;
use crate::functions::aggregate::states::{AggregateFunctionImpl, UnaryStateLogic};
use crate::functions::aggregate::{AggregateFunction2, PlannedAggregateFunction2};
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

impl AggregateFunction2 for First {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction2> {
        plan_check_num_args(self, &inputs, 1)?;

        let datatype = inputs[0].datatype()?;

        let function_impl = match datatype.physical_type() {
            PhysicalType::UntypedNull => create_primitive_impl::<PhysicalUntypedNull>(),
            PhysicalType::Boolean => create_primitive_impl::<PhysicalBool>(),
            PhysicalType::Int8 => create_primitive_impl::<PhysicalI8>(),
            PhysicalType::Int16 => create_primitive_impl::<PhysicalI16>(),
            PhysicalType::Int32 => create_primitive_impl::<PhysicalI32>(),
            PhysicalType::Int64 => create_primitive_impl::<PhysicalI64>(),
            PhysicalType::Int128 => create_primitive_impl::<PhysicalI128>(),
            PhysicalType::UInt8 => create_primitive_impl::<PhysicalU8>(),
            PhysicalType::UInt16 => create_primitive_impl::<PhysicalU16>(),
            PhysicalType::UInt32 => create_primitive_impl::<PhysicalU32>(),
            PhysicalType::UInt64 => create_primitive_impl::<PhysicalU64>(),
            PhysicalType::UInt128 => create_primitive_impl::<PhysicalU128>(),
            PhysicalType::Float16 => create_primitive_impl::<PhysicalF16>(),
            PhysicalType::Float32 => create_primitive_impl::<PhysicalF32>(),
            PhysicalType::Float64 => create_primitive_impl::<PhysicalF64>(),
            PhysicalType::Interval => create_primitive_impl::<PhysicalInterval>(),
            PhysicalType::Utf8 => AggregateFunctionImpl::new::<
                UnaryStateLogic<FirstStringState, PhysicalUtf8, PhysicalUtf8>,
            >(None),
            PhysicalType::Binary => AggregateFunctionImpl::new::<
                UnaryStateLogic<FirstBinaryState, PhysicalBinary, PhysicalBinary>,
            >(None),

            other => not_implemented!("FIRST for physical type: {other}"),
        };

        Ok(PlannedAggregateFunction2 {
            function: Box::new(*self),
            return_type: datatype,
            inputs,
            function_impl,
        })
    }
}

fn create_primitive_impl<S>() -> AggregateFunctionImpl
where
    S: MutableScalarStorage,
    S::StorageType: Copy + Sized + Default + Debug,
{
    AggregateFunctionImpl::new::<UnaryStateLogic<FirstPrimitiveState<S::StorageType>, S, S>>(None)
}

#[derive(Debug, Default)]
pub struct FirstPrimitiveState<T> {
    value: Option<T>,
}

impl<T> AggregateState<&T, T> for FirstPrimitiveState<T>
where
    T: Debug + Default + Copy + Sync + Send,
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

    fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = T>,
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

    fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = [u8]>,
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

    fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = str>,
    {
        match &self.value {
            Some(val) => output.put(val),
            None => output.put_null(),
        }
        Ok(())
    }
}
