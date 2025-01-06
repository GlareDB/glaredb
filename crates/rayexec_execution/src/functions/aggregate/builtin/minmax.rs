use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_error::{not_implemented, Result};

use crate::arrays::buffer::physical_type::{
    AddressableMut,
    MutablePhysicalStorage,
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
use crate::arrays::executor_exp::aggregate::AggregateState;
use crate::arrays::executor_exp::PutBuffer;
use crate::expr::Expression;
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
pub struct Min;

impl FunctionInfo for Min {
    fn name(&self) -> &'static str {
        "min"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Any],
            variadic_arg: None,
            return_type: DataTypeId::Any,
            doc: Some(&Documentation {
                category: Category::Aggregate,
                description: "Return the minimum non-NULL value seen from input.",
                arguments: &["input"],
                example: None,
            }),
        }]
    }
}

impl AggregateFunction for Min {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction> {
        plan_check_num_args(self, &inputs, 1)?;

        let datatype = inputs[0].datatype(table_list)?;

        let function_impl: Box<dyn AggregateFunctionImpl> = match datatype.physical_type() {
            PhysicalType::UntypedNull => Box::new(MinPrimitiveImpl::<PhysicalUntypedNull>::new()),
            PhysicalType::Boolean => Box::new(MinPrimitiveImpl::<PhysicalBool>::new()),
            PhysicalType::Int8 => Box::new(MinPrimitiveImpl::<PhysicalI8>::new()),
            PhysicalType::Int16 => Box::new(MinPrimitiveImpl::<PhysicalI16>::new()),
            PhysicalType::Int32 => Box::new(MinPrimitiveImpl::<PhysicalI32>::new()),
            PhysicalType::Int64 => Box::new(MinPrimitiveImpl::<PhysicalI64>::new()),
            PhysicalType::Int128 => Box::new(MinPrimitiveImpl::<PhysicalI128>::new()),
            PhysicalType::UInt8 => Box::new(MinPrimitiveImpl::<PhysicalU8>::new()),
            PhysicalType::UInt16 => Box::new(MinPrimitiveImpl::<PhysicalU16>::new()),
            PhysicalType::UInt32 => Box::new(MinPrimitiveImpl::<PhysicalU32>::new()),
            PhysicalType::UInt64 => Box::new(MinPrimitiveImpl::<PhysicalU64>::new()),
            PhysicalType::UInt128 => Box::new(MinPrimitiveImpl::<PhysicalU128>::new()),
            PhysicalType::Float16 => Box::new(MinPrimitiveImpl::<PhysicalF16>::new()),
            PhysicalType::Float32 => Box::new(MinPrimitiveImpl::<PhysicalF32>::new()),
            PhysicalType::Float64 => Box::new(MinPrimitiveImpl::<PhysicalF64>::new()),
            PhysicalType::Interval => Box::new(MinPrimitiveImpl::<PhysicalInterval>::new()),
            PhysicalType::Utf8 => Box::new(MinStringImpl),
            PhysicalType::Binary => Box::new(MinBinaryImpl),
            other => not_implemented!("max for type {other:?}"),
        };

        Ok(PlannedAggregateFunction {
            function: Box::new(*self),
            return_type: datatype,
            inputs,
            function_impl,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Max;

impl FunctionInfo for Max {
    fn name(&self) -> &'static str {
        "max"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Any],
            variadic_arg: None,
            return_type: DataTypeId::Any,
            doc: Some(&Documentation {
                category: Category::Aggregate,
                description: "Return the maximum non-NULL value seen from input.",
                arguments: &["input"],
                example: None,
            }),
        }]
    }
}

impl AggregateFunction for Max {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction> {
        plan_check_num_args(self, &inputs, 1)?;

        let datatype = inputs[0].datatype(table_list)?;

        let function_impl: Box<dyn AggregateFunctionImpl> = match datatype.physical_type() {
            PhysicalType::UntypedNull => Box::new(MaxPrimitiveImpl::<PhysicalUntypedNull>::new()),
            PhysicalType::Boolean => Box::new(MaxPrimitiveImpl::<PhysicalBool>::new()),
            PhysicalType::Int8 => Box::new(MaxPrimitiveImpl::<PhysicalI8>::new()),
            PhysicalType::Int16 => Box::new(MaxPrimitiveImpl::<PhysicalI16>::new()),
            PhysicalType::Int32 => Box::new(MaxPrimitiveImpl::<PhysicalI32>::new()),
            PhysicalType::Int64 => Box::new(MaxPrimitiveImpl::<PhysicalI64>::new()),
            PhysicalType::Int128 => Box::new(MaxPrimitiveImpl::<PhysicalI128>::new()),
            PhysicalType::UInt8 => Box::new(MaxPrimitiveImpl::<PhysicalU8>::new()),
            PhysicalType::UInt16 => Box::new(MaxPrimitiveImpl::<PhysicalU16>::new()),
            PhysicalType::UInt32 => Box::new(MaxPrimitiveImpl::<PhysicalU32>::new()),
            PhysicalType::UInt64 => Box::new(MaxPrimitiveImpl::<PhysicalU64>::new()),
            PhysicalType::UInt128 => Box::new(MaxPrimitiveImpl::<PhysicalU128>::new()),
            PhysicalType::Float16 => Box::new(MaxPrimitiveImpl::<PhysicalF16>::new()),
            PhysicalType::Float32 => Box::new(MaxPrimitiveImpl::<PhysicalF32>::new()),
            PhysicalType::Float64 => Box::new(MaxPrimitiveImpl::<PhysicalF64>::new()),
            PhysicalType::Interval => Box::new(MaxPrimitiveImpl::<PhysicalInterval>::new()),
            PhysicalType::Utf8 => Box::new(MaxStringImpl),
            PhysicalType::Binary => Box::new(MaxBinaryImpl),
            other => not_implemented!("max for type {other:?}"),
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
pub struct MaxPrimitiveImpl<S> {
    _s: PhantomData<S>,
}

impl<S> MaxPrimitiveImpl<S> {
    const fn new() -> Self {
        MaxPrimitiveImpl { _s: PhantomData }
    }
}

impl<S> AggregateFunctionImpl for MaxPrimitiveImpl<S>
where
    S: MutablePhysicalStorage,
    S::StorageType: Default + Debug + Sync + Send + PartialOrd + Copy,
{
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        Box::new(TypedAggregateGroupStates::new(
            MaxStatePrimitive::<S::StorageType>::default,
            unary_update::<S, S, _>,
            drain::<S, _, _>,
        ))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MaxBinaryImpl;

impl AggregateFunctionImpl for MaxBinaryImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        Box::new(TypedAggregateGroupStates::new(
            MaxStateBinary::default,
            unary_update::<PhysicalBinary, PhysicalBinary, _>,
            drain::<PhysicalBinary, _, _>,
        ))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MaxStringImpl;

impl AggregateFunctionImpl for MaxStringImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        Box::new(TypedAggregateGroupStates::new(
            MaxStateString::default,
            unary_update::<PhysicalUtf8, PhysicalUtf8, _>,
            drain::<PhysicalUtf8, _, _>,
        ))
    }
}

#[derive(Debug, Default)]
pub struct MaxStatePrimitive<T> {
    max: T,
    valid: bool,
}

impl<T> AggregateState<&T, T> for MaxStatePrimitive<T>
where
    T: Debug + Sync + Send + PartialOrd + Copy,
{
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        if !self.valid {
            self.valid = other.valid;
            std::mem::swap(&mut self.max, &mut other.max);
            return Ok(());
        }

        if self.max.lt(&other.max) {
            std::mem::swap(&mut self.max, &mut other.max);
        }

        Ok(())
    }

    fn update(&mut self, input: &T) -> Result<()> {
        if !self.valid {
            self.max = *input;
            return Ok(());
        }

        if self.max.lt(input) {
            self.max = *input;
        }

        Ok(())
    }

    fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = T>,
    {
        if self.valid {
            output.put(&self.max);
        } else {
            output.put_null();
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct MaxStateBinary {
    max: Vec<u8>,
    valid: bool,
}

impl AggregateState<&[u8], [u8]> for MaxStateBinary {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        if !self.valid {
            self.valid = other.valid;
            std::mem::swap(&mut self.max, &mut other.max);
            return Ok(());
        }

        if self.max.lt(&other.max) {
            std::mem::swap(&mut self.max, &mut other.max);
        }

        Ok(())
    }

    fn update(&mut self, input: &[u8]) -> Result<()> {
        if !self.valid {
            self.max = input.to_vec();
            return Ok(());
        }

        if self.max.as_slice().lt(input) {
            self.max = input.to_vec();
        }

        Ok(())
    }

    fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = [u8]>,
    {
        if self.valid {
            output.put(&self.max);
        } else {
            output.put_null();
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct MaxStateString {
    max: String,
    valid: bool,
}

impl AggregateState<&str, str> for MaxStateString {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        if !self.valid {
            self.valid = other.valid;
            std::mem::swap(&mut self.max, &mut other.max);
            return Ok(());
        }

        if self.max.lt(&other.max) {
            std::mem::swap(&mut self.max, &mut other.max);
        }

        Ok(())
    }

    fn update(&mut self, input: &str) -> Result<()> {
        if !self.valid {
            self.max = input.to_string();
            return Ok(());
        }

        if self.max.as_str().lt(input) {
            self.max = input.to_string();
        }

        Ok(())
    }

    fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = str>,
    {
        if self.valid {
            output.put(&self.max);
        } else {
            output.put_null();
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MinPrimitiveImpl<S> {
    _s: PhantomData<S>,
}

impl<S> MinPrimitiveImpl<S> {
    const fn new() -> Self {
        MinPrimitiveImpl { _s: PhantomData }
    }
}

impl<S> AggregateFunctionImpl for MinPrimitiveImpl<S>
where
    S: MutablePhysicalStorage,
    S::StorageType: Default + Debug + Sync + Send + PartialOrd + Copy,
{
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        Box::new(TypedAggregateGroupStates::new(
            MinStatePrimitive::<S::StorageType>::default,
            unary_update::<S, S, _>,
            drain::<S, _, _>,
        ))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MinBinaryImpl;

impl AggregateFunctionImpl for MinBinaryImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        Box::new(TypedAggregateGroupStates::new(
            MinStateBinary::default,
            unary_update::<PhysicalBinary, PhysicalBinary, _>,
            drain::<PhysicalBinary, _, _>,
        ))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MinStringImpl;

impl AggregateFunctionImpl for MinStringImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        Box::new(TypedAggregateGroupStates::new(
            MinStateString::default,
            unary_update::<PhysicalUtf8, PhysicalUtf8, _>,
            drain::<PhysicalUtf8, _, _>,
        ))
    }
}

#[derive(Debug, Default)]
pub struct MinStatePrimitive<T> {
    min: T,
    valid: bool,
}

impl<T> AggregateState<&T, T> for MinStatePrimitive<T>
where
    T: Debug + Sync + Send + PartialOrd + Copy,
{
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        if !self.valid {
            self.valid = other.valid;
            std::mem::swap(&mut self.min, &mut other.min);
            return Ok(());
        }

        if self.min.gt(&other.min) {
            std::mem::swap(&mut self.min, &mut other.min);
        }

        Ok(())
    }

    fn update(&mut self, input: &T) -> Result<()> {
        if !self.valid {
            self.min = *input;
            return Ok(());
        }

        if self.min.gt(input) {
            self.min = *input;
        }

        Ok(())
    }

    fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = T>,
    {
        if self.valid {
            output.put(&self.min);
        } else {
            output.put_null();
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct MinStateBinary {
    min: Vec<u8>,
    valid: bool,
}

impl AggregateState<&[u8], [u8]> for MinStateBinary {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        if !self.valid {
            self.valid = other.valid;
            std::mem::swap(&mut self.min, &mut other.min);
            return Ok(());
        }

        if self.min.gt(&other.min) {
            std::mem::swap(&mut self.min, &mut other.min);
        }

        Ok(())
    }

    fn update(&mut self, input: &[u8]) -> Result<()> {
        if !self.valid {
            self.min = input.to_vec();
            return Ok(());
        }

        if self.min.as_slice().gt(input) {
            self.min = input.to_vec();
        }

        Ok(())
    }

    fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = [u8]>,
    {
        if self.valid {
            output.put(&self.min);
        } else {
            output.put_null();
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct MinStateString {
    min: String,
    valid: bool,
}

impl AggregateState<&str, str> for MinStateString {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        if !self.valid {
            self.valid = other.valid;
            std::mem::swap(&mut self.min, &mut other.min);
            return Ok(());
        }

        if self.min.gt(&other.min) {
            std::mem::swap(&mut self.min, &mut other.min);
        }

        Ok(())
    }

    fn update(&mut self, input: &str) -> Result<()> {
        if !self.valid {
            self.min = input.to_string();
            return Ok(());
        }

        if self.min.as_str().gt(input) {
            self.min = input.to_string();
        }

        Ok(())
    }

    fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = str>,
    {
        if self.valid {
            output.put(&self.min);
        } else {
            output.put_null();
        }

        Ok(())
    }
}
