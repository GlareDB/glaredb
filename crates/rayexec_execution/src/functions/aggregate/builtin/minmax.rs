use std::borrow::Borrow;
use std::fmt::Debug;
use std::marker::PhantomData;

use half::f16;
use rayexec_error::{not_implemented, Result};

use crate::arrays::array::ArrayData2;
use crate::arrays::buffer::physical_type::{
    AddressableMut,
    MutablePhysicalStorage,
    PhysicalBinary,
    PhysicalBool,
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
use crate::arrays::executor::aggregate::{AggregateState2, StateFinalizer};
use crate::arrays::executor::builder::{ArrayBuilder, GermanVarlenBuffer};
use crate::arrays::executor::physical_type::{
    PhysicalBinary_2,
    PhysicalBool_2,
    PhysicalF16_2,
    PhysicalF32_2,
    PhysicalF64_2,
    PhysicalI128_2,
    PhysicalI16_2,
    PhysicalI32_2,
    PhysicalI64_2,
    PhysicalI8_2,
    PhysicalInterval_2,
    PhysicalStorage2,
    PhysicalType2,
    PhysicalU128_2,
    PhysicalU16_2,
    PhysicalU32_2,
    PhysicalU64_2,
    PhysicalU8_2,
    PhysicalUntypedNull_2,
};
use crate::arrays::executor_exp::aggregate::AggregateState;
use crate::arrays::executor_exp::PutBuffer;
use crate::arrays::scalar::interval::Interval;
use crate::arrays::storage::{PrimitiveStorage, UntypedNull};
use crate::expr::Expression;
use crate::functions::aggregate::states::{
    boolean_finalize,
    drain,
    new_unary_aggregate_states2,
    primitive_finalize,
    unary_update,
    untyped_null_finalize,
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

        unimplemented!()
        // let function_impl: Box<dyn AggregateFunctionImpl> = match datatype.physical_type2()? {
        //     PhysicalType2::UntypedNull => Box::new(MinMaxUntypedNull),
        //     PhysicalType2::Boolean => Box::new(MinBoolImpl::new()),
        //     PhysicalType2::Float16 => Box::new(MinPrimitiveImpl::<PhysicalF16_2, f16>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::Float32 => Box::new(MinPrimitiveImpl::<PhysicalF32_2, f32>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::Float64 => Box::new(MinPrimitiveImpl::<PhysicalF64_2, f64>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::Int8 => {
        //         Box::new(MinPrimitiveImpl::<PhysicalI8_2, i8>::new(datatype.clone()))
        //     }
        //     PhysicalType2::Int16 => Box::new(MinPrimitiveImpl::<PhysicalI16_2, i16>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::Int32 => Box::new(MinPrimitiveImpl::<PhysicalI32_2, i32>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::Int64 => Box::new(MinPrimitiveImpl::<PhysicalI64_2, i64>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::Int128 => Box::new(MinPrimitiveImpl::<PhysicalI128_2, i128>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::UInt8 => {
        //         Box::new(MinPrimitiveImpl::<PhysicalU8_2, u8>::new(datatype.clone()))
        //     }
        //     PhysicalType2::UInt16 => Box::new(MinPrimitiveImpl::<PhysicalU16_2, u16>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::UInt32 => Box::new(MinPrimitiveImpl::<PhysicalU32_2, u32>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::UInt64 => Box::new(MinPrimitiveImpl::<PhysicalU64_2, u64>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::UInt128 => Box::new(MinPrimitiveImpl::<PhysicalU128_2, u128>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::Interval => Box::new(
        //         MinPrimitiveImpl::<PhysicalInterval_2, Interval>::new(datatype.clone()),
        //     ),
        //     PhysicalType2::Binary => Box::new(MinBinaryImpl::new(datatype.clone())),
        //     PhysicalType2::Utf8 => Box::new(MinBinaryImpl::new(datatype.clone())),
        //     PhysicalType2::List => {
        //         not_implemented!("MIN for list arrays")
        //     }
        // };

        // Ok(PlannedAggregateFunction {
        //     function: Box::new(*self),
        //     return_type: datatype,
        //     inputs,
        //     function_impl,
        // })
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
            // PhysicalType::Utf8 => Box::new(MaxImpl::<PhysicalUtf8>::new()),
            // PhysicalType::Binary => Box::new(MaxImpl::<PhysicalBinary>::new()),
            other => not_implemented!("max for type {other:?}"),
        };

        // let function_impl: Box<dyn AggregateFunctionImpl> = match datatype.physical_type2()? {
        //     PhysicalType2::UntypedNull => Box::new(MinMaxUntypedNull),
        //     PhysicalType2::Boolean => Box::new(MaxBoolImpl::new()),
        //     PhysicalType2::Float16 => Box::new(MaxPrimitiveImpl::<PhysicalF16_2, f16>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::Float32 => Box::new(MaxPrimitiveImpl::<PhysicalF32_2, f32>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::Float64 => Box::new(MaxPrimitiveImpl::<PhysicalF64_2, f64>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::Int8 => {
        //         Box::new(MaxPrimitiveImpl::<PhysicalI8_2, i8>::new(datatype.clone()))
        //     }
        //     PhysicalType2::Int16 => Box::new(MaxPrimitiveImpl::<PhysicalI16_2, i16>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::Int32 => Box::new(MaxPrimitiveImpl::<PhysicalI32_2, i32>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::Int64 => Box::new(MaxPrimitiveImpl::<PhysicalI64_2, i64>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::Int128 => Box::new(MaxPrimitiveImpl::<PhysicalI128_2, i128>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::UInt8 => {
        //         Box::new(MaxPrimitiveImpl::<PhysicalU8_2, u8>::new(datatype.clone()))
        //     }
        //     PhysicalType2::UInt16 => Box::new(MaxPrimitiveImpl::<PhysicalU16_2, u16>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::UInt32 => Box::new(MaxPrimitiveImpl::<PhysicalU32_2, u32>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::UInt64 => Box::new(MaxPrimitiveImpl::<PhysicalU64_2, u64>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::UInt128 => Box::new(MaxPrimitiveImpl::<PhysicalU128_2, u128>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::Interval => Box::new(
        //         MaxPrimitiveImpl::<PhysicalInterval_2, Interval>::new(datatype.clone()),
        //     ),
        //     PhysicalType2::Binary => Box::new(MaxBinaryImpl::new(datatype.clone())),
        //     PhysicalType2::Utf8 => Box::new(MaxBinaryImpl::new(datatype.clone())),
        //     PhysicalType2::List => {
        //         not_implemented!("MAX for list arrays")
        //     }
        // };
        unimplemented!()

        // Ok(PlannedAggregateFunction {
        //     function: Box::new(*self),
        //     return_type: datatype,
        //     inputs,
        //     function_impl,
        // })
    }
}

#[derive(Debug, Clone)]
pub struct MinMaxUntypedNull;

impl AggregateFunctionImpl for MinMaxUntypedNull {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        // Note min vs max doesn't matter. Everything is null.
        new_unary_aggregate_states2::<PhysicalUntypedNull_2, _, _, _, _>(
            MinState::<UntypedNull>::default,
            untyped_null_finalize,
        )
    }
}

pub type MinBinaryImpl = MinMaxBinaryImpl<MinStateBinary>;
pub type MaxBinaryImpl = MinMaxBinaryImpl<MaxStateBinary2>;

#[derive(Debug)]
pub struct MinMaxBinaryImpl<M> {
    datatype: DataType,
    _m: PhantomData<M>,
}

impl<M> MinMaxBinaryImpl<M> {
    fn new(datatype: DataType) -> Self {
        MinMaxBinaryImpl {
            datatype,
            _m: PhantomData,
        }
    }
}

impl<M> AggregateFunctionImpl for MinMaxBinaryImpl<M>
where
    M: for<'a> AggregateState2<&'a [u8], Vec<u8>> + Default + Sync + Send + 'static,
{
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        let datatype = self.datatype.clone();

        new_unary_aggregate_states2::<PhysicalBinary_2, _, _, _, _>(M::default, move |states| {
            let builder = ArrayBuilder {
                datatype: datatype.clone(),
                buffer: GermanVarlenBuffer::<[u8]>::with_len(states.len()),
            };
            StateFinalizer::finalize(states, builder)
        })
    }
}

impl<M> Clone for MinMaxBinaryImpl<M> {
    fn clone(&self) -> Self {
        Self::new(self.datatype.clone())
    }
}

pub type MinBoolImpl = MinMaxBoolImpl<MinState<bool>>;
pub type MaxBoolImpl = MinMaxBoolImpl<MaxState2<bool>>;

#[derive(Debug)]
pub struct MinMaxBoolImpl<M> {
    _m: PhantomData<M>,
}

impl<M> MinMaxBoolImpl<M> {
    fn new() -> Self {
        MinMaxBoolImpl { _m: PhantomData }
    }
}

impl<M> AggregateFunctionImpl for MinMaxBoolImpl<M>
where
    M: AggregateState2<bool, bool> + Default + Sync + Send + 'static,
{
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        new_unary_aggregate_states2::<PhysicalBool_2, _, _, _, _>(M::default, move |states| {
            boolean_finalize(DataType::Boolean, states)
        })
    }
}

impl<M> Clone for MinMaxBoolImpl<M> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

pub type MinPrimitiveImpl<S, T> = MinMaxPrimitiveImpl<MinState<T>, S, T>;
pub type MaxPrimitiveImpl2<S, T> = MinMaxPrimitiveImpl<MaxState2<T>, S, T>;

// TODO: Remove T
#[derive(Debug)]
pub struct MinMaxPrimitiveImpl<M, S, T> {
    datatype: DataType,
    _m: PhantomData<M>,
    _s: PhantomData<S>,
    _t: PhantomData<T>,
}

impl<M, S, T> MinMaxPrimitiveImpl<M, S, T> {
    fn new(datatype: DataType) -> Self {
        MinMaxPrimitiveImpl {
            datatype,
            _m: PhantomData,
            _s: PhantomData,
            _t: PhantomData,
        }
    }
}

impl<M, S, T> AggregateFunctionImpl for MinMaxPrimitiveImpl<M, S, T>
where
    for<'a> S: PhysicalStorage2<Type<'a> = T>,
    T: PartialOrd + Debug + Default + Sync + Send + Copy + 'static,
    M: AggregateState2<T, T> + Default + Sync + Send + 'static,
    ArrayData2: From<PrimitiveStorage<T>>,
{
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        let datatype = self.datatype.clone();

        new_unary_aggregate_states2::<S, _, _, _, _>(M::default, move |states| {
            primitive_finalize(datatype.clone(), states)
        })
    }
}

impl<M, S, T> Clone for MinMaxPrimitiveImpl<M, S, T> {
    fn clone(&self) -> Self {
        Self::new(self.datatype.clone())
    }
}

#[derive(Debug, Default)]
pub struct MinState<T> {
    min: T,
    valid: bool,
}

impl<T> AggregateState2<T, T> for MinState<T>
where
    T: PartialOrd + Debug + Default + Copy,
{
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        if !self.valid {
            self.valid = other.valid;
            self.min = other.min;
        } else if other.valid && other.min < self.min {
            self.min = other.min;
        }

        Ok(())
    }

    fn update(&mut self, input: T) -> Result<()> {
        if !self.valid {
            self.valid = true;
            self.min = input;
        } else if input < self.min {
            self.min = input
        }
        Ok(())
    }

    fn finalize(&mut self) -> Result<(T, bool)> {
        if self.valid {
            Ok((self.min, true))
        } else {
            Ok((T::default(), false))
        }
    }
}

#[derive(Debug, Default)]
pub struct MinStateBinary {
    min: Vec<u8>,
    valid: bool,
}

impl AggregateState2<&[u8], Vec<u8>> for MinStateBinary {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        if !self.valid {
            self.valid = other.valid;
            std::mem::swap(&mut self.min, &mut other.min);
        } else if other.valid && other.min < self.min {
            std::mem::swap(&mut self.min, &mut other.min);
        }

        Ok(())
    }

    fn update(&mut self, input: &[u8]) -> Result<()> {
        if !self.valid {
            self.valid = true;
            self.min = input.into();
        } else if input < self.min.as_slice() {
            self.min = input.into();
        }

        Ok(())
    }

    fn finalize(&mut self) -> Result<(Vec<u8>, bool)> {
        if self.valid {
            Ok((std::mem::take(&mut self.min), true))
        } else {
            Ok((Vec::new(), false))
        }
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
pub struct MaxState2<T> {
    max: T,
    valid: bool,
}

impl<T> AggregateState2<T, T> for MaxState2<T>
where
    T: PartialOrd + Debug + Default + Copy,
{
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        if !self.valid {
            self.valid = other.valid;
            self.max = other.max;
        } else if other.valid && other.max > self.max {
            self.max = other.max;
        }
        Ok(())
    }

    fn update(&mut self, input: T) -> Result<()> {
        if !self.valid {
            self.valid = true;
            self.max = input;
        } else if input > self.max {
            self.max = input
        }

        Ok(())
    }

    fn finalize(&mut self) -> Result<(T, bool)> {
        if self.valid {
            Ok((self.max, true))
        } else {
            Ok((T::default(), false))
        }
    }
}

#[derive(Debug, Default)]
pub struct MaxStateBinary2 {
    max: Vec<u8>,
    valid: bool,
}

impl AggregateState2<&[u8], Vec<u8>> for MaxStateBinary2 {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        if !self.valid {
            self.valid = other.valid;
            std::mem::swap(&mut self.max, &mut other.max);
        } else if other.valid && other.max > self.max {
            std::mem::swap(&mut self.max, &mut other.max);
        }

        Ok(())
    }

    fn update(&mut self, input: &[u8]) -> Result<()> {
        if !self.valid {
            self.valid = true;
            self.max = input.into();
        } else if input > self.max.as_slice() {
            self.max = input.into();
        }

        Ok(())
    }

    fn finalize(&mut self) -> Result<(Vec<u8>, bool)> {
        if self.valid {
            Ok((std::mem::take(&mut self.max), true))
        } else {
            Ok((Vec::new(), false))
        }
    }
}
