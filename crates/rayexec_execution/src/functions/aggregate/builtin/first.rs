use std::fmt::Debug;
use std::marker::PhantomData;

use half::f16;
use rayexec_error::{not_implemented, Result};

use crate::arrays::array::ArrayData2;
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
use crate::arrays::scalar::interval::Interval;
use crate::arrays::storage::{PrimitiveStorage, UntypedNull};
use crate::expr::Expression;
use crate::functions::aggregate::states::{
    boolean_finalize,
    new_unary_aggregate_states2,
    primitive_finalize,
    untyped_null_finalize,
    AggregateGroupStates,
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

        let function_impl: Box<dyn AggregateFunctionImpl> = match datatype.physical_type2()? {
            PhysicalType2::UntypedNull => Box::new(FirstUntypedNullImpl),
            PhysicalType2::Boolean => Box::new(FirstBoolImpl),
            PhysicalType2::Float16 => Box::new(FirstPrimitiveImpl::<PhysicalF16_2, f16>::new(
                datatype.clone(),
            )),
            PhysicalType2::Float32 => Box::new(FirstPrimitiveImpl::<PhysicalF32_2, f32>::new(
                datatype.clone(),
            )),
            PhysicalType2::Float64 => Box::new(FirstPrimitiveImpl::<PhysicalF64_2, f64>::new(
                datatype.clone(),
            )),
            PhysicalType2::Int8 => Box::new(FirstPrimitiveImpl::<PhysicalI8_2, i8>::new(
                datatype.clone(),
            )),
            PhysicalType2::Int16 => Box::new(FirstPrimitiveImpl::<PhysicalI16_2, i16>::new(
                datatype.clone(),
            )),
            PhysicalType2::Int32 => Box::new(FirstPrimitiveImpl::<PhysicalI32_2, i32>::new(
                datatype.clone(),
            )),
            PhysicalType2::Int64 => Box::new(FirstPrimitiveImpl::<PhysicalI64_2, i64>::new(
                datatype.clone(),
            )),
            PhysicalType2::Int128 => Box::new(FirstPrimitiveImpl::<PhysicalI128_2, i128>::new(
                datatype.clone(),
            )),
            PhysicalType2::UInt8 => Box::new(FirstPrimitiveImpl::<PhysicalU8_2, u8>::new(
                datatype.clone(),
            )),
            PhysicalType2::UInt16 => Box::new(FirstPrimitiveImpl::<PhysicalU16_2, u16>::new(
                datatype.clone(),
            )),
            PhysicalType2::UInt32 => Box::new(FirstPrimitiveImpl::<PhysicalU32_2, u32>::new(
                datatype.clone(),
            )),
            PhysicalType2::UInt64 => Box::new(FirstPrimitiveImpl::<PhysicalU64_2, u64>::new(
                datatype.clone(),
            )),
            PhysicalType2::UInt128 => Box::new(FirstPrimitiveImpl::<PhysicalU128_2, u128>::new(
                datatype.clone(),
            )),
            PhysicalType2::Interval => Box::new(
                FirstPrimitiveImpl::<PhysicalInterval_2, Interval>::new(datatype.clone()),
            ),
            PhysicalType2::Binary => Box::new(FirstBinaryImpl {
                datatype: datatype.clone(),
            }),
            PhysicalType2::Utf8 => Box::new(FirstBinaryImpl {
                datatype: datatype.clone(),
            }),
            PhysicalType2::List => {
                // TODO: Easy, clone underlying array and select.
                not_implemented!("FIRST for list arrays")
            }
        };

        Ok(PlannedAggregateFunction {
            function: Box::new(*self),
            return_type: datatype,
            inputs,
            function_impl,
        })
    }
}

/// FIRST aggregate impl for utf8 and binary.
#[derive(Debug, Clone)]
pub struct FirstBinaryImpl {
    datatype: DataType,
}

impl AggregateFunctionImpl for FirstBinaryImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        let datatype = self.datatype.clone();

        new_unary_aggregate_states2::<PhysicalBinary_2, _, _, _, _>(
            FirstStateBinary::default,
            move |states| {
                let builder = ArrayBuilder {
                    datatype: datatype.clone(),
                    buffer: GermanVarlenBuffer::<[u8]>::with_len(states.len()),
                };
                StateFinalizer::finalize(states, builder)
            },
        )
    }
}

#[derive(Debug, Clone)]
pub struct FirstUntypedNullImpl;

impl AggregateFunctionImpl for FirstUntypedNullImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        new_unary_aggregate_states2::<PhysicalUntypedNull_2, _, _, _, _>(
            FirstState::<UntypedNull>::default,
            untyped_null_finalize,
        )
    }
}

#[derive(Debug, Clone)]
pub struct FirstBoolImpl;

impl AggregateFunctionImpl for FirstBoolImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        new_unary_aggregate_states2::<PhysicalBool_2, _, _, _, _>(
            FirstState::<bool>::default,
            move |states| boolean_finalize(DataType::Boolean, states),
        )
    }
}

// TODO: Remove T
#[derive(Debug, Clone)]
pub struct FirstPrimitiveImpl<S, T> {
    datatype: DataType,
    _s: PhantomData<S>,
    _t: PhantomData<T>,
}

impl<S, T> FirstPrimitiveImpl<S, T> {
    fn new(datatype: DataType) -> Self {
        FirstPrimitiveImpl {
            datatype,
            _s: PhantomData,
            _t: PhantomData,
        }
    }
}

impl<S, T> AggregateFunctionImpl for FirstPrimitiveImpl<S, T>
where
    for<'a> S: PhysicalStorage2<Type<'a> = T>,
    T: Copy + Debug + Default + Sync + Send + 'static,
    ArrayData2: From<PrimitiveStorage<T>>,
{
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        let datatype = self.datatype.clone();

        new_unary_aggregate_states2::<S, _, _, _, _>(FirstState::<T>::default, move |states| {
            primitive_finalize(datatype.clone(), states)
        })
    }
}

#[derive(Debug, Default)]
pub struct FirstState<T> {
    value: Option<T>,
}

impl<T: Default + Debug + Copy> AggregateState2<T, T> for FirstState<T> {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        if self.value.is_none() {
            self.value = other.value;
            return Ok(());
        }
        Ok(())
    }

    fn update(&mut self, input: T) -> Result<()> {
        if self.value.is_none() {
            self.value = Some(input);
        }
        Ok(())
    }

    fn finalize(&mut self) -> Result<(T, bool)> {
        match self.value {
            Some(v) => Ok((v, true)),
            None => Ok((T::default(), false)),
        }
    }
}

#[derive(Debug, Default)]
pub struct FirstStateBinary {
    value: Option<Vec<u8>>,
}

impl AggregateState2<&[u8], Vec<u8>> for FirstStateBinary {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        if self.value.is_none() {
            std::mem::swap(&mut self.value, &mut other.value);
            return Ok(());
        }
        Ok(())
    }

    fn update(&mut self, input: &[u8]) -> Result<()> {
        if self.value.is_none() {
            self.value = Some(input.to_owned());
        }
        Ok(())
    }

    fn finalize(&mut self) -> Result<(Vec<u8>, bool)> {
        match self.value.as_mut() {
            Some(v) => Ok((std::mem::take(v), true)),
            None => Ok((Vec::new(), false)),
        }
    }
}
