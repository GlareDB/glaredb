use std::borrow::Borrow;
use std::fmt::{self, Debug};
use std::marker::PhantomData;

use half::f16;
use rayexec_error::{not_implemented, Result};

use crate::arrays::array::ArrayData2;
use crate::arrays::buffer::physical_type::{
    AddressableMut,
    MutablePhysicalStorage,
    PhysicalBinary,
    PhysicalType,
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

        let function_impl: Box<dyn AggregateFunctionImpl> = match datatype.physical_type() {
            // PhysicalType::Boolean
            other => not_implemented!("FIRST for physical type: {other}"),
        };

        // let function_impl: Box<dyn AggregateFunctionImpl> = match datatype.physical_type2()? {
        //     PhysicalType2::UntypedNull => Box::new(FirstUntypedNullImpl),
        //     PhysicalType2::Boolean => Box::new(FirstBoolImpl),
        //     PhysicalType2::Float16 => Box::new(FirstPrimitiveImpl::<PhysicalF16_2, f16>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::Float32 => Box::new(FirstPrimitiveImpl::<PhysicalF32_2, f32>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::Float64 => Box::new(FirstPrimitiveImpl::<PhysicalF64_2, f64>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::Int8 => Box::new(FirstPrimitiveImpl::<PhysicalI8_2, i8>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::Int16 => Box::new(FirstPrimitiveImpl::<PhysicalI16_2, i16>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::Int32 => Box::new(FirstPrimitiveImpl::<PhysicalI32_2, i32>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::Int64 => Box::new(FirstPrimitiveImpl::<PhysicalI64_2, i64>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::Int128 => Box::new(FirstPrimitiveImpl::<PhysicalI128_2, i128>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::UInt8 => Box::new(FirstPrimitiveImpl::<PhysicalU8_2, u8>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::UInt16 => Box::new(FirstPrimitiveImpl::<PhysicalU16_2, u16>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::UInt32 => Box::new(FirstPrimitiveImpl::<PhysicalU32_2, u32>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::UInt64 => Box::new(FirstPrimitiveImpl::<PhysicalU64_2, u64>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::UInt128 => Box::new(FirstPrimitiveImpl::<PhysicalU128_2, u128>::new(
        //         datatype.clone(),
        //     )),
        //     PhysicalType2::Interval => Box::new(
        //         FirstPrimitiveImpl::<PhysicalInterval_2, Interval>::new(datatype.clone()),
        //     ),
        //     PhysicalType2::Binary => Box::new(FirstBinaryImpl {
        //         datatype: datatype.clone(),
        //     }),
        //     PhysicalType2::Utf8 => Box::new(FirstBinaryImpl {
        //         datatype: datatype.clone(),
        //     }),
        //     PhysicalType2::List => {
        //         // TODO: Easy, clone underlying array and select.
        //         not_implemented!("FIRST for list arrays")
        //     }
        // };

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

impl<S> AggregateFunctionImpl for FirstPrimitiveImpl<S>
where
    S: MutablePhysicalStorage,
    S::StorageType: Debug + Default + Copy,
{
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        Box::new(TypedAggregateGroupStates::new(
            FirstPrimitiveState::<S::StorageType>::default,
            unary_update::<S, S, _>,
            drain::<S, _, _>,
        ))
    }
}

// #[derive(Debug, Clone, Copy)]
// pub struct FirstBinaryImpl;

// impl AggregateFunctionImpl for FirstBinaryImpl {
//     fn new_states(&self) -> Box<dyn AggregateGroupStates> {
//         Box::new(TypedAggregateGroupStates::new(
//             FirstBinaryState::default,
//             unary_update::<PhysicalBinary, PhysicalBinary, _>,
//             drain::<PhysicalBinary, _, _>,
//         ))
//     }
// }

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
