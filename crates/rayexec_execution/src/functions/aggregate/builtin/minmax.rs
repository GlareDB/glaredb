use std::fmt::Debug;
use std::marker::PhantomData;

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
    ScalarStorage,
};
use crate::arrays::datatype::DataTypeId;
use crate::arrays::executor::aggregate::AggregateState;
use crate::arrays::executor::PutBuffer;
use crate::buffer::buffer_manager::BufferManager;
use crate::expr::Expression;
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

// Min/max is used in some aggregate layout tests, assuming the size and
// alignment of min/max primitive states.
//
// If we change the state from just being a val+bool, those tests may need
// updating.

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

impl AggregateFunction2 for Min {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction2> {
        plan_check_num_args(self, &inputs, 1)?;

        let datatype = inputs[0].datatype()?;

        let function_impl = match datatype.physical_type() {
            PhysicalType::UntypedNull => create_primitive_min_impl::<PhysicalUntypedNull>(),
            PhysicalType::Boolean => create_primitive_min_impl::<PhysicalBool>(),
            PhysicalType::Int8 => create_primitive_min_impl::<PhysicalI8>(),
            PhysicalType::Int16 => create_primitive_min_impl::<PhysicalI16>(),
            PhysicalType::Int32 => create_primitive_min_impl::<PhysicalI32>(),
            PhysicalType::Int64 => create_primitive_min_impl::<PhysicalI64>(),
            PhysicalType::Int128 => create_primitive_min_impl::<PhysicalI128>(),
            PhysicalType::UInt8 => create_primitive_min_impl::<PhysicalU8>(),
            PhysicalType::UInt16 => create_primitive_min_impl::<PhysicalU16>(),
            PhysicalType::UInt32 => create_primitive_min_impl::<PhysicalU32>(),
            PhysicalType::UInt64 => create_primitive_min_impl::<PhysicalU64>(),
            PhysicalType::UInt128 => create_primitive_min_impl::<PhysicalU128>(),
            PhysicalType::Float16 => create_primitive_min_impl::<PhysicalF16>(),
            PhysicalType::Float32 => create_primitive_min_impl::<PhysicalF32>(),
            PhysicalType::Float64 => create_primitive_min_impl::<PhysicalF64>(),
            PhysicalType::Interval => create_primitive_min_impl::<PhysicalInterval>(),
            PhysicalType::Utf8 => AggregateFunctionImpl::new::<
                UnaryStateLogic<MinStateString, PhysicalUtf8, PhysicalUtf8>,
            >(None),
            PhysicalType::Binary => AggregateFunctionImpl::new::<
                UnaryStateLogic<MinStateBinary, PhysicalBinary, PhysicalBinary>,
            >(None),
            other => not_implemented!("max for type {other:?}"),
        };

        Ok(PlannedAggregateFunction2 {
            function: Box::new(*self),
            return_type: datatype,
            inputs,
            function_impl,
        })
    }
}

fn create_primitive_min_impl<S>() -> AggregateFunctionImpl
where
    S: MutableScalarStorage,
    S::StorageType: Copy + Sized + Default + Debug + PartialOrd,
{
    AggregateFunctionImpl::new::<UnaryStateLogic<MinStatePrimitive<S::StorageType>, S, S>>(None)
}

pub const FUNCTION_SET_MAX: AggregateFunctionSet = AggregateFunctionSet {
    name: "max",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Aggregate,
        description: "Return the maximum non-NULL value seen from input.",
        arguments: &["input"],
        example: None,
    }),
    functions: &[RawAggregateFunction::new(
        &Signature::new(&[DataTypeId::Int8], DataTypeId::Int8),
        &SimpleUnaryAggregate::new(&MaxPrimitive::<PhysicalI8>::new()),
    )],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxPrimitive<S> {
    _s: PhantomData<S>,
}

impl<S> MaxPrimitive<S> {
    pub const fn new() -> Self {
        MaxPrimitive { _s: PhantomData }
    }
}

impl<S> UnaryAggregate for MaxPrimitive<S>
where
    S: MutableScalarStorage,
    S::StorageType: PartialOrd + Default + Copy + Sized,
{
    type Input = S;
    type Output = S;

    type BindState = ();
    type AggregateState = MaxStatePrimitive<S::StorageType>;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        Ok(BindState {
            state: (),
            return_type: inputs[0].datatype()?,
            inputs,
        })
    }

    fn new_aggregate_state(_state: &Self::BindState) -> Self::AggregateState {
        Default::default()
    }
}

// impl FunctionInfo for Max {
//     fn name(&self) -> &'static str {
//         "max"
//     }

//     fn signatures(&self) -> &[Signature] {
//         &[Signature {
//             positional_args: &[DataTypeId::Any],
//             variadic_arg: None,
//             return_type: DataTypeId::Any,
//             doc: Some(&Documentation {
//                 category: Category::Aggregate,
//                 description: "Return the maximum non-NULL value seen from input.",
//                 arguments: &["input"],
//                 example: None,
//             }),
//         }]
//     }
// }

// impl AggregateFunction2 for Max {
//     fn plan(
//         &self,
//         table_list: &TableList,
//         inputs: Vec<Expression>,
//     ) -> Result<PlannedAggregateFunction2> {
//         plan_check_num_args(self, &inputs, 1)?;

//         let datatype = inputs[0].datatype()?;

//         let function_impl = match datatype.physical_type() {
//             PhysicalType::UntypedNull => create_primitive_max_impl::<PhysicalUntypedNull>(),
//             PhysicalType::Boolean => create_primitive_max_impl::<PhysicalBool>(),
//             PhysicalType::Int8 => create_primitive_max_impl::<PhysicalI8>(),
//             PhysicalType::Int16 => create_primitive_max_impl::<PhysicalI16>(),
//             PhysicalType::Int32 => create_primitive_max_impl::<PhysicalI32>(),
//             PhysicalType::Int64 => create_primitive_max_impl::<PhysicalI64>(),
//             PhysicalType::Int128 => create_primitive_max_impl::<PhysicalI128>(),
//             PhysicalType::UInt8 => create_primitive_max_impl::<PhysicalU8>(),
//             PhysicalType::UInt16 => create_primitive_max_impl::<PhysicalU16>(),
//             PhysicalType::UInt32 => create_primitive_max_impl::<PhysicalU32>(),
//             PhysicalType::UInt64 => create_primitive_max_impl::<PhysicalU64>(),
//             PhysicalType::UInt128 => create_primitive_max_impl::<PhysicalU128>(),
//             PhysicalType::Float16 => create_primitive_max_impl::<PhysicalF16>(),
//             PhysicalType::Float32 => create_primitive_max_impl::<PhysicalF32>(),
//             PhysicalType::Float64 => create_primitive_max_impl::<PhysicalF64>(),
//             PhysicalType::Interval => create_primitive_max_impl::<PhysicalInterval>(),
//             PhysicalType::Utf8 => AggregateFunctionImpl::new::<
//                 UnaryStateLogic<MaxStateString, PhysicalUtf8, PhysicalUtf8>,
//             >(None),
//             PhysicalType::Binary => AggregateFunctionImpl::new::<
//                 UnaryStateLogic<MaxStateBinary, PhysicalBinary, PhysicalBinary>,
//             >(None),
//             other => not_implemented!("max for type {other:?}"),
//         };

//         Ok(PlannedAggregateFunction2 {
//             function: Box::new(*self),
//             return_type: datatype,
//             inputs,
//             function_impl,
//         })
//     }
// }

fn create_primitive_max_impl<S>() -> AggregateFunctionImpl
where
    S: MutableScalarStorage,
    S::StorageType: Copy + Sized + Default + Debug + PartialOrd,
{
    AggregateFunctionImpl::new::<UnaryStateLogic<MaxStatePrimitive<S::StorageType>, S, S>>(None)
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
            self.valid = true;
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
            self.valid = true;
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
            self.valid = true;
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
            self.valid = true;
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
            self.valid = true;
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
            self.valid = true;
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
