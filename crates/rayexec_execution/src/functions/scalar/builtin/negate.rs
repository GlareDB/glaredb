use std::marker::PhantomData;

use rayexec_bullet::array::{ArrayData, ArrayOld};
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, BooleanBuffer, PrimitiveBuffer};
use rayexec_bullet::executor::physical_type::{
    PhysicalBoolOld,
    PhysicalF16Old,
    PhysicalF32Old,
    PhysicalF64Old,
    PhysicalI128Old,
    PhysicalI16Old,
    PhysicalI32Old,
    PhysicalI64Old,
    PhysicalI8Old,
    PhysicalStorageOld,
};
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_bullet::storage::PrimitiveStorage;
use rayexec_error::Result;

use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Negate;

impl FunctionInfo for Negate {
    fn name(&self) -> &'static str {
        "negate"
    }

    fn signatures(&self) -> &[Signature] {
        const SIGS: &[Signature] = &[
            Signature::new_positional(&[DataTypeId::Float16], DataTypeId::Float16),
            Signature::new_positional(&[DataTypeId::Float32], DataTypeId::Float32),
            Signature::new_positional(&[DataTypeId::Float64], DataTypeId::Float64),
            Signature::new_positional(&[DataTypeId::Int8], DataTypeId::Int8),
            Signature::new_positional(&[DataTypeId::Int16], DataTypeId::Int16),
            Signature::new_positional(&[DataTypeId::Int32], DataTypeId::Int32),
            Signature::new_positional(&[DataTypeId::Int64], DataTypeId::Int64),
            Signature::new_positional(&[DataTypeId::Int128], DataTypeId::Int128),
            Signature::new_positional(&[DataTypeId::Interval], DataTypeId::Interval),
        ];
        SIGS
    }
}

impl ScalarFunction for Negate {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args(self, &inputs, 1)?;

        let dt = inputs[0].datatype(table_list)?;

        // TODO: Interval
        let function_impl: Box<dyn ScalarFunctionImpl> = match dt.clone() {
            dt @ DataType::Int8 => Box::new(NegateImpl::<PhysicalI8Old>::new(dt)),
            dt @ DataType::Int16 => Box::new(NegateImpl::<PhysicalI16Old>::new(dt)),
            dt @ DataType::Int32 => Box::new(NegateImpl::<PhysicalI32Old>::new(dt)),
            dt @ DataType::Int64 => Box::new(NegateImpl::<PhysicalI64Old>::new(dt)),
            dt @ DataType::Int128 => Box::new(NegateImpl::<PhysicalI128Old>::new(dt)),
            dt @ DataType::Float16 => Box::new(NegateImpl::<PhysicalF16Old>::new(dt)),
            dt @ DataType::Float32 => Box::new(NegateImpl::<PhysicalF32Old>::new(dt)),
            dt @ DataType::Float64 => Box::new(NegateImpl::<PhysicalF64Old>::new(dt)),
            other => return Err(invalid_input_types_error(self, &[other])),
        };

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: dt,
            inputs,
            function_impl,
        })
    }
}

#[derive(Debug, Clone)]
pub struct NegateImpl<S> {
    datatype: DataType, // TODO: Would be nice not needing to store this.
    _s: PhantomData<S>,
}

impl<S> NegateImpl<S> {
    fn new(datatype: DataType) -> Self {
        NegateImpl {
            datatype,
            _s: PhantomData,
        }
    }
}

impl<S> ScalarFunctionImpl for NegateImpl<S>
where
    S: PhysicalStorageOld,
    for<'a> S::Type<'a>: std::ops::Neg<Output = S::Type<'static>> + Default + Copy,
    ArrayData: From<PrimitiveStorage<S::Type<'static>>>,
{
    fn execute(&self, inputs: &[&ArrayOld]) -> Result<ArrayOld> {
        use std::ops::Neg;

        let a = inputs[0];
        let datatype = self.datatype.clone();
        let builder = ArrayBuilder {
            datatype,
            buffer: PrimitiveBuffer::with_len(a.logical_len()),
        };

        UnaryExecutor::execute::<S, _, _>(a, builder, |a, buf| buf.put(&(a.neg())))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Not;

impl FunctionInfo for Not {
    fn name(&self) -> &'static str {
        "not"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Boolean],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(&Documentation {
                category: Category::General,
                description: "Return the inverse bool of the input. Returns NULL if input is NULL.",
                arguments: &["input"],
                example: Some(Example {
                    example: "not(true)",
                    output: "false",
                }),
            }),
        }]
    }
}

impl ScalarFunction for Not {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args(self, &inputs, 1)?;
        match inputs[0].datatype(table_list)? {
            DataType::Boolean => Ok(PlannedScalarFunction {
                function: Box::new(*self),
                return_type: DataType::Boolean,
                inputs,
                function_impl: Box::new(NotImpl),
            }),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotImpl;

impl ScalarFunctionImpl for NotImpl {
    fn execute(&self, inputs: &[&ArrayOld]) -> Result<ArrayOld> {
        UnaryExecutor::execute::<PhysicalBoolOld, _, _>(
            inputs[0],
            ArrayBuilder {
                datatype: DataType::Boolean,
                buffer: BooleanBuffer::with_len(inputs[0].logical_len()),
            },
            |b, buf| buf.put(&(!b)),
        )
    }
}
