use std::marker::PhantomData;

use rayexec_error::Result;

use crate::arrays::array::physical_type::{
    MutableScalarStorage,
    PhysicalBool,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::executor::OutBuffer;
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
            DataType::Int8 => Box::new(NegateImpl::<PhysicalI8>::new()),
            DataType::Int16 => Box::new(NegateImpl::<PhysicalI16>::new()),
            DataType::Int32 => Box::new(NegateImpl::<PhysicalI32>::new()),
            DataType::Int64 => Box::new(NegateImpl::<PhysicalI64>::new()),
            DataType::Int128 => Box::new(NegateImpl::<PhysicalI128>::new()),
            DataType::Float16 => Box::new(NegateImpl::<PhysicalF16>::new()),
            DataType::Float32 => Box::new(NegateImpl::<PhysicalF32>::new()),
            DataType::Float64 => Box::new(NegateImpl::<PhysicalF64>::new()),
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
    _s: PhantomData<S>,
}

impl<S> NegateImpl<S> {
    const fn new() -> Self {
        NegateImpl { _s: PhantomData }
    }
}

impl<S> ScalarFunctionImpl for NegateImpl<S>
where
    S: MutableScalarStorage,
    S::StorageType: std::ops::Neg<Output = S::StorageType> + Copy,
{
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();

        UnaryExecutor::execute::<S, S, _, _>(
            &input.arrays()[0],
            sel,
            OutBuffer::from_array(output)?,
            |&a, buf| buf.put(&(-a)),
        )
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
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();

        UnaryExecutor::execute::<PhysicalBool, PhysicalBool, _, _>(
            &input.arrays()[0],
            sel,
            OutBuffer::from_array(output)?,
            |&b, buf| buf.put(&(!b)),
        )
    }
}
