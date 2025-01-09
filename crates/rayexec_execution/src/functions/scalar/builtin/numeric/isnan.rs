use std::marker::PhantomData;

use num_traits::Float;
use rayexec_error::Result;

use super::ScalarFunction;
use crate::arrays::array::physical_type::{
    PhysicalBool,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalStorage,
};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::executor::OutBuffer;
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsNan;

impl FunctionInfo for IsNan {
    fn name(&self) -> &'static str {
        "isnan"
    }

    fn signatures(&self) -> &[Signature] {
        const DOC: &Documentation = &Documentation {
            category: Category::Numeric,
            description: "Return if the given float is a NaN.",
            arguments: &["float"],
            example: Some(Example {
                example: "isnan('NaN'::FLOAT)",
                output: "true",
            }),
        };

        &[
            Signature {
                positional_args: &[DataTypeId::Float16],
                variadic_arg: None,
                return_type: DataTypeId::Boolean,
                doc: Some(DOC),
            },
            Signature {
                positional_args: &[DataTypeId::Float32],
                variadic_arg: None,
                return_type: DataTypeId::Boolean,
                doc: Some(DOC),
            },
            Signature {
                positional_args: &[DataTypeId::Float64],
                variadic_arg: None,
                return_type: DataTypeId::Boolean,
                doc: Some(DOC),
            },
        ]
    }
}

impl ScalarFunction for IsNan {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args(self, &inputs, 1)?;

        let function_impl: Box<dyn ScalarFunctionImpl> = match inputs[0].datatype(table_list)? {
            DataType::Float16 => Box::new(IsNanImpl::<PhysicalF16>::new()),
            DataType::Float32 => Box::new(IsNanImpl::<PhysicalF32>::new()),
            DataType::Float64 => Box::new(IsNanImpl::<PhysicalF64>::new()),
            other => return Err(invalid_input_types_error(self, &[other])),
        };

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataType::Boolean,
            inputs,
            function_impl,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct IsNanImpl<S: PhysicalStorage> {
    _s: PhantomData<S>,
}

impl<S: PhysicalStorage> IsNanImpl<S> {
    const fn new() -> Self {
        IsNanImpl { _s: PhantomData }
    }
}

impl<S> ScalarFunctionImpl for IsNanImpl<S>
where
    S: PhysicalStorage,
    S::StorageType: Float,
{
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let input = &input.arrays()[0];

        UnaryExecutor::execute::<S, PhysicalBool, _>(
            input,
            sel,
            OutBuffer::from_array(output)?,
            |&v, buf| buf.put(&v.is_nan()),
        )
    }
}
