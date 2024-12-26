use std::marker::PhantomData;

use num_traits::Float;
use rayexec_bullet::array::ArrayOld;
use rayexec_bullet::datatype::{DataTypeId, DataTypeOld};
use rayexec_bullet::executor::builder::{ArrayBuilder, BooleanBuffer};
use rayexec_bullet::executor::physical_type::{
    PhysicalF16Old,
    PhysicalF32Old,
    PhysicalF64Old,
    PhysicalStorageOld,
};
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_error::Result;

use super::ScalarFunction;
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
            DataTypeOld::Float16 => Box::new(IsNanImpl::<PhysicalF16Old>::new()),
            DataTypeOld::Float32 => Box::new(IsNanImpl::<PhysicalF32Old>::new()),
            DataTypeOld::Float64 => Box::new(IsNanImpl::<PhysicalF64Old>::new()),
            other => return Err(invalid_input_types_error(self, &[other])),
        };

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataTypeOld::Boolean,
            inputs,
            function_impl,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct IsNanImpl<S: PhysicalStorageOld> {
    _s: PhantomData<S>,
}

impl<S: PhysicalStorageOld> IsNanImpl<S> {
    fn new() -> Self {
        IsNanImpl { _s: PhantomData }
    }
}

impl<S> ScalarFunctionImpl for IsNanImpl<S>
where
    S: PhysicalStorageOld,
    for<'a> S::Type<'a>: Float,
{
    fn execute_old(&self, inputs: &[&ArrayOld]) -> Result<ArrayOld> {
        let input = inputs[0];
        let builder = ArrayBuilder {
            datatype: DataTypeOld::Boolean,
            buffer: BooleanBuffer::with_len(input.logical_len()),
        };

        UnaryExecutor::execute::<S, _, _>(input, builder, |v, buf| buf.put(&v.is_nan()))
    }
}
