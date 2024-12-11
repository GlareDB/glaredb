use std::marker::PhantomData;

use num_traits::Float;
use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, BooleanBuffer};
use rayexec_bullet::executor::physical_type::{
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalStorage,
};
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_error::Result;

use super::ScalarFunction;
use crate::expr::Expression;
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
        &[
            Signature {
                input: &[DataTypeId::Float16],
                variadic: None,
                return_type: DataTypeId::Boolean,
            },
            Signature {
                input: &[DataTypeId::Float32],
                variadic: None,
                return_type: DataTypeId::Boolean,
            },
            Signature {
                input: &[DataTypeId::Float64],
                variadic: None,
                return_type: DataTypeId::Boolean,
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
    fn new() -> Self {
        IsNanImpl { _s: PhantomData }
    }
}

impl<S> ScalarFunctionImpl for IsNanImpl<S>
where
    S: PhysicalStorage,
    for<'a> S::Type<'a>: Float,
{
    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let input = inputs[0];
        let builder = ArrayBuilder {
            datatype: DataType::Boolean,
            buffer: BooleanBuffer::with_len(input.logical_len()),
        };

        UnaryExecutor::execute::<S, _, _>(input, builder, |v, buf| buf.put(&v.is_nan()))
    }
}
