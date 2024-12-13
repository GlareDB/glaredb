use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, BooleanBuffer};
use rayexec_bullet::executor::physical_type::PhysicalUtf8;
use rayexec_bullet::executor::scalar::{BinaryExecutor, UnaryExecutor};
use rayexec_error::Result;

use crate::expr::Expression;
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartsWith;

impl FunctionInfo for StartsWith {
    fn name(&self) -> &'static str {
        "starts_with"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["prefix"]
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Utf8, DataTypeId::Utf8],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
        }]
    }
}

impl ScalarFunction for StartsWith {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args(self, &inputs, 2)?;

        match (
            inputs[0].datatype(table_list)?,
            inputs[1].datatype(table_list)?,
        ) {
            (DataType::Utf8, DataType::Utf8) => (),
            (a, b) => return Err(invalid_input_types_error(self, &[a, b])),
        }

        let constant = if inputs[1].is_const_foldable() {
            let search_string = ConstFold::rewrite(table_list, inputs[1].clone())?
                .try_into_scalar()?
                .try_into_string()?;

            Some(search_string)
        } else {
            None
        };

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataType::Boolean,
            inputs,
            function_impl: Box::new(StartsWithImpl { constant }),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartsWithImpl {
    pub constant: Option<String>,
}

impl ScalarFunctionImpl for StartsWithImpl {
    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let builder = ArrayBuilder {
            datatype: DataType::Boolean,
            buffer: BooleanBuffer::with_len(inputs[0].logical_len()),
        };

        match self.constant.as_ref() {
            Some(constant) => {
                UnaryExecutor::execute::<PhysicalUtf8, _, _>(inputs[0], builder, |s, buf| {
                    buf.put(&s.starts_with(constant))
                })
            }
            None => BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _, _>(
                inputs[0],
                inputs[1],
                builder,
                |s, c, buf| buf.put(&s.starts_with(c)),
            ),
        }
    }
}
