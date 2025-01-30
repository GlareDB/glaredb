use rayexec_error::Result;

use crate::arrays::array::physical_type::{PhysicalBool, PhysicalUtf8};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::scalar::{BinaryExecutor, UnaryExecutor};
use crate::arrays::executor::OutBuffer;
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
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
            doc: Some(&Documentation {
                category: Category::String,
                description: "Check if a string starts with a prefix.",
                arguments: &["string", "prefix"],
                example: Some(Example {
                    example: "starts_with('hello', 'he')",
                    output: "true",
                }),
            }),
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
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();

        match self.constant.as_ref() {
            Some(prefix) => UnaryExecutor::execute::<PhysicalUtf8, PhysicalBool, _, _>(
                &input.arrays()[0],
                sel,
                OutBuffer::from_array(output)?,
                |s, buf| buf.put(&s.starts_with(prefix)),
            ),
            None => BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, PhysicalBool, _, _>(
                &input.arrays()[0],
                sel,
                &input.arrays()[1],
                sel,
                OutBuffer::from_array(output)?,
                |s, prefix, buf| buf.put(&s.starts_with(prefix)),
            ),
        }
    }
}
