use rayexec_error::Result;

use crate::arrays::array::Array;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::builder::{ArrayBuilder, BooleanBuffer};
use crate::arrays::array::physical_type::PhysicalUtf8;
use crate::arrays::executor::scalar::{BinaryExecutor, UnaryExecutor};
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Contains;

impl FunctionInfo for Contains {
    fn name(&self) -> &'static str {
        "contains"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Utf8, DataTypeId::Utf8],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(&Documentation {
                category: Category::String,
                description: "Check if string contains a search string.",
                arguments: &["string", "search"],
                example: Some(Example {
                    example: "contains('house', 'ou')",
                    output: "true",
                }),
            }),
        }]
    }
}

impl ScalarFunction for Contains {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        let datatypes = inputs
            .iter()
            .map(|expr| expr.datatype(table_list))
            .collect::<Result<Vec<_>>>()?;

        match (&datatypes[0], &datatypes[1]) {
            (DataType::Utf8, DataType::Utf8) => (),
            (a, b) => return Err(invalid_input_types_error(self, &[a, b])),
        }

        let function_impl: Box<dyn ScalarFunctionImpl> = if inputs[1].is_const_foldable() {
            let search_string = ConstFold::rewrite(table_list, inputs[1].clone())?
                .try_into_scalar()?
                .try_into_string()?;

            Box::new(StringContainsConstantImpl {
                constant: search_string,
            })
        } else {
            Box::new(StringContainsImpl)
        };

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataType::Boolean,
            inputs,
            function_impl,
        })
    }
}

#[derive(Debug, Clone)]
pub struct StringContainsConstantImpl {
    pub constant: String,
}

impl ScalarFunctionImpl for StringContainsConstantImpl {
    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let builder = ArrayBuilder {
            datatype: DataType::Boolean,
            buffer: BooleanBuffer::with_len(inputs[0].logical_len()),
        };

        UnaryExecutor::execute::<PhysicalUtf8, _, _>(inputs[0], builder, |s, buf| {
            buf.put(&s.contains(&self.constant))
        })
    }
}

#[derive(Debug, Clone)]
pub struct StringContainsImpl;

impl ScalarFunctionImpl for StringContainsImpl {
    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let builder = ArrayBuilder {
            datatype: DataType::Boolean,
            buffer: BooleanBuffer::with_len(inputs[0].logical_len()),
        };

        BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _, _>(
            inputs[0],
            inputs[1],
            builder,
            |s, c, buf| buf.put(&s.contains(c)),
        )
    }
}
