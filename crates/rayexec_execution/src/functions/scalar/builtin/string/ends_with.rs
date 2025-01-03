use rayexec_error::Result;

use crate::arrays::array::exp::Array;
use crate::arrays::batch_exp::Batch;
use crate::arrays::buffer::physical_type::{PhysicalBool, PhysicalUtf8};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor_exp::scalar::binary::BinaryExecutor;
use crate::arrays::executor_exp::scalar::unary::UnaryExecutor;
use crate::arrays::executor_exp::OutBuffer;
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EndsWith;

impl FunctionInfo for EndsWith {
    fn name(&self) -> &'static str {
        "ends_with"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["suffix"]
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Utf8, DataTypeId::Utf8],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(&Documentation {
                category: Category::String,
                description: "Check if a string ends with a given suffix.",
                arguments: &["string", "suffix"],
                example: Some(Example {
                    example: "ends_with('house', 'se')",
                    output: "true",
                }),
            }),
        }]
    }
}

impl ScalarFunction for EndsWith {
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

            Box::new(EndsWithConstantImpl {
                constant: search_string,
            })
        } else {
            Box::new(EndsWithImpl)
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
pub struct EndsWithConstantImpl {
    pub constant: String,
}

impl ScalarFunctionImpl for EndsWithConstantImpl {
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let input = &input.arrays()[0];

        UnaryExecutor::execute::<PhysicalUtf8, PhysicalBool, _>(
            input,
            sel,
            OutBuffer::from_array(output)?,
            |s, buf| {
                let v = s.ends_with(&self.constant);
                buf.put(&v);
            },
        )
    }
}

#[derive(Debug, Clone)]
pub struct EndsWithImpl;

impl ScalarFunctionImpl for EndsWithImpl {
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let strings = &input.arrays()[0];
        let suffix = &input.arrays()[1];

        BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, PhysicalBool, _>(
            strings,
            sel,
            suffix,
            sel,
            OutBuffer::from_array(output)?,
            |s, suffix, buf| {
                let v = s.ends_with(&suffix);
                buf.put(&v);
            },
        )
    }
}
