use rayexec_error::Result;

use crate::arrays::array::physical_type::{AddressableMut, PhysicalUtf8};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::scalar::{BinaryExecutor, UnaryExecutor, UniformExecutor};
use crate::arrays::executor::OutBuffer;
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

// TODO: Currently '||' aliases to this, however there should be two separate
// concat functions. One that should return null on any null arguments (||), and
// one that should omit null arguments when concatenating (the normal concat).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Concat;

impl FunctionInfo for Concat {
    fn name(&self) -> &'static str {
        "concat"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[],
            variadic_arg: Some(DataTypeId::Utf8),
            return_type: DataTypeId::Utf8,
            doc: Some(&Documentation {
                category: Category::String,
                description: "Concatenate many strings into a single string.",
                arguments: &["var_args"],
                example: Some(Example {
                    example: "concat('cat', 'dog', 'mouse')",
                    output: "catdogmouse",
                }),
            }),
        }]
    }
}

impl ScalarFunction for Concat {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        let datatypes = inputs
            .iter()
            .map(|input| input.datatype(table_list))
            .collect::<Result<Vec<_>>>()?;

        if !datatypes.iter().all(|dt| dt == &DataType::Utf8) {
            return Err(invalid_input_types_error(self, &datatypes));
        }

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataType::Utf8,
            inputs,
            function_impl: Box::new(StringConcatImpl),
        })
    }
}

#[derive(Debug, Clone)]
pub struct StringConcatImpl;

impl ScalarFunctionImpl for StringConcatImpl {
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();

        match input.arrays().len() {
            0 => {
                // TODO: Zero args should actually error during planning.
                // Currently this just sets everything to an empty string.
                let mut addressable = output
                    .next_mut()
                    .data
                    .try_as_mut()?
                    .try_as_string_view_addressable_mut()?;

                for idx in 0..addressable.len() {
                    addressable.put(idx, "");
                }
            }
            1 => {
                let input = &input.arrays()[0];

                UnaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _>(
                    input,
                    sel,
                    OutBuffer::from_array(output)?,
                    |s, buf| buf.put(s),
                )?;
            }
            2 => {
                let a = &input.arrays()[0];
                let b = &input.arrays()[0];

                let mut str_buf = String::new();

                BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, PhysicalUtf8, _>(
                    a,
                    sel,
                    b,
                    sel,
                    OutBuffer::from_array(output)?,
                    |s1, s2, buf| {
                        str_buf.clear();
                        str_buf.push_str(s1);
                        str_buf.push_str(s2);
                        buf.put(&str_buf);
                    },
                )?;
            }
            _ => {
                let mut str_buf = String::new();

                UniformExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _>(
                    input.arrays(),
                    sel,
                    OutBuffer::from_array(output)?,
                    |ss, buf| {
                        str_buf.clear();
                        for s in ss {
                            str_buf.push_str(s);
                        }
                        buf.put(&str_buf);
                    },
                )?;
            }
        }

        Ok(())
    }
}
