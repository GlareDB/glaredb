use rayexec_error::Result;

use crate::arrays::array::physical_type::{AddressableMut, MutableScalarStorage, PhysicalUtf8};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::scalar::{BinaryExecutor, UnaryExecutor, UniformExecutor};
use crate::arrays::executor::OutBuffer;
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};
use crate::functions::Signature;
use crate::logical::binder::table_list::TableList;

// TODO: Currently '||' aliases to this, however there should be two separate
// concat functions. One that should return null on any null arguments (||), and
// one that should omit null arguments when concatenating (the normal concat).
pub const FUNCTION_SET_CONCAT: ScalarFunctionSet = ScalarFunctionSet {
    name: "concat",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::String,
        description: "Concatenate many strings into a single string.",
        arguments: &["var_args"],
        example: Some(Example {
            example: "concat('cat', 'dog', 'mouse')",
            output: "catdogmouse",
        }),
    }),
    functions: &[RawScalarFunction::new(
        Signature {
            positional_args: &[],
            variadic_arg: Some(DataTypeId::Utf8),
            return_type: DataTypeId::Utf8,
            doc: None,
        },
        &StringConcat,
    )],
};

#[derive(Debug, Clone)]
pub struct StringConcat;

impl ScalarFunction for StringConcat {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Utf8,
            inputs,
        })
    }

    fn execute(&self, _state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();

        match input.arrays().len() {
            0 => {
                // TODO: Zero args should actually error during planning.
                // Currently this just sets everything to an empty string.
                let mut s = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                for idx in 0..s.len() {
                    s.put(idx, "");
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
