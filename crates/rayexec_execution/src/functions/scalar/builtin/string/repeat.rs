use std::fmt::Debug;

use rayexec_error::Result;

use crate::arrays::array::physical_type::{PhysicalI64, PhysicalUtf8};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::scalar::BinaryExecutor;
use crate::arrays::executor::OutBuffer;
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};
use crate::functions::Signature;
use crate::logical::binder::table_list::TableList;

pub const FUNCTION_SET_REPEAT: ScalarFunctionSet = ScalarFunctionSet {
    name: "repeat",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::String,
        description: "Repeat a string some number of times.",
        arguments: &["string", "count"],
        example: Some(Example {
            example: "repeat('abc', 3)",
            output: "abcabcabc",
        }),
    }),
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Utf8, DataTypeId::Int64], DataTypeId::Utf8),
        &Repeat,
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct Repeat;

impl ScalarFunction for Repeat {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Utf8,
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let strings = &input.arrays()[0];
        let counts = &input.arrays()[1];

        let mut str_buf = String::new();

        BinaryExecutor::execute::<PhysicalUtf8, PhysicalI64, PhysicalUtf8, _>(
            strings,
            sel,
            counts,
            sel,
            OutBuffer::from_array(output)?,
            |s, &num, buf| {
                str_buf.clear();
                for _ in 0..num {
                    str_buf.push_str(s);
                }
                buf.put(str_buf.as_str())
            },
        )
    }
}
