use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::PhysicalUtf8;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::{FnName, ScalarFunctionSet};
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_MD5: ScalarFunctionSet = ScalarFunctionSet {
    name: FnName::default("md5"),
    aliases: &[],
    doc: &[&Documentation {
        category: Category::String,
        description: "Compute the MD5 hash of a string, returning the result as a hexadecimal string.",
        arguments: &["string"],
        example: Some(Example {
            example: "md5('hello')",
            output: "5d41402abc4b2a76b9719d911017c592",
        }),
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Utf8], DataTypeId::Utf8),
        &Md5Function,
    )],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Md5Function;

impl ScalarFunction for Md5Function {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::utf8(),
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let input = &input.arrays()[0];

        let mut result_buf = String::new();

        UnaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _>(
            input,
            sel,
            OutBuffer::from_array(output)?,
            |v, buf| {
                let digest = md5::compute(v.as_bytes());

                result_buf.clear();
                hex_encode(&digest.0, &mut result_buf);
                buf.put(&result_buf)
            },
        )
    }
}

fn hex_encode(bytes: &[u8], output: &mut String) {
    const CHARS: &[u8; 16] = b"0123456789abcdef";

    output.reserve(bytes.len() * 2);
    for &byte in bytes {
        output.push(CHARS[((byte >> 4) & 0xF) as usize] as char);
        output.push(CHARS[(byte & 0xF) as usize] as char);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hex_encode() {
        let mut output = String::new();
        hex_encode(&[0x5d, 0x41, 0x40, 0x2a], &mut output);
        assert_eq!(output, "5d41402a");
    }

    #[test]
    fn test_md5_compute() {
        let digest = md5::compute(b"hello");
        let mut result = String::new();
        hex_encode(&digest.0, &mut result);
        assert_eq!(result, "5d41402abc4b2a76b9719d911017c592");
    }

    #[test]
    fn test_md5_empty() {
        let digest = md5::compute(b"");
        let mut result = String::new();
        hex_encode(&digest.0, &mut result);
        assert_eq!(result, "d41d8cd98f00b204e9800998ecf8427e");
    }
}
