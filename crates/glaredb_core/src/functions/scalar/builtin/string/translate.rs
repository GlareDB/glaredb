use std::collections::HashMap;

use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::PhysicalUtf8;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::TernaryExecutor;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_TRANSLATE: ScalarFunctionSet = ScalarFunctionSet {
    name: "translate",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::String,
        description: "Replace each character in string that matches a character in the from set with the corresponding character in the to set. If from is longer than to, occurrences of the extra characters in from are deleted.",
        arguments: &["string", "from", "to"],
        example: Some(Example {
            example: "translate('12345', '143', 'ax')",
            output: "a2x5",
        }),
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(
            &[DataTypeId::Utf8, DataTypeId::Utf8, DataTypeId::Utf8],
            DataTypeId::Utf8,
        ),
        &Translate,
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct Translate;

impl ScalarFunction for Translate {
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

        TernaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, PhysicalUtf8, PhysicalUtf8, _>(
            &input.arrays()[0],
            sel,
            &input.arrays()[1],
            sel,
            &input.arrays()[2],
            sel,
            OutBuffer::from_array(output)?,
            |string, from_set, to_set, buf| {
                let result = translate_string(string, from_set, to_set);
                buf.put(&result);
            },
        )
    }
}

fn translate_string(string: &str, from_set: &str, to_set: &str) -> String {
    let mut char_map: HashMap<char, Option<char>> = HashMap::new();

    let from_chars: Vec<char> = from_set.chars().collect();
    let to_chars: Vec<char> = to_set.chars().collect();

    for (i, &from_char) in from_chars.iter().enumerate() {
        char_map.entry(from_char).or_insert_with(|| {
            if i < to_chars.len() {
                Some(to_chars[i])
            } else {
                None
            }
        });
    }

    let mut result = String::new();
    for ch in string.chars() {
        match char_map.get(&ch) {
            Some(Some(replacement)) => result.push(*replacement),
            Some(None) => {}
            None => result.push(ch),
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_translate_basic() {
        assert_eq!(translate_string("12345", "143", "ax"), "a2x5");
        assert_eq!(translate_string("hello", "el", "XY"), "hXYYo");
        assert_eq!(translate_string("hello", "elo", "XY"), "hXYY");
    }

    #[test]
    fn test_translate_delete() {
        assert_eq!(translate_string("hello", "elo", "X"), "hX");
        assert_eq!(translate_string("hello", "helo", ""), "");
    }

    #[test]
    fn test_translate_unicode() {
        assert_eq!(translate_string("héllo", "é", "e"), "hello");
        assert_eq!(translate_string("🌍🌎🌏", "🌍🌏", "AB"), "A🌎B");
    }

    #[test]
    fn test_translate_empty() {
        assert_eq!(translate_string("", "abc", "xyz"), "");
        assert_eq!(translate_string("hello", "", ""), "hello");
        assert_eq!(translate_string("hello", "abc", ""), "hello");
    }

    #[test]
    fn test_translate_repeated_chars() {
        assert_eq!(translate_string("abcdef", "ace", "XY"), "XbYdf");
        assert_eq!(translate_string("hello", "ell", "XYZ"), "hXYYo");
    }
}
