use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{
    MutableScalarStorage,
    PhysicalBool,
    PhysicalType,
    ScalarStorage,
};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::{FnName, ScalarFunctionSet};
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_IS_NULL: ScalarFunctionSet = ScalarFunctionSet {
    name: FnName::default("is_null"),
    aliases: &[],
    doc: &[&Documentation {
        category: Category::COMPARISON_OPERATOR,
        description: "Check if a value is NULL.",
        arguments: &["value"],
        example: Some(Example {
            example: "is_null(NULL)",
            output: "true",
        }),
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Any], DataTypeId::Boolean),
        &CheckNull::<true>,
    )],
};

pub const FUNCTION_SET_IS_NOT_NULL: ScalarFunctionSet = ScalarFunctionSet {
    name: FnName::default("is_not_null"),
    aliases: &[],
    doc: &[&Documentation {
        category: Category::COMPARISON_OPERATOR,
        description: "Check if a value is not NULL.",
        arguments: &["value"],
        example: Some(Example {
            example: "is_not_null(NULL)",
            output: "false",
        }),
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Any], DataTypeId::Boolean),
        &CheckNull::<false>,
    )],
};

/// Return RETURN if a value is null.
#[derive(Debug, Clone, Copy)]
pub struct CheckNull<const RETURN: bool>;

impl<const RETURN: bool> ScalarFunction for CheckNull<RETURN> {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::boolean(),
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let input = &input.arrays()[0];

        let out = PhysicalBool::get_addressable_mut(&mut output.data)?;
        if input.physical_type()? == PhysicalType::UntypedNull {
            // Everything null, just set to default value.
            out.slice.iter_mut().for_each(|v| *v = RETURN);
            return Ok(());
        }

        // Just need to look at the validity (already logical), no flattening
        // needed.

        for (output_idx, idx) in sel.into_iter().enumerate() {
            let is_valid = input.validity.is_valid(idx);
            if is_valid {
                out.slice[output_idx] = !RETURN;
            } else {
                out.slice[output_idx] = RETURN;
            }
        }

        Ok(())
    }
}

pub const FUNCTION_SET_IS_TRUE: ScalarFunctionSet = ScalarFunctionSet {
    name: FnName::default("is_true"),
    aliases: &[],
    doc: &[&Documentation {
        category: Category::COMPARISON_OPERATOR,
        description: "Check if a value is true.",
        arguments: &["value"],
        example: Some(Example {
            example: "is_true(false)",
            output: "false",
        }),
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Boolean], DataTypeId::Boolean),
        &IsBool::<false, true>,
    )],
};

pub const FUNCTION_SET_IS_NOT_TRUE: ScalarFunctionSet = ScalarFunctionSet {
    name: FnName::default("is_not_true"),
    aliases: &[],
    doc: &[&Documentation {
        category: Category::COMPARISON_OPERATOR,
        description: "Check if a value is not true.",
        arguments: &["value"],
        example: Some(Example {
            example: "is_not_true(false)",
            output: "true",
        }),
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Boolean], DataTypeId::Boolean),
        &IsBool::<true, true>,
    )],
};

pub const FUNCTION_SET_IS_FALSE: ScalarFunctionSet = ScalarFunctionSet {
    name: FnName::default("is_false"),
    aliases: &[],
    doc: &[&Documentation {
        category: Category::COMPARISON_OPERATOR,
        description: "Check if a value is false.",
        arguments: &["value"],
        example: Some(Example {
            example: "is_false(false)",
            output: "true",
        }),
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Boolean], DataTypeId::Boolean),
        &IsBool::<false, false>,
    )],
};

pub const FUNCTION_SET_IS_NOT_FALSE: ScalarFunctionSet = ScalarFunctionSet {
    name: FnName::default("is_not_false"),
    aliases: &[],
    doc: &[&Documentation {
        category: Category::COMPARISON_OPERATOR,
        description: "Check if a value is not false.",
        arguments: &["value"],
        example: Some(Example {
            example: "is_not_false(false)",
            output: "false",
        }),
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Boolean], DataTypeId::Boolean),
        &IsBool::<true, false>,
    )],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsBool<const NOT: bool, const BOOL: bool>;

impl<const NOT: bool, const BOOL: bool> ScalarFunction for IsBool<NOT, BOOL> {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::boolean(),
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let input = &input.arrays()[0];

        let out = PhysicalBool::get_addressable_mut(&mut output.data)?;

        let buffer =
            PhysicalBool::downcast_execution_format(&input.data)?.into_selection_format()?;
        let input_bools = PhysicalBool::addressable(buffer.buffer);

        for (output_idx, idx) in sel.into_iter().enumerate() {
            let is_valid = input.validity.is_valid(idx);
            if is_valid {
                let sel_idx = buffer.selection.get(idx).unwrap();
                let val = input_bools.slice[sel_idx];
                out.slice[output_idx] = if NOT { val != BOOL } else { val == BOOL }
            } else {
                // 'IS TRUE', 'IS FALSE' => false
                // 'IS NOT TRUE', 'IS NOT FALSE' => true
                out.slice[output_idx] = NOT;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_manager::DefaultBufferManager;
    use crate::testutil::arrays::assert_arrays_eq;
    use crate::{generate_array, generate_batch};

    #[test]
    fn is_null() {
        let input = generate_batch!([Some(4), None, Some(5)]);
        let mut out = Array::new(&DefaultBufferManager, DataType::boolean(), 3).unwrap();

        CheckNull::<true>::execute(&(), &input, &mut out).unwrap();

        let expected = generate_array!([false, true, false]);
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn is_not_null() {
        let input = generate_batch!([Some(4), None, Some(5)]);
        let mut out = Array::new(&DefaultBufferManager, DataType::boolean(), 3).unwrap();

        CheckNull::<false>::execute(&(), &input, &mut out).unwrap();

        let expected = generate_array!([true, false, true]);
        assert_arrays_eq(&expected, &out);
    }
}
