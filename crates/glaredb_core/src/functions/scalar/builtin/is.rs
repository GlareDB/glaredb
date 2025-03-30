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
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_IS_NULL: ScalarFunctionSet = ScalarFunctionSet {
    name: "is_null",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::General,
        description: "Check if a value is NULL.",
        arguments: &["value"],
        example: Some(Example {
            example: "is_null(NULL)",
            output: "true",
        }),
    }),
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Any], DataTypeId::Boolean),
        &IsNull::<false>,
    )],
};

pub const FUNCTION_SET_IS_NOT_NULL: ScalarFunctionSet = ScalarFunctionSet {
    name: "is_not_null",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::General,
        description: "Check if a value is not NULL.",
        arguments: &["value"],
        example: Some(Example {
            example: "is_not_null(NULL)",
            output: "false",
        }),
    }),
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Any], DataTypeId::Boolean),
        &IsNull::<true>,
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct IsNull<const NEGATE: bool>;

impl<const NEGATE: bool> ScalarFunction for IsNull<NEGATE> {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Boolean,
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let input = &input.arrays()[0];

        let out = PhysicalBool::get_addressable_mut(&mut output.data)?;
        if input.physical_type() == PhysicalType::UntypedNull {
            // Everything null, just set to default value.
            out.slice.iter_mut().for_each(|v| *v = NEGATE);
            return Ok(());
        }

        let flat = input.flatten()?;

        for (output_idx, idx) in sel.into_iter().enumerate() {
            let is_valid = flat.validity.is_valid(idx);
            if is_valid {
                out.slice[output_idx] = NEGATE;
            } else {
                out.slice[output_idx] = !NEGATE;
            }
        }

        Ok(())
    }
}

pub const FUNCTION_SET_IS_TRUE: ScalarFunctionSet = ScalarFunctionSet {
    name: "is_true",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::General,
        description: "Check if a value is true.",
        arguments: &["value"],
        example: Some(Example {
            example: "is_true(false)",
            output: "false",
        }),
    }),
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Boolean], DataTypeId::Boolean),
        &IsBool::<false, true>,
    )],
};

pub const FUNCTION_SET_IS_NOT_TRUE: ScalarFunctionSet = ScalarFunctionSet {
    name: "is_not_true",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::General,
        description: "Check if a value is not true.",
        arguments: &["value"],
        example: Some(Example {
            example: "is_not_true(false)",
            output: "true",
        }),
    }),
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Boolean], DataTypeId::Boolean),
        &IsBool::<true, true>,
    )],
};

pub const FUNCTION_SET_IS_FALSE: ScalarFunctionSet = ScalarFunctionSet {
    name: "is_false",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::General,
        description: "Check if a value is false.",
        arguments: &["value"],
        example: Some(Example {
            example: "is_false(false)",
            output: "true",
        }),
    }),
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Boolean], DataTypeId::Boolean),
        &IsBool::<false, false>,
    )],
};

pub const FUNCTION_SET_IS_NOT_FALSE: ScalarFunctionSet = ScalarFunctionSet {
    name: "is_not_false",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::General,
        description: "Check if a value is not false.",
        arguments: &["value"],
        example: Some(Example {
            example: "is_not_false(false)",
            output: "false",
        }),
    }),
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
            return_type: DataType::Boolean,
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let input = &input.arrays()[0];

        let out = PhysicalBool::get_addressable_mut(&mut output.data)?;
        let flat = input.flatten()?;
        let input = PhysicalBool::get_addressable(flat.array_buffer)?;

        for (output_idx, idx) in sel.into_iter().enumerate() {
            let is_valid = flat.validity.is_valid(idx);
            if is_valid {
                let val = input.slice[idx];
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
