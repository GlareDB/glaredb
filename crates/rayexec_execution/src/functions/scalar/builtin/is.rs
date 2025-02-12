use rayexec_error::Result;

use crate::arrays::array::physical_type::{
    MutableScalarStorage,
    PhysicalBool,
    PhysicalType,
    ScalarStorage,
};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsNull;

impl FunctionInfo for IsNull {
    fn name(&self) -> &'static str {
        "is_null"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Any],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(&Documentation {
                category: Category::General,
                description: "Check if a value is NULL.",
                arguments: &["value"],
                example: Some(Example {
                    example: "is_null(NULL)",
                    output: "true",
                }),
            }),
        }]
    }
}

impl ScalarFunction for IsNull {
    fn plan(
        &self,
        _table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args(self, &inputs, 1)?;

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataType::Boolean,
            inputs,
            function_impl: Box::new(CheckNullImpl::<true>),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsNotNull;

impl FunctionInfo for IsNotNull {
    fn name(&self) -> &'static str {
        "is_not_null"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Any],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(&Documentation {
                category: Category::General,
                description: "Check if a value is not NULL.",
                arguments: &["value"],
                example: Some(Example {
                    example: "is_not_null(NULL)",
                    output: "false",
                }),
            }),
        }]
    }
}

impl ScalarFunction for IsNotNull {
    fn plan(
        &self,
        _table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args(self, &inputs, 1)?;

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataType::Boolean,
            inputs,
            function_impl: Box::new(CheckNullImpl::<false>),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckNullImpl<const IS_NULL: bool>;

impl<const IS_NULL: bool> ScalarFunctionImpl for CheckNullImpl<IS_NULL> {
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let input = &input.arrays()[0];

        let out = PhysicalBool::get_addressable_mut(&mut output.data)?;
        if input.physical_type() == PhysicalType::UntypedNull {
            // Everything null, just set to default value.
            out.slice.iter_mut().for_each(|v| *v = IS_NULL);
            return Ok(());
        }

        let flat = input.flatten()?;

        for (output_idx, idx) in sel.into_iter().enumerate() {
            let is_valid = flat.validity.is_valid(idx);
            if is_valid {
                out.slice[output_idx] = !IS_NULL;
            } else {
                out.slice[output_idx] = IS_NULL;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsTrue;

impl FunctionInfo for IsTrue {
    fn name(&self) -> &'static str {
        "is_true"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Boolean],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(&Documentation {
                category: Category::General,
                description: "Check if a value is true.",
                arguments: &["value"],
                example: Some(Example {
                    example: "is_true(false)",
                    output: "false",
                }),
            }),
        }]
    }
}

impl ScalarFunction for IsTrue {
    fn plan(
        &self,
        _table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args(self, &inputs, 1)?;

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataType::Boolean,
            inputs,
            function_impl: Box::new(CheckBoolImpl::<false, true>),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsNotTrue;

impl FunctionInfo for IsNotTrue {
    fn name(&self) -> &'static str {
        "is_not_true"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Boolean],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(&Documentation {
                category: Category::General,
                description: "Check if a value is not true.",
                arguments: &["value"],
                example: Some(Example {
                    example: "is_not_true(false)",
                    output: "true",
                }),
            }),
        }]
    }
}

impl ScalarFunction for IsNotTrue {
    fn plan(
        &self,
        _table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args(self, &inputs, 1)?;

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataType::Boolean,
            inputs,
            function_impl: Box::new(CheckBoolImpl::<true, true>),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsFalse;

impl FunctionInfo for IsFalse {
    fn name(&self) -> &'static str {
        "is_false"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Boolean],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(&Documentation {
                category: Category::General,
                description: "Check if a value is false.",
                arguments: &["value"],
                example: Some(Example {
                    example: "is_false(false)",
                    output: "true",
                }),
            }),
        }]
    }
}

impl ScalarFunction for IsFalse {
    fn plan(
        &self,
        _table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args(self, &inputs, 1)?;

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataType::Boolean,
            inputs,
            function_impl: Box::new(CheckBoolImpl::<false, false>),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsNotFalse;

impl FunctionInfo for IsNotFalse {
    fn name(&self) -> &'static str {
        "is_not_false"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Boolean],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(&Documentation {
                category: Category::General,
                description: "Check if a value is not false.",
                arguments: &["value"],
                example: Some(Example {
                    example: "is_not_false(false)",
                    output: "false",
                }),
            }),
        }]
    }
}

impl ScalarFunction for IsNotFalse {
    fn plan(
        &self,
        _table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args(self, &inputs, 1)?;

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataType::Boolean,
            inputs,
            function_impl: Box::new(CheckBoolImpl::<true, false>),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckBoolImpl<const NOT: bool, const BOOL: bool>;

impl<const NOT: bool, const BOOL: bool> ScalarFunctionImpl for CheckBoolImpl<NOT, BOOL> {
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let input = &input.arrays()[0];

        let out = PhysicalBool::get_addressable_mut(&mut output.data)?;
        let flat = input.flatten()?;
        let input = PhysicalBool::get_addressable(&flat.array_buffer)?;

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

#[cfg(test)]
mod tests {

    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::buffer_manager::NopBufferManager;
    use crate::expr;
    use crate::testutil::arrays::assert_arrays_eq;

    #[test]
    fn is_null_all_valid() {
        let a = Array::try_from_iter([1, 2, 3]).unwrap();
        let batch = Batch::try_from_arrays([a]).unwrap();

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(None, vec![DataType::Boolean], vec!["a".to_string()])
            .unwrap();

        let planned = IsNull
            .plan(&table_list, vec![expr::col_ref(table_ref, 0)])
            .unwrap();

        let mut out = Array::try_new(&NopBufferManager, DataType::Boolean, 3).unwrap();
        planned.function_impl.execute(&batch, &mut out).unwrap();

        let expected = Array::try_from_iter([false, false, false]).unwrap();

        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn is_null_some_invalid() {
        let a = Array::try_from_iter([Some(1), None, None]).unwrap();
        let batch = Batch::try_from_arrays([a]).unwrap();

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(None, vec![DataType::Boolean], vec!["a".to_string()])
            .unwrap();

        let planned = IsNull
            .plan(&table_list, vec![expr::col_ref(table_ref, 0)])
            .unwrap();

        let mut out = Array::try_new(&NopBufferManager, DataType::Boolean, 3).unwrap();
        planned.function_impl.execute(&batch, &mut out).unwrap();

        let expected = Array::try_from_iter([false, true, true]).unwrap();

        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn is_true() {
        let a = Array::try_from_iter([Some(true), Some(false), None]).unwrap();
        let batch = Batch::try_from_arrays([a]).unwrap();

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(None, vec![DataType::Boolean], vec!["a".to_string()])
            .unwrap();

        let planned = IsTrue
            .plan(&table_list, vec![expr::col_ref(table_ref, 0)])
            .unwrap();

        let mut out = Array::try_new(&NopBufferManager, DataType::Boolean, 3).unwrap();
        planned.function_impl.execute(&batch, &mut out).unwrap();

        let expected = Array::try_from_iter([Some(true), Some(false), Some(false)]).unwrap();

        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn is_not_true() {
        let a = Array::try_from_iter([Some(true), Some(false), None]).unwrap();
        let batch = Batch::try_from_arrays([a]).unwrap();

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(None, vec![DataType::Boolean], vec!["a".to_string()])
            .unwrap();

        let planned = IsNotTrue
            .plan(&table_list, vec![expr::col_ref(table_ref, 0)])
            .unwrap();

        let mut out = Array::try_new(&NopBufferManager, DataType::Boolean, 3).unwrap();
        planned.function_impl.execute(&batch, &mut out).unwrap();

        let expected = Array::try_from_iter([Some(false), Some(true), Some(true)]).unwrap();

        assert_arrays_eq(&expected, &out);
    }
}
