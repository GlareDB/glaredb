use rayexec_error::Result;

use crate::arrays::array::physical_type::{PhysicalAny, PhysicalBool};
use crate::arrays::array::Array;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::builder::{ArrayBuilder, BooleanBuffer};
use crate::arrays::executor::scalar::UnaryExecutor;
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
    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let input = inputs[0];

        let (initial, updated) = if IS_NULL {
            // Executor will only execute on non-null inputs, so we can assume
            // everything is null first then selectively set false for things
            // that the executor executes.
            (true, false)
        } else {
            (false, true)
        };

        let builder = ArrayBuilder {
            datatype: DataType::Boolean,
            buffer: BooleanBuffer::with_len_and_default_value(input.logical_len(), initial),
        };
        let array = UnaryExecutor::execute2::<PhysicalAny, _, _>(input, builder, |_, buf| {
            buf.put(&updated)
        })?;

        // Drop validity.
        let data = array.into_array_data();
        Ok(Array::new_with_array_data(DataType::Boolean, data))
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
    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let input = inputs[0];

        let initial = NOT;

        let builder = ArrayBuilder {
            datatype: DataType::Boolean,
            buffer: BooleanBuffer::with_len_and_default_value(input.logical_len(), initial),
        };
        let array = UnaryExecutor::execute2::<PhysicalBool, _, _>(input, builder, |val, buf| {
            let b = if NOT { val != BOOL } else { val == BOOL };
            buf.put(&b)
        })?;

        // Drop validity.
        let data = array.into_array_data();
        Ok(Array::new_with_array_data(DataType::Boolean, data))
    }
}
