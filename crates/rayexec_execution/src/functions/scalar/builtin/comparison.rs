use std::cmp::Ordering;
use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_error::{RayexecError, Result};

use crate::arrays::array::exp::Array;
use crate::arrays::array::Array2;
use crate::arrays::batch_exp::Batch;
use crate::arrays::buffer::physical_type::{
    PhysicalBinary,
    PhysicalBool,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalInterval,
    PhysicalStorage,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUtf8,
};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::builder::{ArrayBuilder, BooleanBuffer};
use crate::arrays::executor::physical_type::{
    PhysicalBinary_2,
    PhysicalBool_2,
    PhysicalF16_2,
    PhysicalF32_2,
    PhysicalF64_2,
    PhysicalI128_2,
    PhysicalI16_2,
    PhysicalI32_2,
    PhysicalI64_2,
    PhysicalI8_2,
    PhysicalInterval_2,
    PhysicalType2,
    PhysicalU128_2,
    PhysicalU16_2,
    PhysicalU32_2,
    PhysicalU64_2,
    PhysicalU8_2,
    PhysicalUntypedNull_2,
    PhysicalUtf8_2,
};
use crate::arrays::executor::scalar::{BinaryListReducer2, FlexibleListExecutor};
use crate::arrays::executor_exp::scalar::binary::BinaryExecutor;
use crate::arrays::executor_exp::OutBuffer;
use crate::expr::cast_expr::CastExpr;
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

// TODOs:
//
// - Normalize scales for decimals for comparisons (will be needed elsewhere too).
// - Normalize intervals for comparisons

const fn generate_comparison_sigs(doc: &'static Documentation) -> [Signature; 21] {
    [
        Signature {
            positional_args: &[DataTypeId::Boolean, DataTypeId::Boolean],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(doc),
        },
        Signature {
            positional_args: &[DataTypeId::Int8, DataTypeId::Int8],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(doc),
        },
        Signature {
            positional_args: &[DataTypeId::Int16, DataTypeId::Int16],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(doc),
        },
        Signature {
            positional_args: &[DataTypeId::Int32, DataTypeId::Int32],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(doc),
        },
        Signature {
            positional_args: &[DataTypeId::Int64, DataTypeId::Int64],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(doc),
        },
        Signature {
            positional_args: &[DataTypeId::Int128, DataTypeId::Int128],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(doc),
        },
        Signature {
            positional_args: &[DataTypeId::UInt8, DataTypeId::UInt8],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(doc),
        },
        Signature {
            positional_args: &[DataTypeId::UInt16, DataTypeId::UInt16],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(doc),
        },
        Signature {
            positional_args: &[DataTypeId::UInt32, DataTypeId::UInt32],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(doc),
        },
        Signature {
            positional_args: &[DataTypeId::UInt64, DataTypeId::UInt64],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(doc),
        },
        Signature {
            positional_args: &[DataTypeId::UInt128, DataTypeId::UInt128],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(doc),
        },
        Signature {
            positional_args: &[DataTypeId::Float16, DataTypeId::Float16],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(doc),
        },
        Signature {
            positional_args: &[DataTypeId::Float32, DataTypeId::Float32],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(doc),
        },
        Signature {
            positional_args: &[DataTypeId::Float64, DataTypeId::Float64],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(doc),
        },
        Signature {
            positional_args: &[DataTypeId::Decimal64, DataTypeId::Decimal64],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(doc),
        },
        Signature {
            positional_args: &[DataTypeId::Decimal128, DataTypeId::Decimal128],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(doc),
        },
        Signature {
            positional_args: &[DataTypeId::Timestamp, DataTypeId::Timestamp],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(doc),
        },
        Signature {
            positional_args: &[DataTypeId::Date32, DataTypeId::Date32],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(doc),
        },
        Signature {
            positional_args: &[DataTypeId::Utf8, DataTypeId::Utf8],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(doc),
        },
        Signature {
            positional_args: &[DataTypeId::Binary, DataTypeId::Binary],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(doc),
        },
        Signature {
            positional_args: &[DataTypeId::List, DataTypeId::List],
            variadic_arg: None,
            return_type: DataTypeId::Boolean,
            doc: Some(doc),
        },
    ]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Eq;

impl FunctionInfo for Eq {
    fn name(&self) -> &'static str {
        "="
    }

    fn signatures(&self) -> &[Signature] {
        const DOC: Documentation = Documentation {
            category: Category::General,
            description: "Check if two values are equal. Returns NULL if either argument is NULL.",
            arguments: &["a", "b"],
            example: Some(Example {
                example: "a = b",
                output: "true",
            }),
        };

        const SIGS: &[Signature] = &generate_comparison_sigs(&DOC);

        SIGS
    }
}

impl ScalarFunction for Eq {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        new_planned_comparison_function::<_, EqOperation>(*self, inputs, table_list)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Neq;

impl FunctionInfo for Neq {
    fn name(&self) -> &'static str {
        "<>"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["!="]
    }

    fn signatures(&self) -> &[Signature] {
        const DOC: Documentation = Documentation {
            category: Category::General,
            description:
                "Check if two values are not equal. Returns NULL if either argument is NULL.",
            arguments: &["a", "b"],
            example: Some(Example {
                example: "a != b",
                output: "false",
            }),
        };

        const SIGS: &[Signature] = &generate_comparison_sigs(&DOC);

        SIGS
    }
}

impl ScalarFunction for Neq {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        new_planned_comparison_function::<_, NotEqOperation>(*self, inputs, table_list)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lt;

impl FunctionInfo for Lt {
    fn name(&self) -> &'static str {
        "<"
    }

    fn signatures(&self) -> &[Signature] {
        const DOC: Documentation = Documentation {
            category: Category::General,
            description:
                "Check if the left argument is less than the right. Returns NULL if either argument is NULL.",
            arguments: &["a", "b"],
            example: Some(Example {
                example: "a < b",
                output: "false",
            }),
        };

        const SIGS: &[Signature] = &generate_comparison_sigs(&DOC);

        SIGS
    }
}

impl ScalarFunction for Lt {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        new_planned_comparison_function::<_, LtOperation>(*self, inputs, table_list)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LtEq;

impl FunctionInfo for LtEq {
    fn name(&self) -> &'static str {
        "<="
    }

    fn signatures(&self) -> &[Signature] {
        const DOC: Documentation = Documentation {
            category: Category::General,
            description:
                "Check if the left argument is less than or equal to the right. Returns NULL if either argument is NULL.",
            arguments: &["a", "b"],
            example: Some(Example {
                example: "a <= b",
                output: "true",
            }),
        };

        const SIGS: &[Signature] = &generate_comparison_sigs(&DOC);

        SIGS
    }
}

impl ScalarFunction for LtEq {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        new_planned_comparison_function::<_, LtEqOperation>(*self, inputs, table_list)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gt;

impl FunctionInfo for Gt {
    fn name(&self) -> &'static str {
        ">"
    }

    fn signatures(&self) -> &[Signature] {
        const DOC: Documentation = Documentation {
            category: Category::General,
            description:
                "Check if the left argument is greater than the right. Returns NULL if either argument is NULL.",
            arguments: &["a", "b"],
            example: Some(Example {
                example: "a > b",
                output: "false",
            }),
        };

        const SIGS: &[Signature] = &generate_comparison_sigs(&DOC);

        SIGS
    }
}

impl ScalarFunction for Gt {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        new_planned_comparison_function::<_, GtOperation>(*self, inputs, table_list)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GtEq;

impl FunctionInfo for GtEq {
    fn name(&self) -> &'static str {
        ">="
    }

    fn signatures(&self) -> &[Signature] {
        const DOC: Documentation = Documentation {
            category: Category::General,
            description:
                "Check if the left argument is greater than or equal to the right. Returns NULL if either argument is NULL.",
            arguments: &["a", "b"],
            example: Some(Example {
                example: "a >= b",
                output: "true",
            }),
        };

        const SIGS: &[Signature] = &generate_comparison_sigs(&DOC);

        SIGS
    }
}

impl ScalarFunction for GtEq {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        new_planned_comparison_function::<_, GtEqOperation>(*self, inputs, table_list)
    }
}

/// Describes a comparison betweeen a left and right element.
trait ComparisonOperation: Debug + Sync + Send + Copy + 'static {
    fn compare<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EqOperation;

impl ComparisonOperation for EqOperation {
    fn compare<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        left == right
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NotEqOperation;

impl ComparisonOperation for NotEqOperation {
    fn compare<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        left != right
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LtOperation;

impl ComparisonOperation for LtOperation {
    fn compare<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        left < right
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LtEqOperation;

impl ComparisonOperation for LtEqOperation {
    fn compare<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        left <= right
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GtOperation;

impl ComparisonOperation for GtOperation {
    fn compare<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        left > right
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GtEqOperation;

impl ComparisonOperation for GtEqOperation {
    fn compare<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        left >= right
    }
}

/// Create new planned scalar function for some comparison operation.
///
/// This will normalize input expressions as required.
fn new_planned_comparison_function<F, O>(
    func: F,
    mut inputs: Vec<Expression>,
    table_list: &TableList,
) -> Result<PlannedScalarFunction>
where
    F: ScalarFunction + 'static,
    O: ComparisonOperation,
{
    plan_check_num_args(&func, &inputs, 2)?;

    let function_impl: Box<dyn ScalarFunctionImpl> = match (
        inputs[0].datatype(table_list)?,
        inputs[1].datatype(table_list)?,
    ) {
        (DataType::Boolean, DataType::Boolean) => {
            Box::new(UnnestedComparisonImpl::<O, PhysicalBool>::new())
        }
        (DataType::Int8, DataType::Int8) => {
            Box::new(UnnestedComparisonImpl::<O, PhysicalI8>::new())
        }
        (DataType::Int16, DataType::Int16) => {
            Box::new(UnnestedComparisonImpl::<O, PhysicalI16>::new())
        }
        (DataType::Int32, DataType::Int32) => {
            Box::new(UnnestedComparisonImpl::<O, PhysicalI32>::new())
        }
        (DataType::Int64, DataType::Int64) => {
            Box::new(UnnestedComparisonImpl::<O, PhysicalI64>::new())
        }
        (DataType::Int128, DataType::Int128) => {
            Box::new(UnnestedComparisonImpl::<O, PhysicalI128>::new())
        }
        (DataType::UInt8, DataType::UInt8) => {
            Box::new(UnnestedComparisonImpl::<O, PhysicalU8>::new())
        }
        (DataType::UInt16, DataType::UInt16) => {
            Box::new(UnnestedComparisonImpl::<O, PhysicalU16>::new())
        }
        (DataType::UInt32, DataType::UInt32) => {
            Box::new(UnnestedComparisonImpl::<O, PhysicalU32>::new())
        }
        (DataType::UInt64, DataType::UInt64) => {
            Box::new(UnnestedComparisonImpl::<O, PhysicalU64>::new())
        }
        (DataType::UInt128, DataType::UInt128) => {
            Box::new(UnnestedComparisonImpl::<O, PhysicalU128>::new())
        }
        (DataType::Float16, DataType::Float16) => {
            Box::new(UnnestedComparisonImpl::<O, PhysicalF16>::new())
        }
        (DataType::Float32, DataType::Float32) => {
            Box::new(UnnestedComparisonImpl::<O, PhysicalF32>::new())
        }
        (DataType::Float64, DataType::Float64) => {
            Box::new(UnnestedComparisonImpl::<O, PhysicalF64>::new())
        }
        (DataType::Decimal64(left), DataType::Decimal64(right)) => {
            // Normalize decimals.
            match left.scale.cmp(&right.scale) {
                Ordering::Less => {
                    // Scale up left.
                    inputs[0] = Expression::Cast(CastExpr {
                        to: DataType::Decimal64(right),
                        expr: Box::new(inputs[0].clone()),
                    })
                }
                Ordering::Greater => {
                    // Scale up right.
                    inputs[1] = Expression::Cast(CastExpr {
                        to: DataType::Decimal64(left),
                        expr: Box::new(inputs[1].clone()),
                    })
                }
                Ordering::Equal => (), // Nothing to do
            }
            Box::new(UnnestedComparisonImpl::<O, PhysicalI64>::new())
        }
        (DataType::Decimal128(left), DataType::Decimal128(right)) => {
            // Normalize decimals.
            match left.scale.cmp(&right.scale) {
                Ordering::Less => {
                    // Scale up left.
                    inputs[0] = Expression::Cast(CastExpr {
                        to: DataType::Decimal128(right),
                        expr: Box::new(inputs[0].clone()),
                    })
                }
                Ordering::Greater => {
                    // Scale up right.
                    inputs[1] = Expression::Cast(CastExpr {
                        to: DataType::Decimal128(left),
                        expr: Box::new(inputs[1].clone()),
                    })
                }
                Ordering::Equal => (), // Nothing to do
            }
            Box::new(UnnestedComparisonImpl::<O, PhysicalI128>::new())
        }
        (DataType::Timestamp(_), DataType::Timestamp(_)) => {
            Box::new(UnnestedComparisonImpl::<O, PhysicalBool>::new())
        }
        (DataType::Interval, DataType::Interval) => {
            Box::new(UnnestedComparisonImpl::<O, PhysicalInterval>::new())
        }
        (DataType::Date32, DataType::Date32) => {
            Box::new(UnnestedComparisonImpl::<O, PhysicalI32>::new())
        }
        (DataType::Date64, DataType::Date64) => {
            Box::new(UnnestedComparisonImpl::<O, PhysicalI64>::new())
        }
        (DataType::Utf8, DataType::Utf8) => {
            Box::new(UnnestedComparisonImpl::<O, PhysicalUtf8>::new())
        }
        (DataType::Binary, DataType::Binary) => {
            Box::new(UnnestedComparisonImpl::<O, PhysicalBinary>::new())
        }

        (a, b) => return Err(invalid_input_types_error(&func, &[a, b])),
    };

    Ok(PlannedScalarFunction {
        function: Box::new(func),
        return_type: DataType::Boolean,
        inputs,
        function_impl,
    })
}

#[derive(Debug)]
struct ListComparisonReducer<T, O> {
    left_len: i32,
    right_len: i32,
    all_equal: bool,
    result: Option<bool>,
    _typ: PhantomData<T>,
    _op: PhantomData<O>,
}

impl<T, O> BinaryListReducer2<T, bool> for ListComparisonReducer<T, O>
where
    T: PartialEq + PartialOrd,
    O: ComparisonOperation,
{
    fn new(left_len: i32, right_len: i32) -> Self {
        ListComparisonReducer {
            all_equal: true,
            result: None,
            left_len,
            right_len,
            _op: PhantomData,
            _typ: PhantomData,
        }
    }

    fn put_values(&mut self, v1: T, v2: T) {
        if self.result.is_some() {
            return;
        }
        if v1 != v2 {
            self.all_equal = false;
            self.result = Some(O::compare(v1, v2));
        }
    }

    fn finish(self) -> bool {
        if let Some(result) = self.result {
            return result;
        }

        if self.all_equal {
            O::compare(self.left_len, self.right_len)
        } else {
            true
        }
    }
}

#[derive(Debug, Clone)]
struct ListComparisonImpl<O> {
    inner_physical_type: PhysicalType2,
    _op: PhantomData<O>,
}

impl<O> ListComparisonImpl<O> {
    fn new(inner_physical_type: PhysicalType2) -> Self {
        ListComparisonImpl {
            _op: PhantomData,
            inner_physical_type,
        }
    }
}

impl<O> ScalarFunctionImpl for ListComparisonImpl<O>
where
    O: ComparisonOperation,
{
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        unimplemented!()
    }

    fn execute2(&self, inputs: &[&Array2]) -> Result<Array2> {
        let left = inputs[0];
        let right = inputs[1];

        let builder = ArrayBuilder {
            datatype: DataType::Boolean,
            buffer: BooleanBuffer::with_len(left.logical_len()),
        };

        let array = match self.inner_physical_type {
            PhysicalType2::UntypedNull => FlexibleListExecutor::binary_reduce::<
                PhysicalUntypedNull_2,
                _,
                ListComparisonReducer<_, O>,
            >(left, right, builder)?,
            PhysicalType2::Boolean => {
                FlexibleListExecutor::binary_reduce::<PhysicalBool_2, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType2::Int8 => {
                FlexibleListExecutor::binary_reduce::<PhysicalI8_2, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType2::Int16 => {
                FlexibleListExecutor::binary_reduce::<PhysicalI16_2, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType2::Int32 => {
                FlexibleListExecutor::binary_reduce::<PhysicalI32_2, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType2::Int64 => {
                FlexibleListExecutor::binary_reduce::<PhysicalI64_2, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType2::Int128 => {
                FlexibleListExecutor::binary_reduce::<PhysicalI128_2, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType2::UInt8 => {
                FlexibleListExecutor::binary_reduce::<PhysicalU8_2, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType2::UInt16 => {
                FlexibleListExecutor::binary_reduce::<PhysicalU16_2, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType2::UInt32 => {
                FlexibleListExecutor::binary_reduce::<PhysicalU32_2, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType2::UInt64 => {
                FlexibleListExecutor::binary_reduce::<PhysicalU64_2, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType2::UInt128 => {
                FlexibleListExecutor::binary_reduce::<PhysicalU128_2, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType2::Float16 => {
                FlexibleListExecutor::binary_reduce::<PhysicalF16_2, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType2::Float32 => {
                FlexibleListExecutor::binary_reduce::<PhysicalF32_2, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType2::Float64 => {
                FlexibleListExecutor::binary_reduce::<PhysicalF64_2, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType2::Interval => FlexibleListExecutor::binary_reduce::<
                PhysicalInterval_2,
                _,
                ListComparisonReducer<_, O>,
            >(left, right, builder)?,
            PhysicalType2::Binary => FlexibleListExecutor::binary_reduce::<
                PhysicalBinary_2,
                _,
                ListComparisonReducer<_, O>,
            >(left, right, builder)?,
            PhysicalType2::Utf8 => {
                FlexibleListExecutor::binary_reduce::<PhysicalUtf8_2, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType2::List => {
                return Err(RayexecError::new(
                    "Comparison between nested lists not yet supported",
                ))
            }
        };

        Ok(array)
    }
}

#[derive(Debug, Clone)]
struct UnnestedComparisonImpl<O: ComparisonOperation, S: PhysicalStorage> {
    _op: PhantomData<O>,
    _s: PhantomData<S>,
}

impl<O, S> UnnestedComparisonImpl<O, S>
where
    O: ComparisonOperation,
    S: PhysicalStorage,
{
    const fn new() -> Self {
        UnnestedComparisonImpl {
            _op: PhantomData,
            _s: PhantomData,
        }
    }
}

impl<O, S> ScalarFunctionImpl for UnnestedComparisonImpl<O, S>
where
    O: ComparisonOperation,
    S: PhysicalStorage,
    S::StorageType: PartialEq + PartialOrd,
{
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let left = &input.arrays()[0];
        let right = &input.arrays()[1];

        BinaryExecutor::execute::<S, S, PhysicalBool, _>(
            left,
            sel,
            right,
            sel,
            OutBuffer::from_array(output)?,
            |left, right, buf| buf.put(&O::compare(left, right)),
        )
    }
}

#[cfg(test)]
mod tests {

    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::buffer::buffer_manager::NopBufferManager;
    use crate::arrays::testutil::assert_arrays_eq;
    use crate::expr;

    #[test]
    fn eq_i32() {
        let a = Array::try_from_iter([1, 2, 3]).unwrap();
        let b = Array::try_from_iter([2, 2, 6]).unwrap();
        let batch = Batch::from_arrays([a, b], true).unwrap();

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(
                None,
                vec![DataType::Int32, DataType::Int32],
                vec!["a".to_string(), "b".to_string()],
            )
            .unwrap();

        let planned = Eq
            .plan(
                &table_list,
                vec![expr::col_ref(table_ref, 0), expr::col_ref(table_ref, 1)],
            )
            .unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Boolean, 3).unwrap();
        planned.function_impl.execute(&batch, &mut out).unwrap();
        let expected = Array::try_from_iter([false, true, false]).unwrap();

        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn neq_i32() {
        let a = Array::try_from_iter([1, 2, 3]).unwrap();
        let b = Array::try_from_iter([2, 2, 6]).unwrap();
        let batch = Batch::from_arrays([a, b], true).unwrap();

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(
                None,
                vec![DataType::Int32, DataType::Int32],
                vec!["a".to_string(), "b".to_string()],
            )
            .unwrap();

        let planned = Neq
            .plan(
                &table_list,
                vec![expr::col_ref(table_ref, 0), expr::col_ref(table_ref, 1)],
            )
            .unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Boolean, 3).unwrap();
        planned.function_impl.execute(&batch, &mut out).unwrap();
        let expected = Array::try_from_iter([true, false, true]).unwrap();

        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn lt_i32() {
        let a = Array::try_from_iter([1, 2, 3]).unwrap();
        let b = Array::try_from_iter([2, 2, 6]).unwrap();
        let batch = Batch::from_arrays([a, b], true).unwrap();

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(
                None,
                vec![DataType::Int32, DataType::Int32],
                vec!["a".to_string(), "b".to_string()],
            )
            .unwrap();

        let planned = Lt
            .plan(
                &table_list,
                vec![expr::col_ref(table_ref, 0), expr::col_ref(table_ref, 1)],
            )
            .unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Boolean, 3).unwrap();
        planned.function_impl.execute(&batch, &mut out).unwrap();
        let expected = Array::try_from_iter([true, false, true]).unwrap();

        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn lt_eq_i32() {
        let a = Array::try_from_iter([1, 2, 3]).unwrap();
        let b = Array::try_from_iter([2, 2, 6]).unwrap();
        let batch = Batch::from_arrays([a, b], true).unwrap();

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(
                None,
                vec![DataType::Int32, DataType::Int32],
                vec!["a".to_string(), "b".to_string()],
            )
            .unwrap();

        let planned = LtEq
            .plan(
                &table_list,
                vec![expr::col_ref(table_ref, 0), expr::col_ref(table_ref, 1)],
            )
            .unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Boolean, 3).unwrap();
        planned.function_impl.execute(&batch, &mut out).unwrap();
        let expected = Array::try_from_iter([true, true, true]).unwrap();

        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn gt_i32() {
        let a = Array::try_from_iter([1, 2, 3]).unwrap();
        let b = Array::try_from_iter([2, 2, 6]).unwrap();
        let batch = Batch::from_arrays([a, b], true).unwrap();

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(
                None,
                vec![DataType::Int32, DataType::Int32],
                vec!["a".to_string(), "b".to_string()],
            )
            .unwrap();

        let planned = Gt
            .plan(
                &table_list,
                vec![expr::col_ref(table_ref, 0), expr::col_ref(table_ref, 1)],
            )
            .unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Boolean, 3).unwrap();
        planned.function_impl.execute(&batch, &mut out).unwrap();
        let expected = Array::try_from_iter([false, false, false]).unwrap();

        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn gt_eq_i32() {
        let a = Array::try_from_iter([1, 2, 3]).unwrap();
        let b = Array::try_from_iter([2, 2, 6]).unwrap();
        let batch = Batch::from_arrays([a, b], true).unwrap();

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(
                None,
                vec![DataType::Int32, DataType::Int32],
                vec!["a".to_string(), "b".to_string()],
            )
            .unwrap();

        let planned = GtEq
            .plan(
                &table_list,
                vec![expr::col_ref(table_ref, 0), expr::col_ref(table_ref, 1)],
            )
            .unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Boolean, 3).unwrap();
        planned.function_impl.execute(&batch, &mut out).unwrap();
        let expected = Array::try_from_iter([false, true, false]).unwrap();

        assert_arrays_eq(&expected, &out);
    }
}
