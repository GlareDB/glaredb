use std::cmp::Ordering;
use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_error::{not_implemented, Result};

use crate::arrays::array::physical_type::{
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
    PhysicalType,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUntypedNull,
    PhysicalUtf8,
};
use crate::arrays::array::{Array, ArrayData};
use crate::arrays::compute::cast::array::decimal_rescale;
use crate::arrays::compute::cast::behavior::CastFailBehavior;
use crate::arrays::datatype::{DataType, DataTypeId, DecimalTypeMeta};
use crate::arrays::executor::builder::{ArrayBuilder, BooleanBuffer};
use crate::arrays::executor::scalar::{BinaryExecutor, BinaryListReducer, FlexibleListExecutor};
use crate::arrays::scalar::decimal::{Decimal128Type, Decimal64Type, DecimalType};
use crate::arrays::storage::PrimitiveStorage;
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
        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataType::Boolean,
            function_impl: new_comparison_impl::<EqOperation>(self, &inputs, table_list)?,
            inputs,
        })
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
        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataType::Boolean,
            function_impl: new_comparison_impl::<NotEqOperation>(self, &inputs, table_list)?,
            inputs,
        })
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
        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataType::Boolean,
            function_impl: new_comparison_impl::<LtOperation>(self, &inputs, table_list)?,
            inputs,
        })
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
        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataType::Boolean,
            function_impl: new_comparison_impl::<LtEqOperation>(self, &inputs, table_list)?,
            inputs,
        })
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
        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataType::Boolean,
            function_impl: new_comparison_impl::<GtOperation>(self, &inputs, table_list)?,
            inputs,
        })
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
        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataType::Boolean,
            function_impl: new_comparison_impl::<GtEqOperation>(self, &inputs, table_list)?,
            inputs,
        })
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

/// Creates a new scalar function implementation based on input types.
fn new_comparison_impl<O: ComparisonOperation>(
    func: &impl FunctionInfo,
    inputs: &[Expression],
    table_list: &TableList,
) -> Result<Box<dyn ScalarFunctionImpl>> {
    plan_check_num_args(func, inputs, 2)?;
    Ok(
        match (
            inputs[0].datatype(table_list)?,
            inputs[1].datatype(table_list)?,
        ) {
            (DataType::Boolean, DataType::Boolean) => {
                Box::new(BaseComparisonImpl::<O, PhysicalBool>::new())
            }
            (DataType::Int8, DataType::Int8) => {
                Box::new(BaseComparisonImpl::<O, PhysicalI8>::new())
            }
            (DataType::Int16, DataType::Int16) => {
                Box::new(BaseComparisonImpl::<O, PhysicalI16>::new())
            }
            (DataType::Int32, DataType::Int32) => {
                Box::new(BaseComparisonImpl::<O, PhysicalI32>::new())
            }
            (DataType::Int64, DataType::Int64) => {
                Box::new(BaseComparisonImpl::<O, PhysicalI64>::new())
            }
            (DataType::Int128, DataType::Int128) => {
                Box::new(BaseComparisonImpl::<O, PhysicalI128>::new())
            }

            (DataType::UInt8, DataType::UInt8) => {
                Box::new(BaseComparisonImpl::<O, PhysicalU8>::new())
            }
            (DataType::UInt16, DataType::UInt16) => {
                Box::new(BaseComparisonImpl::<O, PhysicalU16>::new())
            }
            (DataType::UInt32, DataType::UInt32) => {
                Box::new(BaseComparisonImpl::<O, PhysicalU32>::new())
            }
            (DataType::UInt64, DataType::UInt64) => {
                Box::new(BaseComparisonImpl::<O, PhysicalU64>::new())
            }
            (DataType::UInt128, DataType::UInt128) => {
                Box::new(BaseComparisonImpl::<O, PhysicalU128>::new())
            }
            (DataType::Float16, DataType::Float16) => {
                Box::new(BaseComparisonImpl::<O, PhysicalF16>::new())
            }
            (DataType::Float32, DataType::Float32) => {
                Box::new(BaseComparisonImpl::<O, PhysicalF32>::new())
            }
            (DataType::Float64, DataType::Float64) => {
                Box::new(BaseComparisonImpl::<O, PhysicalF64>::new())
            }
            (DataType::Decimal64(left), DataType::Decimal64(right)) => Box::new(
                RescalingComparisionImpl::<O, Decimal64Type>::new(left, right),
            ),
            (DataType::Decimal128(left), DataType::Decimal128(right)) => Box::new(
                RescalingComparisionImpl::<O, Decimal128Type>::new(left, right),
            ),
            (DataType::Timestamp(_), DataType::Timestamp(_)) => {
                Box::new(BaseComparisonImpl::<O, PhysicalBool>::new())
            }
            (DataType::Interval, DataType::Interval) => {
                Box::new(BaseComparisonImpl::<O, PhysicalInterval>::new())
            }
            (DataType::Date32, DataType::Date32) => {
                Box::new(BaseComparisonImpl::<O, PhysicalI32>::new())
            }
            (DataType::Date64, DataType::Date64) => {
                Box::new(BaseComparisonImpl::<O, PhysicalI64>::new())
            }
            (DataType::Utf8, DataType::Utf8) => {
                Box::new(BaseComparisonImpl::<O, PhysicalUtf8>::new())
            }
            (DataType::Binary, DataType::Binary) => {
                Box::new(BaseComparisonImpl::<O, PhysicalBinary>::new())
            }
            (DataType::List(m1), DataType::List(m2)) if m1 == m2 => {
                // TODO: We'll want to figure out casting for lists.
                Box::new(ListComparisonImpl::<O>::new(m1.datatype.physical_type()?))
            }
            (a, b) => return Err(invalid_input_types_error(func, &[a, b])),
        },
    )
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

impl<T, O> BinaryListReducer<T, bool> for ListComparisonReducer<T, O>
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
    inner_physical_type: PhysicalType,
    _op: PhantomData<O>,
}

impl<O> ListComparisonImpl<O> {
    fn new(inner_physical_type: PhysicalType) -> Self {
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
    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let left = inputs[0];
        let right = inputs[1];

        let builder = ArrayBuilder {
            datatype: DataType::Boolean,
            buffer: BooleanBuffer::with_len(left.logical_len()),
        };

        let array = match self.inner_physical_type {
            PhysicalType::UntypedNull => FlexibleListExecutor::binary_reduce::<
                PhysicalUntypedNull,
                _,
                ListComparisonReducer<_, O>,
            >(left, right, builder)?,
            PhysicalType::Boolean => {
                FlexibleListExecutor::binary_reduce::<PhysicalBool, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType::Int8 => {
                FlexibleListExecutor::binary_reduce::<PhysicalI8, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType::Int16 => {
                FlexibleListExecutor::binary_reduce::<PhysicalI16, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType::Int32 => {
                FlexibleListExecutor::binary_reduce::<PhysicalI32, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType::Int64 => {
                FlexibleListExecutor::binary_reduce::<PhysicalI64, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType::Int128 => {
                FlexibleListExecutor::binary_reduce::<PhysicalI128, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType::UInt8 => {
                FlexibleListExecutor::binary_reduce::<PhysicalU8, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType::UInt16 => {
                FlexibleListExecutor::binary_reduce::<PhysicalU16, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType::UInt32 => {
                FlexibleListExecutor::binary_reduce::<PhysicalU32, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType::UInt64 => {
                FlexibleListExecutor::binary_reduce::<PhysicalU64, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType::UInt128 => {
                FlexibleListExecutor::binary_reduce::<PhysicalU128, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType::Float16 => {
                FlexibleListExecutor::binary_reduce::<PhysicalF16, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType::Float32 => {
                FlexibleListExecutor::binary_reduce::<PhysicalF32, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType::Float64 => {
                FlexibleListExecutor::binary_reduce::<PhysicalF64, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType::Interval => FlexibleListExecutor::binary_reduce::<
                PhysicalInterval,
                _,
                ListComparisonReducer<_, O>,
            >(left, right, builder)?,
            PhysicalType::Binary => {
                FlexibleListExecutor::binary_reduce::<PhysicalBinary, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            PhysicalType::Utf8 => {
                FlexibleListExecutor::binary_reduce::<PhysicalUtf8, _, ListComparisonReducer<_, O>>(
                    left, right, builder,
                )?
            }
            other => not_implemented!("comparison: {other}"),
        };

        Ok(array)
    }
}

#[derive(Debug, Clone)]
struct BaseComparisonImpl<O: ComparisonOperation, S: PhysicalStorage> {
    _op: PhantomData<O>,
    _s: PhantomData<S>,
}

impl<O, S> BaseComparisonImpl<O, S>
where
    O: ComparisonOperation,
    S: PhysicalStorage,
    for<'a> S::Type<'a>: PartialEq + PartialOrd,
{
    fn new() -> Self {
        BaseComparisonImpl {
            _op: PhantomData,
            _s: PhantomData,
        }
    }
}

impl<O, S> ScalarFunctionImpl for BaseComparisonImpl<O, S>
where
    O: ComparisonOperation,
    S: PhysicalStorage,
    for<'a> S::Type<'a>: PartialEq + PartialOrd,
{
    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let left = inputs[0];
        let right = inputs[1];

        let builder = ArrayBuilder {
            datatype: DataType::Boolean,
            buffer: BooleanBuffer::with_len(left.logical_len()),
        };

        BinaryExecutor::execute::<S, S, _, _>(left, right, builder, |a, b, buf| {
            buf.put(&O::compare(a, b))
        })
    }
}

// TODO: Determine if this is still needed. Ideally scaling happens prior to
// calling the comparison function.
#[derive(Debug, Clone)]
struct RescalingComparisionImpl<O: ComparisonOperation, T: DecimalType> {
    _op: PhantomData<O>,
    _t: PhantomData<T>,

    left: DecimalTypeMeta,
    right: DecimalTypeMeta,
}

impl<O, T> RescalingComparisionImpl<O, T>
where
    O: ComparisonOperation,
    T: DecimalType,
    ArrayData: From<PrimitiveStorage<T::Primitive>>,
{
    fn new(left: DecimalTypeMeta, right: DecimalTypeMeta) -> Self {
        RescalingComparisionImpl {
            _op: PhantomData,
            _t: PhantomData,
            left,
            right,
        }
    }
}

impl<O, T> ScalarFunctionImpl for RescalingComparisionImpl<O, T>
where
    O: ComparisonOperation,
    T: DecimalType,
    ArrayData: From<PrimitiveStorage<T::Primitive>>,
{
    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let left = inputs[0];
        let right = inputs[1];

        let builder = ArrayBuilder {
            datatype: DataType::Boolean,
            buffer: BooleanBuffer::with_len(left.logical_len()),
        };

        match self.left.scale.cmp(&self.right.scale) {
            Ordering::Greater => {
                let scaled_right = decimal_rescale::<T::Storage, T>(
                    right,
                    left.datatype().clone(),
                    CastFailBehavior::Error,
                )?;

                BinaryExecutor::execute::<T::Storage, T::Storage, _, _>(
                    left,
                    &scaled_right,
                    builder,
                    |a, b, buf| buf.put(&O::compare(a, b)),
                )
            }
            Ordering::Less => {
                let scaled_left = decimal_rescale::<T::Storage, T>(
                    left,
                    right.datatype().clone(),
                    CastFailBehavior::Error,
                )?;

                BinaryExecutor::execute::<T::Storage, T::Storage, _, _>(
                    &scaled_left,
                    right,
                    builder,
                    |a, b, buf| buf.put(&O::compare(a, b)),
                )
            }
            Ordering::Equal => BinaryExecutor::execute::<T::Storage, T::Storage, _, _>(
                left,
                right,
                builder,
                |a, b, buf| buf.put(&O::compare(a, b)),
            ),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::expr;

    #[test]
    fn eq_i32() {
        let a = Array::from_iter([1, 2, 3]);
        let b = Array::from_iter([2, 2, 6]);

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

        let out = planned.function_impl.execute(&[&a, &b]).unwrap();
        let expected = Array::from_iter([false, true, false]);

        assert_eq!(expected, out);
    }

    #[test]
    fn neq_i32() {
        let a = Array::from_iter([1, 2, 3]);
        let b = Array::from_iter([2, 2, 6]);

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

        let out = planned.function_impl.execute(&[&a, &b]).unwrap();
        let expected = Array::from_iter([true, false, true]);

        assert_eq!(expected, out);
    }

    #[test]
    fn lt_i32() {
        let a = Array::from_iter([1, 2, 3]);
        let b = Array::from_iter([2, 2, 6]);

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

        let out = planned.function_impl.execute(&[&a, &b]).unwrap();
        let expected = Array::from_iter([true, false, true]);

        assert_eq!(expected, out);
    }

    #[test]
    fn lt_eq_i32() {
        let a = Array::from_iter([1, 2, 3]);
        let b = Array::from_iter([2, 2, 6]);

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

        let out = planned.function_impl.execute(&[&a, &b]).unwrap();
        let expected = Array::from_iter([true, true, true]);

        assert_eq!(expected, out);
    }

    #[test]
    fn gt_i32() {
        let a = Array::from_iter([1, 2, 3]);
        let b = Array::from_iter([2, 2, 6]);

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

        let out = planned.function_impl.execute(&[&a, &b]).unwrap();
        let expected = Array::from_iter([false, false, false]);

        assert_eq!(expected, out);
    }

    #[test]
    fn gt_eq_i32() {
        let a = Array::from_iter([1, 2, 3]);
        let b = Array::from_iter([2, 2, 6]);

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

        let out = planned.function_impl.execute(&[&a, &b]).unwrap();
        let expected = Array::from_iter([false, true, false]);

        assert_eq!(expected, out);
    }
}
