use std::cmp::Ordering;
use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_bullet::array::{Array, ArrayData};
use rayexec_bullet::compute::cast::array::decimal_rescale;
use rayexec_bullet::compute::cast::behavior::CastFailBehavior;
use rayexec_bullet::datatype::{DataType, DataTypeId, DecimalTypeMeta};
use rayexec_bullet::executor::builder::{ArrayBuilder, BooleanBuffer};
use rayexec_bullet::executor::physical_type::{
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
use rayexec_bullet::executor::scalar::BinaryExecutor;
use rayexec_bullet::scalar::decimal::{Decimal128Type, Decimal64Type, DecimalType};
use rayexec_bullet::storage::PrimitiveStorage;
use rayexec_error::Result;

use crate::expr::Expression;
use crate::functions::scalar::{PlannedScalarFuntion, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::bind_context::BindContext;
use crate::logical::binder::table_list::TableList;

// TODOs:
//
// - Normalize scales for decimals for comparisons (will be needed elsewhere too).
// - Normalize intervals for comparisons

const COMPARISON_SIGNATURES: &[Signature] = &[
    Signature {
        input: &[DataTypeId::Boolean, DataTypeId::Boolean],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Int8, DataTypeId::Int8],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Int16, DataTypeId::Int16],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Int32, DataTypeId::Int32],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Int64, DataTypeId::Int64],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Int128, DataTypeId::Int128],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::UInt8, DataTypeId::UInt8],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::UInt16, DataTypeId::UInt16],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::UInt32, DataTypeId::UInt32],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::UInt64, DataTypeId::UInt64],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::UInt128, DataTypeId::UInt128],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Float16, DataTypeId::Float16],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Float32, DataTypeId::Float32],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Float64, DataTypeId::Float64],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Decimal64, DataTypeId::Decimal64],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Decimal128, DataTypeId::Decimal128],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Timestamp, DataTypeId::Timestamp],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Date32, DataTypeId::Date32],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Utf8, DataTypeId::Utf8],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Binary, DataTypeId::Binary],
        variadic: None,
        return_type: DataTypeId::Boolean,
    },
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Eq;

impl FunctionInfo for Eq {
    fn name(&self) -> &'static str {
        "="
    }

    fn signatures(&self) -> &[Signature] {
        COMPARISON_SIGNATURES
    }
}

impl ScalarFunction for Eq {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFuntion> {
        Ok(PlannedScalarFuntion {
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
        COMPARISON_SIGNATURES
    }
}

impl ScalarFunction for Neq {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFuntion> {
        Ok(PlannedScalarFuntion {
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
        COMPARISON_SIGNATURES
    }
}

impl ScalarFunction for Lt {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFuntion> {
        Ok(PlannedScalarFuntion {
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
        COMPARISON_SIGNATURES
    }
}

impl ScalarFunction for LtEq {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFuntion> {
        Ok(PlannedScalarFuntion {
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
        COMPARISON_SIGNATURES
    }
}

impl ScalarFunction for Gt {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFuntion> {
        Ok(PlannedScalarFuntion {
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
        COMPARISON_SIGNATURES
    }
}

impl ScalarFunction for GtEq {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFuntion> {
        Ok(PlannedScalarFuntion {
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
            (a, b) => return Err(invalid_input_types_error(func, &[a, b])),
        },
    )
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

                return BinaryExecutor::execute::<T::Storage, T::Storage, _, _>(
                    left,
                    &scaled_right,
                    builder,
                    |a, b, buf| buf.put(&O::compare(a, b)),
                );
            }
            Ordering::Less => {
                let scaled_left = decimal_rescale::<T::Storage, T>(
                    left,
                    right.datatype().clone(),
                    CastFailBehavior::Error,
                )?;

                return BinaryExecutor::execute::<T::Storage, T::Storage, _, _>(
                    &scaled_left,
                    right,
                    builder,
                    |a, b, buf| buf.put(&O::compare(a, b)),
                );
            }
            Ordering::Equal => {
                return BinaryExecutor::execute::<T::Storage, T::Storage, _, _>(
                    left,
                    right,
                    builder,
                    |a, b, buf| buf.put(&O::compare(a, b)),
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {

    // use super::*;

    // TODO
    // #[test]
    // fn eq_i32() {
    //     let a = Array::from_iter([1, 2, 3]);
    //     let b = Array::from_iter([2, 2, 6]);

    //     let specialized = Eq
    //         .plan_from_datatypes(&[DataType::Int32, DataType::Int32])
    //         .unwrap();

    //     let out = specialized.execute(&[&a, &b]).unwrap();
    //     let expected = Array::from_iter([false, true, false]);

    //     assert_eq!(expected, out);
    // }

    // #[test]
    // fn neq_i32() {
    //     let a = Array::from_iter([1, 2, 3]);
    //     let b = Array::from_iter([2, 2, 6]);

    //     let specialized = Neq
    //         .plan_from_datatypes(&[DataType::Int32, DataType::Int32])
    //         .unwrap();

    //     let out = specialized.execute(&[&a, &b]).unwrap();
    //     let expected = Array::from_iter([true, false, true]);

    //     assert_eq!(expected, out);
    // }

    // #[test]
    // fn lt_i32() {
    //     let a = Array::from_iter([1, 2, 3]);
    //     let b = Array::from_iter([2, 2, 6]);

    //     let specialized = Lt
    //         .plan_from_datatypes(&[DataType::Int32, DataType::Int32])
    //         .unwrap();

    //     let out = specialized.execute(&[&a, &b]).unwrap();
    //     let expected = Array::from_iter([true, false, true]);

    //     assert_eq!(expected, out);
    // }

    // #[test]
    // fn lt_eq_i32() {
    //     let a = Array::from_iter([1, 2, 3]);
    //     let b = Array::from_iter([2, 2, 6]);

    //     let specialized = LtEq
    //         .plan_from_datatypes(&[DataType::Int32, DataType::Int32])
    //         .unwrap();

    //     let out = specialized.execute(&[&a, &b]).unwrap();
    //     let expected = Array::from_iter([true, true, true]);

    //     assert_eq!(expected, out);
    // }

    // #[test]
    // fn gt_i32() {
    //     let a = Array::from_iter([1, 2, 3]);
    //     let b = Array::from_iter([2, 2, 6]);

    //     let specialized = Gt
    //         .plan_from_datatypes(&[DataType::Int32, DataType::Int32])
    //         .unwrap();

    //     let out = specialized.execute(&[&a, &b]).unwrap();
    //     let expected = Array::from_iter([false, false, false]);

    //     assert_eq!(expected, out);
    // }

    // #[test]
    // fn gt_eq_i32() {
    //     let a = Array::from_iter([1, 2, 3]);
    //     let b = Array::from_iter([2, 2, 6]);

    //     let specialized = GtEq
    //         .plan_from_datatypes(&[DataType::Int32, DataType::Int32])
    //         .unwrap();

    //     let out = specialized.execute(&[&a, &b]).unwrap();
    //     let expected = Array::from_iter([false, true, false]);

    //     assert_eq!(expected, out);
    // }
}
