//! Helpers for executing compute operations on columns.
use super::Column;
use crate::arrow::datatype::GetArrowDataType;
use crate::arrow::scalar::ScalarOwned;
use crate::errors::{internal, Result};
use arrow2::array::{BooleanArray, PrimitiveArray};
use arrow2::compute::arithmetics::basic;
use arrow2::compute::comparison;
use arrow2::datatypes::DataType as ArrowDataType;
use arrow2::types::NativeType;

fn compute_primitive_binary<T, F>(left: &Column, right: &Column, op: F) -> Column
where
    T: NativeType,
    F: FnOnce(&PrimitiveArray<T>, &PrimitiveArray<T>) -> PrimitiveArray<T>,
{
    let left = left.0.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    let right = right
        .0
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap();
    op(left, right).into()
}

fn compute_primitive_binary_scalar<T, F>(left: &Column, right: &Option<T>, op: F) -> Column
where
    T: NativeType,
    F: FnOnce(&PrimitiveArray<T>, &T) -> PrimitiveArray<T>,
{
    match right {
        Some(right) => {
            let left = left.0.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
            op(left, right).into()
        }
        None => PrimitiveArray::<T>::new_null(left.get_arrow_data_type(), left.len()).into(),
    }
}

macro_rules! dispatch_primitive_binary_op {
    ($left:expr, $right:expr, $int_op:path, $float_op:path) => {{
        let result: Result<Column> =
            match ($left.get_arrow_data_type(), $right.get_arrow_data_type()) {
                (ArrowDataType::Int8, ArrowDataType::Int8) => {
                    Ok(compute_primitive_binary::<i8, _>($left, $right, $int_op))
                }
                (ArrowDataType::Int16, ArrowDataType::Int16) => {
                    Ok(compute_primitive_binary::<i16, _>($left, $right, $int_op))
                }
                (ArrowDataType::Int32, ArrowDataType::Int32) => {
                    Ok(compute_primitive_binary::<i32, _>($left, $right, $int_op))
                }
                (ArrowDataType::Int64, ArrowDataType::Int64) => {
                    Ok(compute_primitive_binary::<i64, _>($left, $right, $int_op))
                }
                (ArrowDataType::UInt8, ArrowDataType::UInt8) => {
                    Ok(compute_primitive_binary::<u8, _>($left, $right, $int_op))
                }
                (ArrowDataType::UInt16, ArrowDataType::UInt16) => {
                    Ok(compute_primitive_binary::<u16, _>($left, $right, $int_op))
                }
                (ArrowDataType::UInt32, ArrowDataType::UInt32) => {
                    Ok(compute_primitive_binary::<u32, _>($left, $right, $int_op))
                }
                (ArrowDataType::UInt64, ArrowDataType::UInt64) => {
                    Ok(compute_primitive_binary::<u64, _>($left, $right, $int_op))
                }
                (ArrowDataType::Float32, ArrowDataType::Float32) => {
                    Ok(compute_primitive_binary::<f32, _>($left, $right, $float_op))
                }
                (ArrowDataType::Float64, ArrowDataType::Float64) => {
                    Ok(compute_primitive_binary::<f64, _>($left, $right, $float_op))
                }
                (left, right) => Err(internal!(
                    "cannot compute '{}' for data types {:?} and {:?}",
                    stringify!($op),
                    left,
                    right
                )),
            };
        result
    }};
}

macro_rules! dispatch_primitive_binary_scalar_op {
    ($left:expr, $right:expr, $int_op:path, $float_op:path) => {{
        let result: Result<Column> = match ($left.get_arrow_data_type(), $right) {
            (ArrowDataType::Int8, ScalarOwned::Int8(right)) => Ok(
                compute_primitive_binary_scalar::<i8, _>($left, right, $int_op),
            ),
            (ArrowDataType::Int16, ScalarOwned::Int16(right)) => Ok(
                compute_primitive_binary_scalar::<i16, _>($left, right, $int_op),
            ),
            (ArrowDataType::Int32, ScalarOwned::Int32(right)) => Ok(
                compute_primitive_binary_scalar::<i32, _>($left, right, $int_op),
            ),
            (ArrowDataType::Int64, ScalarOwned::Int64(right)) => Ok(
                compute_primitive_binary_scalar::<i64, _>($left, right, $int_op),
            ),
            (ArrowDataType::UInt8, ScalarOwned::Uint8(right)) => Ok(
                compute_primitive_binary_scalar::<u8, _>($left, right, $int_op),
            ),
            (ArrowDataType::UInt16, ScalarOwned::Uint16(right)) => Ok(
                compute_primitive_binary_scalar::<u16, _>($left, right, $int_op),
            ),
            (ArrowDataType::UInt32, ScalarOwned::Uint32(right)) => Ok(
                compute_primitive_binary_scalar::<u32, _>($left, right, $int_op),
            ),
            (ArrowDataType::UInt64, ScalarOwned::Uint64(right)) => Ok(
                compute_primitive_binary_scalar::<u64, _>($left, right, $int_op),
            ),
            (ArrowDataType::Float32, ScalarOwned::Float32(right)) => Ok(
                compute_primitive_binary_scalar::<f32, _>($left, right, $float_op),
            ),
            (ArrowDataType::Float64, ScalarOwned::Float64(right)) => Ok(
                compute_primitive_binary_scalar::<f64, _>($left, right, $float_op),
            ),

            (left, right) => Err(internal!(
                "cannot compute '{}' for data types {:?} and {:?}",
                stringify!($op),
                left,
                right
            )),
        };
        result
    }};
}

pub fn add(left: &Column, right: &Column) -> Result<Column> {
    dispatch_primitive_binary_op!(left, right, basic::checked_add, basic::add)
}

pub fn add_scalar(left: &Column, right: &ScalarOwned) -> Result<Column> {
    dispatch_primitive_binary_scalar_op!(left, right, basic::checked_add_scalar, basic::add_scalar)
}

pub fn sub(left: &Column, right: &Column) -> Result<Column> {
    dispatch_primitive_binary_op!(left, right, basic::checked_sub, basic::sub)
}

pub fn sub_scalar(left: &Column, right: &ScalarOwned) -> Result<Column> {
    dispatch_primitive_binary_scalar_op!(left, right, basic::checked_sub_scalar, basic::sub_scalar)
}

pub fn mul(left: &Column, right: &Column) -> Result<Column> {
    dispatch_primitive_binary_op!(left, right, basic::checked_mul, basic::mul)
}

pub fn mul_scalar(left: &Column, right: &ScalarOwned) -> Result<Column> {
    dispatch_primitive_binary_scalar_op!(left, right, basic::checked_mul_scalar, basic::mul_scalar)
}

pub fn div(left: &Column, right: &Column) -> Result<Column> {
    dispatch_primitive_binary_op!(left, right, basic::checked_div, basic::div)
}

pub fn div_scalar(left: &Column, right: &ScalarOwned) -> Result<Column> {
    dispatch_primitive_binary_scalar_op!(left, right, basic::checked_div_scalar, basic::div_scalar)
}

fn compute_primitive_comparison<T, F>(left: &Column, right: &Column, op: F) -> Column
where
    T: NativeType + comparison::Simd8,
    T::Simd: comparison::Simd8PartialEq + comparison::Simd8PartialOrd,
    F: FnOnce(&PrimitiveArray<T>, &PrimitiveArray<T>) -> BooleanArray,
{
    let left = left.0.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    let right = right
        .0
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap();
    op(left, right).into()
}

fn compute_primitive_comparison_scalar<T, F>(left: &Column, right: &Option<T>, op: F) -> Column
where
    T: NativeType + comparison::Simd8,
    T::Simd: comparison::Simd8PartialEq + comparison::Simd8PartialOrd,
    F: FnOnce(&PrimitiveArray<T>, T) -> BooleanArray,
{
    match right {
        Some(right) => {
            let left = left.0.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
            op(left, *right).into()
        }
        None => PrimitiveArray::<T>::new_null(left.get_arrow_data_type(), left.len()).into(),
    }
}

// TODO: Non-primitives too.
macro_rules! dispatch_comparison_op {
    ($left:expr, $right:expr, $op:path) => {{
        let result: Result<Column> =
            match ($left.get_arrow_data_type(), $right.get_arrow_data_type()) {
                (ArrowDataType::Int8, ArrowDataType::Int8) => {
                    Ok(compute_primitive_comparison::<i8, _>($left, $right, $op))
                }
                (ArrowDataType::Int16, ArrowDataType::Int16) => {
                    Ok(compute_primitive_comparison::<i16, _>($left, $right, $op))
                }
                (ArrowDataType::Int32, ArrowDataType::Int32) => {
                    Ok(compute_primitive_comparison::<i32, _>($left, $right, $op))
                }
                (ArrowDataType::Int64, ArrowDataType::Int64) => {
                    Ok(compute_primitive_comparison::<i64, _>($left, $right, $op))
                }
                (ArrowDataType::UInt8, ArrowDataType::UInt8) => {
                    Ok(compute_primitive_comparison::<u8, _>($left, $right, $op))
                }
                (ArrowDataType::UInt16, ArrowDataType::UInt16) => {
                    Ok(compute_primitive_comparison::<u16, _>($left, $right, $op))
                }
                (ArrowDataType::UInt32, ArrowDataType::UInt32) => {
                    Ok(compute_primitive_comparison::<u32, _>($left, $right, $op))
                }
                (ArrowDataType::UInt64, ArrowDataType::UInt64) => {
                    Ok(compute_primitive_comparison::<u64, _>($left, $right, $op))
                }
                (ArrowDataType::Float32, ArrowDataType::Float32) => {
                    Ok(compute_primitive_comparison::<f32, _>($left, $right, $op))
                }
                (ArrowDataType::Float64, ArrowDataType::Float64) => {
                    Ok(compute_primitive_comparison::<f64, _>($left, $right, $op))
                }
                (left, right) => Err(internal!(
                    "cannot compute '{}' for data types {:?} and {:?}",
                    stringify!($op),
                    left,
                    right
                )),
            };
        result
    }};
}

macro_rules! dispatch_comparison_scalar_op {
    ($left:expr, $right:expr, $op:path) => {{
        let result: Result<Column> = match ($left.get_arrow_data_type(), $right) {
            (ArrowDataType::Int8, ScalarOwned::Int8(right)) => Ok(
                compute_primitive_comparison_scalar::<i8, _>($left, right, $op),
            ),
            (ArrowDataType::Int16, ScalarOwned::Int16(right)) => Ok(
                compute_primitive_comparison_scalar::<i16, _>($left, right, $op),
            ),
            (ArrowDataType::Int32, ScalarOwned::Int32(right)) => Ok(
                compute_primitive_comparison_scalar::<i32, _>($left, right, $op),
            ),
            (ArrowDataType::Int64, ScalarOwned::Int64(right)) => Ok(
                compute_primitive_comparison_scalar::<i64, _>($left, right, $op),
            ),
            (ArrowDataType::UInt8, ScalarOwned::Uint8(right)) => Ok(
                compute_primitive_comparison_scalar::<u8, _>($left, right, $op),
            ),
            (ArrowDataType::UInt16, ScalarOwned::Uint16(right)) => Ok(
                compute_primitive_comparison_scalar::<u16, _>($left, right, $op),
            ),
            (ArrowDataType::UInt32, ScalarOwned::Uint32(right)) => Ok(
                compute_primitive_comparison_scalar::<u32, _>($left, right, $op),
            ),
            (ArrowDataType::UInt64, ScalarOwned::Uint64(right)) => Ok(
                compute_primitive_comparison_scalar::<u64, _>($left, right, $op),
            ),
            (ArrowDataType::Float32, ScalarOwned::Float32(right)) => Ok(
                compute_primitive_comparison_scalar::<f32, _>($left, right, $op),
            ),
            (ArrowDataType::Float64, ScalarOwned::Float64(right)) => Ok(
                compute_primitive_comparison_scalar::<f64, _>($left, right, $op),
            ),

            (left, right) => Err(internal!(
                "cannot compute '{}' for data types {:?} and {:?}",
                stringify!($op),
                left,
                right
            )),
        };
        result
    }};
}

pub fn eq(left: &Column, right: &Column) -> Result<Column> {
    dispatch_comparison_op!(left, right, comparison::primitive::eq_and_validity)
}

pub fn eq_scalar(left: &Column, right: &ScalarOwned) -> Result<Column> {
    dispatch_comparison_scalar_op!(left, right, comparison::primitive::eq_scalar_and_validity)
}

pub fn neq(left: &Column, right: &Column) -> Result<Column> {
    dispatch_comparison_op!(left, right, comparison::primitive::neq_and_validity)
}

pub fn neq_scalar(left: &Column, right: &ScalarOwned) -> Result<Column> {
    dispatch_comparison_scalar_op!(left, right, comparison::primitive::neq_scalar_and_validity)
}

pub fn gt(left: &Column, right: &Column) -> Result<Column> {
    dispatch_comparison_op!(left, right, comparison::primitive::gt)
}

pub fn gt_scalar(left: &Column, right: &ScalarOwned) -> Result<Column> {
    dispatch_comparison_scalar_op!(left, right, comparison::primitive::gt_scalar)
}

pub fn lt(left: &Column, right: &Column) -> Result<Column> {
    dispatch_comparison_op!(left, right, comparison::primitive::lt)
}

pub fn lt_scalar(left: &Column, right: &ScalarOwned) -> Result<Column> {
    dispatch_comparison_scalar_op!(left, right, comparison::primitive::lt_scalar)
}

pub fn gt_eq(left: &Column, right: &Column) -> Result<Column> {
    dispatch_comparison_op!(left, right, comparison::primitive::gt_eq)
}

pub fn gt_eq_scalar(left: &Column, right: &ScalarOwned) -> Result<Column> {
    dispatch_comparison_scalar_op!(left, right, comparison::primitive::gt_eq_scalar)
}

pub fn lt_eq(left: &Column, right: &Column) -> Result<Column> {
    dispatch_comparison_op!(left, right, comparison::primitive::lt_eq)
}

pub fn lt_eq_scalar(left: &Column, right: &ScalarOwned) -> Result<Column> {
    dispatch_comparison_scalar_op!(left, right, comparison::primitive::lt_eq_scalar)
}
