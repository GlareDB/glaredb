//! General compute operations on vectors.
use crate::repr::ordfloat::{OrdF32, OrdF64};

use crate::repr::vec::{
    FixedLengthType, FixedLengthVec, NativeType, Utf8Vec,
};

use bitvec::vec::BitVec;
use std::ops::{Add, Div, Mul, Sub};

pub mod arith;
pub use arith::*;
pub mod cmp;
pub use cmp::*;
pub mod logic;
pub use logic::*;
pub mod aggregate;
pub use aggregate::*;

pub trait NumericType:
    NativeType
    + Sized
    + Add<Output = Self>
    + Sub<Output = Self>
    + Mul<Output = Self>
    + Div<Output = Self>
{
}

impl NumericType for i8 {}
impl NumericType for i16 {}
impl NumericType for i32 {}
impl NumericType for i64 {}
impl NumericType for OrdF32 {}
impl NumericType for OrdF64 {}

fn binary_op_fixedlen<T1, T2, O, F>(
    left: &FixedLengthVec<T1>,
    right: &FixedLengthVec<T2>,
    f: F,
) -> FixedLengthVec<O>
where
    T1: FixedLengthType,
    T2: FixedLengthType,
    O: FixedLengthType,
    F: Fn(T1, T2) -> O,
{
    let values: Vec<_> = left
        .iter_values()
        .zip(right.iter_values())
        .map(|(a, b)| f(*a, *b))
        .collect();
    let mut validity = left.get_validity().clone();
    intersect_bitvec_with(&mut validity, right.get_validity());

    let mut vec = FixedLengthVec::from_parts(validity, values);
    extend_fixedlen_to_max(&mut vec, left.len(), right.len());

    vec
}

fn unary_op_fixedlen<T, O, F>(vec: &FixedLengthVec<T>, f: F) -> FixedLengthVec<O>
where
    T: FixedLengthType,
    O: FixedLengthType,
    F: Fn(T) -> O,
{
    let values: Vec<_> = vec.iter_values().map(|a| f(*a)).collect();
    let validity = vec.get_validity().clone();
    FixedLengthVec::from_parts(validity, values)
}

fn binary_op_utf8_to_fixed<O, F>(left: &Utf8Vec, right: &Utf8Vec, f: F) -> FixedLengthVec<O>
where
    O: FixedLengthType,
    F: Fn(&str, &str) -> O,
{
    let values: Vec<_> = left
        .iter_values()
        .zip(right.iter_values())
        .map(|(a, b)| f(a, b))
        .collect();
    let mut validity = left.get_validity().clone();
    intersect_bitvec_with(&mut validity, right.get_validity());

    let mut vec = FixedLengthVec::from_parts(validity, values);
    extend_fixedlen_to_max(&mut vec, left.len(), right.len());

    vec
}

/// Create match body for calling a function against two value vectors.
macro_rules! value_vec_dispatch_binary {
    ($left:ident, $right:ident, $func:path) => {
        match ($left, $right) {
            (ValueVec::Bool(left), ValueVec::Bool(right)) => $func(left, right)?.into(),
            (ValueVec::Int8(left), ValueVec::Int8(right)) => $func(left, right)?.into(),
            (ValueVec::Int32(left), ValueVec::Int32(right)) => $func(left, right)?.into(),
            (ValueVec::Utf8(left), ValueVec::Utf8(right)) => $func(left, right)?.into(),
            (left, right) => {
                return Err(anyhow!(
                    "value vector types mismatch, left: {:?}, right: {:?}",
                    left.value_type(),
                    right.value_type()
                ))
            }
        }
    };
}
use value_vec_dispatch_binary;

/// Create a match body for matching against a single vector type.
macro_rules! value_vec_dispatch_unary {
    ($vec:ident, $func:path) => {
        match $vec {
            ValueVec::Bool(v) => $func(v)?.into(),
            ValueVec::Int8(v) => $func(v)?.into(),
            ValueVec::Int32(v) => $func(v)?.into(),
            ValueVec::Utf8(v) => $func(v)?.into(),
            ValueVec::Binary(v) => $func(v)?.into(),
        }
    };
}
use value_vec_dispatch_unary;

/// Like `value_vec_dispatch_unary` but with groups as well.
macro_rules! value_vec_dispatch_unary_groups {
    ($vec:ident, $groups:ident, $func:path) => {
        match $vec {
            ValueVec::Bool(v) => $func(v, $groups)?.into(),
            ValueVec::Int8(v) => $func(v, $groups)?.into(),
            ValueVec::Int32(v) => $func(v, $groups)?.into(),
            ValueVec::Utf8(v) => $func(v, $groups)?.into(),
            ValueVec::Binary(v) => $func(v, $groups)?.into(),
        }
    };
}
use value_vec_dispatch_unary_groups;

/// Extend vector `to_extend` to the max length of `a` or `b`.
///
/// The vector will be extended with nulls.
fn extend_fixedlen_to_max<T>(to_extend: &mut FixedLengthVec<T>, a: usize, b: usize)
where
    T: FixedLengthType,
{
    if a > b {
        to_extend.resize_null(a);
    } else {
        to_extend.resize_null(b);
    }
}

/// Intersect two bitvectors. If the vectors vary in size, `a` will be truncated
/// the min of both lengths.
fn intersect_bitvec_with(a: &mut BitVec, b: &BitVec) {
    if a.len() < b.len() {
        a.truncate(b.len());
    }

    for (mut a, b) in a.iter_mut().zip(b.iter()) {
        *a = *a && *b;
    }
}
