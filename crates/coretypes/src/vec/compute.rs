use super::*;
use anyhow::{anyhow, Result};
use std::ops::{Add, Div, Mul, Sub};

// TODO: Handle invalid (null) values.

pub trait NumericType:
    Sized + Add<Output = Self> + Sub<Output = Self> + Mul<Output = Self> + Div<Output = Self>
{
    const ZERO: Self;
    const ONE: Self;
}

impl NumericType for i8 {
    const ZERO: Self = 0;
    const ONE: Self = 1;
}
impl NumericType for i16 {
    const ZERO: Self = 0;
    const ONE: Self = 1;
}
impl NumericType for i32 {
    const ZERO: Self = 0;
    const ONE: Self = 1;
}
impl NumericType for i64 {
    const ZERO: Self = 0;
    const ONE: Self = 1;
}
impl NumericType for f32 {
    const ZERO: Self = 0.0;
    const ONE: Self = 1.0;
}
impl NumericType for f64 {
    const ZERO: Self = 0.0;
    const ONE: Self = 1.0;
}

pub trait VecAdd<Rhs = Self, Output = Self> {
    fn add(&self, rhs: &Rhs) -> Result<Output>;
}

pub trait VecSub<Rhs = Self, Output = Self> {
    fn sub(&self, rhs: &Rhs) -> Result<Output>;
}

pub trait VecMul<Rhs = Self, Output = Self> {
    fn mul(&self, rhs: &Rhs) -> Result<Output>;
}

pub trait VecDiv<Rhs = Self, Output = Self> {
    fn div(&self, rhs: &Rhs) -> Result<Output>;
}

/// Implements both a vector/vector op, and a vector/scalar op.
macro_rules! impl_numeric_op {
    ($trait:ident, $fn:ident, $op:ident) => {
        impl<T: NumericType + FixedLengthType> $trait for FixedLengthVec<T> {
            fn $fn(&self, rhs: &Self) -> Result<Self> {
                let values = self
                    .iter_values()
                    .zip(rhs.iter_values())
                    .map(|(a, b)| (*a).$op(*b));
                Ok(Self::from_iter_all_valid(values))
            }
        }

        impl<T: NumericType + FixedLengthType> $trait<T> for FixedLengthVec<T> {
            fn $fn(&self, rhs: &T) -> Result<Self> {
                let values = self.iter_values().map(|a| (*a).$op(*rhs));
                Ok(Self::from_iter_all_valid(values))
            }
        }
    };
}

impl_numeric_op!(VecAdd, add, add);
impl_numeric_op!(VecSub, sub, sub);
impl_numeric_op!(VecMul, mul, mul);
impl_numeric_op!(VecDiv, div, div);

/// Aggregates over numeric types.
// TODO: Technically some of these should also be implemented for strings.
pub trait VecNumericAgg<T> {
    fn sum(&self) -> Result<T>;
    fn min(&self) -> Result<T>;
    fn max(&self) -> Result<T>;
}

impl<T: NumericType + FixedLengthType> VecNumericAgg<T> for FixedLengthVec<T> {
    fn sum(&self) -> Result<T> {
        Ok(self.iter_values().fold(T::ZERO, |acc, &v| acc + v))
    }

    fn min(&self) -> Result<T> {
        // TODO: Figure out what to do for floats. Probably just make a wrapper
        // type and reimplement `Ord` with the `total_cmp` method.
        todo!()
    }

    fn max(&self) -> Result<T> {
        todo!()
    }
}

pub trait VecEq<Rhs = Self> {
    fn eq(&self, rhs: &Rhs) -> Result<BoolVec>;
}

pub trait VecNeq<Rhs = Self> {
    fn neq(&self, rhs: &Rhs) -> Result<BoolVec>;
}

pub trait VecGt<Rhs = Self> {
    fn gt(&self, rhs: &Rhs) -> Result<BoolVec>;
}

pub trait VecLt<Rhs = Self> {
    fn lt(&self, rhs: &Rhs) -> Result<BoolVec>;
}

pub trait VecGe<Rhs = Self> {
    fn ge(&self, rhs: &Rhs) -> Result<BoolVec>;
}

pub trait VecLe<Rhs = Self> {
    fn le(&self, rhs: &Rhs) -> Result<BoolVec>;
}

macro_rules! impl_cmp_op_fixed {
    ($trait:ident, $fn:ident, $op:ident) => {
        impl<T: FixedLengthType> $trait for FixedLengthVec<T> {
            fn $fn(&self, rhs: &Self) -> Result<BoolVec> {
                let values = self
                    .iter_values()
                    .zip(rhs.iter_values())
                    .map(|(a, b)| a.$op(b));
                Ok(BoolVec::from_iter_all_valid(values))
            }
        }

        impl<T: FixedLengthType> $trait<T> for FixedLengthVec<T> {
            fn $fn(&self, rhs: &T) -> Result<BoolVec> {
                let values = self.iter_values().map(|a| a.$op(rhs));
                Ok(BoolVec::from_iter_all_valid(values))
            }
        }
    };
}

impl_cmp_op_fixed!(VecEq, eq, eq);
impl_cmp_op_fixed!(VecNeq, neq, ne);
impl_cmp_op_fixed!(VecGt, gt, gt);
impl_cmp_op_fixed!(VecLt, lt, lt);
impl_cmp_op_fixed!(VecGe, ge, ge);
impl_cmp_op_fixed!(VecLe, le, le);

macro_rules! impl_cmp_op_varlen {
    ($trait:ident, $fn:ident, $op:ident, $vec_type:ident) => {
        impl $trait for $vec_type {
            fn $fn(&self, rhs: &Self) -> Result<BoolVec> {
                let values = self
                    .iter_values()
                    .zip(rhs.iter_values())
                    .map(|(a, b)| a.$op(b));
                Ok(BoolVec::from_iter_all_valid(values))
            }
        }
    };
}

macro_rules! impl_cmp_op_varlen_scalar {
    ($trait:ident, $fn:ident, $op:ident, $vec_type:ident, $scalar:ty) => {
        impl<T: AsRef<$scalar>> $trait<T> for $vec_type {
            fn $fn(&self, rhs: &T) -> Result<BoolVec> {
                let b = rhs.as_ref();
                let values = self.iter_values().map(|a| a.$op(b));
                Ok(BoolVec::from_iter_all_valid(values))
            }
        }
    };
}

impl_cmp_op_varlen!(VecEq, eq, eq, Utf8Vec);
impl_cmp_op_varlen!(VecNeq, neq, ne, Utf8Vec);
impl_cmp_op_varlen!(VecGt, gt, gt, Utf8Vec);
impl_cmp_op_varlen!(VecLt, lt, lt, Utf8Vec);
impl_cmp_op_varlen!(VecGe, ge, ge, Utf8Vec);
impl_cmp_op_varlen!(VecLe, le, le, Utf8Vec);

impl_cmp_op_varlen_scalar!(VecEq, eq, eq, Utf8Vec, str);
impl_cmp_op_varlen_scalar!(VecNeq, neq, ne, Utf8Vec, str);
impl_cmp_op_varlen_scalar!(VecGt, gt, gt, Utf8Vec, str);
impl_cmp_op_varlen_scalar!(VecLt, lt, lt, Utf8Vec, str);
impl_cmp_op_varlen_scalar!(VecGe, ge, ge, Utf8Vec, str);
impl_cmp_op_varlen_scalar!(VecLe, le, le, Utf8Vec, str);

impl_cmp_op_varlen!(VecEq, eq, eq, BinaryVec);
impl_cmp_op_varlen!(VecNeq, neq, ne, BinaryVec);
impl_cmp_op_varlen!(VecGt, gt, gt, BinaryVec);
impl_cmp_op_varlen!(VecLt, lt, lt, BinaryVec);
impl_cmp_op_varlen!(VecGe, ge, ge, BinaryVec);
impl_cmp_op_varlen!(VecLe, le, le, BinaryVec);

impl_cmp_op_varlen_scalar!(VecEq, eq, eq, BinaryVec, [u8]);
impl_cmp_op_varlen_scalar!(VecNeq, neq, ne, BinaryVec, [u8]);
impl_cmp_op_varlen_scalar!(VecGt, gt, gt, BinaryVec, [u8]);
impl_cmp_op_varlen_scalar!(VecLt, lt, lt, BinaryVec, [u8]);
impl_cmp_op_varlen_scalar!(VecGe, ge, ge, BinaryVec, [u8]);
impl_cmp_op_varlen_scalar!(VecLe, le, le, BinaryVec, [u8]);

pub trait VecLogic<Rhs = Self> {
    fn and(&self, rhs: &Rhs) -> Result<BoolVec>;
    fn or(&self, rhs: &Rhs) -> Result<BoolVec>;
}

impl VecLogic for BoolVec {
    fn and(&self, rhs: &Self) -> Result<BoolVec> {
        let values = self
            .iter_values()
            .zip(rhs.iter_values())
            .map(|(a, b)| *a && *b);
        Ok(BoolVec::from_iter_all_valid(values))
    }

    fn or(&self, rhs: &Self) -> Result<BoolVec> {
        let values = self
            .iter_values()
            .zip(rhs.iter_values())
            .map(|(a, b)| *a || *b);
        Ok(BoolVec::from_iter_all_valid(values))
    }
}

impl VecLogic<bool> for BoolVec {
    fn and(&self, rhs: &bool) -> Result<BoolVec> {
        let values = self.iter_values().map(|a| *a && *rhs);
        Ok(BoolVec::from_iter_all_valid(values))
    }

    fn or(&self, rhs: &bool) -> Result<BoolVec> {
        let values = self.iter_values().map(|a| *a || *rhs);
        Ok(BoolVec::from_iter_all_valid(values))
    }
}
