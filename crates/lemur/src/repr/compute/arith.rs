use super::{binary_op_fixedlen, value_vec_dispatch_binary, NumericType};
use crate::repr::value::ValueVec;
use crate::repr::vec::{
    BinaryVec, FixedLengthType, FixedLengthVec, Utf8Vec,
};
use anyhow::{anyhow, Result};
use std::ops::{Add, Div, Mul, Sub};

/// Arithmetic operations on vectors.
pub trait VecArith<Rhs = Self> {
    type Output;

    fn add(&self, _rhs: &Rhs) -> Result<Self::Output> {
        Err(anyhow!("add unimplemented"))
    }

    fn sub(&self, _rhs: &Rhs) -> Result<Self::Output> {
        Err(anyhow!("sub unimplemented"))
    }

    fn mul(&self, _rhs: &Rhs) -> Result<Self::Output> {
        Err(anyhow!("mul unimplemented"))
    }

    fn div(&self, _rhs: &Rhs) -> Result<Self::Output> {
        Err(anyhow!("div unimplemented"))
    }
}

impl<T: NumericType + FixedLengthType> VecArith for FixedLengthVec<T> {
    type Output = Self;

    fn add(&self, rhs: &Self) -> Result<Self::Output> {
        Ok(binary_op_fixedlen(self, rhs, Add::add))
    }

    fn sub(&self, rhs: &Self) -> Result<Self::Output> {
        Ok(binary_op_fixedlen(self, rhs, Sub::sub))
    }

    fn mul(&self, rhs: &Self) -> Result<Self::Output> {
        Ok(binary_op_fixedlen(self, rhs, Mul::mul))
    }

    fn div(&self, rhs: &Self) -> Result<Self::Output> {
        Ok(binary_op_fixedlen(self, rhs, Div::div))
    }
}

impl VecArith for FixedLengthVec<bool> {
    type Output = Self;
}
impl VecArith for Utf8Vec {
    type Output = Self;
}
impl VecArith for BinaryVec {
    type Output = Self;
}

impl VecArith for ValueVec {
    type Output = Self;

    fn add(&self, rhs: &Self) -> Result<Self::Output> {
        Ok(value_vec_dispatch_binary!(self, rhs, VecArith::add))
    }

    fn sub(&self, rhs: &Self) -> Result<Self::Output> {
        Ok(value_vec_dispatch_binary!(self, rhs, VecArith::sub))
    }

    fn mul(&self, rhs: &Self) -> Result<Self::Output> {
        Ok(value_vec_dispatch_binary!(self, rhs, VecArith::mul))
    }

    fn div(&self, rhs: &Self) -> Result<Self::Output> {
        Ok(value_vec_dispatch_binary!(self, rhs, VecArith::div))
    }
}
