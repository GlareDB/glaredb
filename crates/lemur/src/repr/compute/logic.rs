use super::{
    binary_op_fixedlen, unary_op_fixedlen, value_vec_dispatch_binary, value_vec_dispatch_unary,
};
use crate::repr::ordfloat::{OrdF32, OrdF64};
use crate::repr::value::ValueVec;
use crate::repr::vec::{BinaryVec, BoolVec, FixedLengthVec, Utf8Vec};
use anyhow::{anyhow, Result};

pub trait VecBinaryLogic<Rhs = Self> {
    fn and(&self, _rhs: &Rhs) -> Result<BoolVec> {
        Err(anyhow!("and unimplemented"))
    }

    fn or(&self, _rhs: &Rhs) -> Result<BoolVec> {
        Err(anyhow!("not unimplemented"))
    }
}

impl VecBinaryLogic for BoolVec {
    fn and(&self, rhs: &Self) -> Result<BoolVec> {
        Ok(binary_op_fixedlen(self, rhs, |a, b| a && b))
    }

    fn or(&self, rhs: &Self) -> Result<BoolVec> {
        Ok(binary_op_fixedlen(self, rhs, |a, b| a || b))
    }
}

impl VecBinaryLogic for FixedLengthVec<i8> {}
impl VecBinaryLogic for FixedLengthVec<i16> {}
impl VecBinaryLogic for FixedLengthVec<i32> {}
impl VecBinaryLogic for FixedLengthVec<i64> {}
impl VecBinaryLogic for FixedLengthVec<OrdF32> {}
impl VecBinaryLogic for FixedLengthVec<OrdF64> {}
impl VecBinaryLogic for Utf8Vec {}
impl VecBinaryLogic for BinaryVec {}

impl VecBinaryLogic for ValueVec {
    fn and(&self, rhs: &Self) -> Result<BoolVec> {
        Ok(value_vec_dispatch_binary!(self, rhs, VecBinaryLogic::and))
    }

    fn or(&self, rhs: &Self) -> Result<BoolVec> {
        Ok(value_vec_dispatch_binary!(self, rhs, VecBinaryLogic::or))
    }
}

pub trait VecUnaryLogic {
    fn not(&self) -> Result<BoolVec> {
        Err(anyhow!("not unimplemented"))
    }
}

impl VecUnaryLogic for BoolVec {
    fn not(&self) -> Result<BoolVec> {
        Ok(unary_op_fixedlen(self, |a| !a))
    }
}

impl VecUnaryLogic for FixedLengthVec<i8> {}
impl VecUnaryLogic for FixedLengthVec<i16> {}
impl VecUnaryLogic for FixedLengthVec<i32> {}
impl VecUnaryLogic for FixedLengthVec<i64> {}
impl VecUnaryLogic for FixedLengthVec<OrdF32> {}
impl VecUnaryLogic for FixedLengthVec<OrdF64> {}
impl VecUnaryLogic for Utf8Vec {}
impl VecUnaryLogic for BinaryVec {}

impl VecUnaryLogic for ValueVec {
    fn not(&self) -> Result<BoolVec> {
        Ok(value_vec_dispatch_unary!(self, VecUnaryLogic::not))
    }
}
