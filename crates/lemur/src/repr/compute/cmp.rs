use super::{binary_op_fixedlen, binary_op_utf8_to_fixed, value_vec_dispatch_binary};
use crate::repr::value::ValueVec;
use crate::repr::vec::{BinaryVec, BoolVec, FixedLengthType, FixedLengthVec, Utf8Vec};
use anyhow::{anyhow, Result};

pub trait VecCmp<Rhs = Self> {
    fn eq(&self, _rhs: &Rhs) -> Result<BoolVec> {
        Err(anyhow!("eq unimplemented"))
    }

    fn neq(&self, _rhs: &Rhs) -> Result<BoolVec> {
        Err(anyhow!("neq unimplemented"))
    }

    fn gt(&self, _rhs: &Rhs) -> Result<BoolVec> {
        Err(anyhow!("gt unimplemented"))
    }

    fn lt(&self, _rhs: &Rhs) -> Result<BoolVec> {
        Err(anyhow!("lt unimplemented"))
    }

    fn ge(&self, _rhs: &Rhs) -> Result<BoolVec> {
        Err(anyhow!("ge unimplemented"))
    }

    fn le(&self, _rhs: &Rhs) -> Result<BoolVec> {
        Err(anyhow!("le unimplemented"))
    }
}

impl<T: FixedLengthType> VecCmp for FixedLengthVec<T> {
    fn eq(&self, rhs: &Self) -> Result<BoolVec> {
        Ok(binary_op_fixedlen(self, rhs, |a, b| a == b))
    }

    fn neq(&self, rhs: &Self) -> Result<BoolVec> {
        Ok(binary_op_fixedlen(self, rhs, |a, b| a != b))
    }

    fn gt(&self, rhs: &Self) -> Result<BoolVec> {
        Ok(binary_op_fixedlen(self, rhs, |a, b| a > b))
    }

    fn lt(&self, rhs: &Self) -> Result<BoolVec> {
        Ok(binary_op_fixedlen(self, rhs, |a, b| a < b))
    }

    fn ge(&self, rhs: &Self) -> Result<BoolVec> {
        Ok(binary_op_fixedlen(self, rhs, |a, b| a >= b))
    }

    fn le(&self, rhs: &Self) -> Result<BoolVec> {
        Ok(binary_op_fixedlen(self, rhs, |a, b| a <= b))
    }
}

impl VecCmp for Utf8Vec {
    fn eq(&self, rhs: &Self) -> Result<BoolVec> {
        Ok(binary_op_utf8_to_fixed(self, rhs, |a, b| a == b))
    }

    fn neq(&self, rhs: &Self) -> Result<BoolVec> {
        Ok(binary_op_utf8_to_fixed(self, rhs, |a, b| a != b))
    }

    fn gt(&self, rhs: &Self) -> Result<BoolVec> {
        Ok(binary_op_utf8_to_fixed(self, rhs, |a, b| a > b))
    }

    fn lt(&self, rhs: &Self) -> Result<BoolVec> {
        Ok(binary_op_utf8_to_fixed(self, rhs, |a, b| a < b))
    }

    fn ge(&self, rhs: &Self) -> Result<BoolVec> {
        Ok(binary_op_utf8_to_fixed(self, rhs, |a, b| a >= b))
    }

    fn le(&self, rhs: &Self) -> Result<BoolVec> {
        Ok(binary_op_utf8_to_fixed(self, rhs, |a, b| a <= b))
    }
}

impl VecCmp for BinaryVec {}

impl VecCmp for ValueVec {
    fn eq(&self, rhs: &Self) -> Result<BoolVec> {
        Ok(value_vec_dispatch_binary!(self, rhs, VecCmp::eq))
    }

    fn neq(&self, rhs: &Self) -> Result<BoolVec> {
        Ok(value_vec_dispatch_binary!(self, rhs, VecCmp::neq))
    }

    fn gt(&self, rhs: &Self) -> Result<BoolVec> {
        Ok(value_vec_dispatch_binary!(self, rhs, VecCmp::gt))
    }

    fn lt(&self, rhs: &Self) -> Result<BoolVec> {
        Ok(value_vec_dispatch_binary!(self, rhs, VecCmp::lt))
    }

    fn ge(&self, rhs: &Self) -> Result<BoolVec> {
        Ok(value_vec_dispatch_binary!(self, rhs, VecCmp::ge))
    }

    fn le(&self, rhs: &Self) -> Result<BoolVec> {
        Ok(value_vec_dispatch_binary!(self, rhs, VecCmp::le))
    }
}
