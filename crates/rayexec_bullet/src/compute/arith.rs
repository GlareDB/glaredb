use crate::array::{Array, PrimitiveArray};
use crate::scalar::ScalarValue;
use rayexec_error::{RayexecError, Result};
use std::ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Rem, RemAssign, Sub, SubAssign};

macro_rules! array_arith_dispatch {
    ($left:ident, $right:ident, $fn:expr) => {{
        match ($left, $right) {
            (Array::Float32(left), Array::Float32(right)) => $fn(left, right),
            (Array::Float64(left), Array::Float64(right)) => $fn(left, right),
            (Array::Int8(left), Array::Int8(right)) => $fn(left, right),
            (Array::Int16(left), Array::Int16(right)) => $fn(left, right),
            (Array::Int32(left), Array::Int32(right)) => $fn(left, right),
            (Array::Int64(left), Array::Int64(right)) => $fn(left, right),
            (Array::UInt8(left), Array::UInt8(right)) => $fn(left, right),
            (Array::UInt16(left), Array::UInt16(right)) => $fn(left, right),
            (Array::UInt32(left), Array::UInt32(right)) => $fn(left, right),
            (Array::UInt64(left), Array::UInt64(right)) => $fn(left, right),
            _ => Err(RayexecError::new(
                "Unsupported arithmetic operation on array",
            )),
        }
    }};
}

pub fn add(left: &mut Array, right: &Array) -> Result<()> {
    array_arith_dispatch!(left, right, add_primitive)
}

pub fn sub(left: &mut Array, right: &Array) -> Result<()> {
    array_arith_dispatch!(left, right, sub_primitive)
}

pub fn mul(left: &mut Array, right: &Array) -> Result<()> {
    array_arith_dispatch!(left, right, mul_primitive)
}

pub fn div(left: &mut Array, right: &Array) -> Result<()> {
    array_arith_dispatch!(left, right, div_primitive)
}

pub fn rem(left: &mut Array, right: &Array) -> Result<()> {
    array_arith_dispatch!(left, right, rem_primitive)
}

macro_rules! scalar_arith_dispatch {
    ($left:ident, $right:ident, $fn:expr) => {{
        match ($left, $right) {
            (ScalarValue::Float32(left), ScalarValue::Float32(right)) => $fn(left, right),
            (ScalarValue::Float64(left), ScalarValue::Float64(right)) => $fn(left, right),
            (ScalarValue::Int8(left), ScalarValue::Int8(right)) => $fn(left, right),
            (ScalarValue::Int16(left), ScalarValue::Int16(right)) => $fn(left, right),
            (ScalarValue::Int32(left), ScalarValue::Int32(right)) => $fn(left, right),
            (ScalarValue::Int64(left), ScalarValue::Int64(right)) => $fn(left, right),
            (ScalarValue::UInt8(left), ScalarValue::UInt8(right)) => $fn(left, right),
            (ScalarValue::UInt16(left), ScalarValue::UInt16(right)) => $fn(left, right),
            (ScalarValue::UInt32(left), ScalarValue::UInt32(right)) => $fn(left, right),
            (ScalarValue::UInt64(left), ScalarValue::UInt64(right)) => $fn(left, right),
            _ => Err(RayexecError::new(
                "Unsupported arithmetic operation on scalar",
            )),
        }
    }};
}

pub fn add_scalar(left: &mut ScalarValue, right: ScalarValue) -> Result<()> {
    scalar_arith_dispatch!(left, right, |l, r| {
        AddAssign::add_assign(l, r);
        Ok(())
    })
}

pub fn sub_scalar(left: &mut ScalarValue, right: ScalarValue) -> Result<()> {
    scalar_arith_dispatch!(left, right, |l, r| {
        SubAssign::sub_assign(l, r);
        Ok(())
    })
}

pub fn mul_scalar(left: &mut ScalarValue, right: ScalarValue) -> Result<()> {
    scalar_arith_dispatch!(left, right, |l, r| {
        MulAssign::mul_assign(l, r);
        Ok(())
    })
}

pub fn div_scalar(left: &mut ScalarValue, right: ScalarValue) -> Result<()> {
    scalar_arith_dispatch!(left, right, |l, r| {
        DivAssign::div_assign(l, r);
        Ok(())
    })
}

pub fn mod_scalar(left: &mut ScalarValue, right: ScalarValue) -> Result<()> {
    scalar_arith_dispatch!(left, right, |l, r| {
        RemAssign::rem_assign(l, r);
        Ok(())
    })
}

pub fn add_primitive<T: Add<Output = T> + Copy>(
    left: &mut PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<()> {
    primitive_bin_op_assign(left, right, Add::add)
}

pub fn sub_primitive<T: Sub<Output = T> + Copy>(
    left: &mut PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<()> {
    primitive_bin_op_assign(left, right, Sub::sub)
}

pub fn mul_primitive<T: Mul<Output = T> + Copy>(
    left: &mut PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<()> {
    primitive_bin_op_assign(left, right, Mul::mul)
}

pub fn div_primitive<T: Div<Output = T> + Copy>(
    left: &mut PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<()> {
    primitive_bin_op_assign(left, right, Div::div)
}

pub fn rem_primitive<T: Rem<Output = T> + Copy>(
    left: &mut PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<()> {
    primitive_bin_op_assign(left, right, Rem::rem)
}

/// Execute a binary function on left and right, assigning the result to left.
fn primitive_bin_op_assign<T, F>(
    left: &mut PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    f: F,
) -> Result<()>
where
    T: Copy,
    F: Fn(T, T) -> T,
{
    if left.len() != right.len() {
        return Err(RayexecError::new(
            "Left and right arrays have different lengths",
        ));
    }

    let left = left.values_mut().try_as_mut()?.iter_mut();
    let right = right.values().as_ref().iter();

    for (left, right) in left.zip(right) {
        *left = f(*left, *right);
    }

    // TODO: Nulls

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::array::{Float32Array, Int32Array};

    use super::*;

    #[test]
    fn primitive_add() {
        let mut left = Int32Array::from_iter([1, 2, 3]);
        let right = Int32Array::from_iter([4, 5, 6]);

        add_primitive(&mut left, &right).unwrap();
        assert_eq!(Int32Array::from_iter([5, 7, 9]), left);
    }

    #[test]
    fn primitive_sub() {
        let mut left = Int32Array::from_iter([1, 2, 3]);
        let right = Int32Array::from_iter([4, 5, 6]);

        sub_primitive(&mut left, &right).unwrap();
        assert_eq!(Int32Array::from_iter([-3, -3, -3]), left);
    }

    #[test]
    fn primitive_mul() {
        let mut left = Int32Array::from_iter([1, 2, 3]);
        let right = Int32Array::from_iter([4, 5, 6]);

        mul_primitive(&mut left, &right).unwrap();
        assert_eq!(Int32Array::from_iter([4, 10, 18]), left);
    }

    #[test]
    fn primitive_div_float() {
        let mut left = Float32Array::from_iter([4.0, 5.0, 6.0]);
        let right = Float32Array::from_iter([2.0, 2.0, 3.0]);

        div_primitive(&mut left, &right).unwrap();
        assert_eq!(Float32Array::from_iter([2.0, 2.5, 2.0]), left);
    }

    #[test]
    fn primitive_rem() {
        let mut left = Int32Array::from_iter([4, 5, 6]);
        let right = Int32Array::from_iter([1, 2, 3]);

        rem_primitive(&mut left, &right).unwrap();
        assert_eq!(Int32Array::from_iter([0, 1, 0]), left);
    }
}
