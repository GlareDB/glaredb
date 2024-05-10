use crate::{
    array::{Array, BooleanArray, OffsetIndex, PrimitiveArray, VarlenArray, VarlenType},
    bitmap::Bitmap,
    scalar::ScalarValue,
};
use rayexec_error::{RayexecError, Result};

macro_rules! array_cmp_dispatch {
    ($left:ident, $right:ident, $prim_fn:expr, $varlen_fn:expr) => {{
        match ($left, $right) {
            (Array::Float32(left), Array::Float32(right)) => $prim_fn(left, right),
            (Array::Float64(left), Array::Float64(right)) => $prim_fn(left, right),
            (Array::Int8(left), Array::Int8(right)) => $prim_fn(left, right),
            (Array::Int16(left), Array::Int16(right)) => $prim_fn(left, right),
            (Array::Int32(left), Array::Int32(right)) => $prim_fn(left, right),
            (Array::Int64(left), Array::Int64(right)) => $prim_fn(left, right),
            (Array::UInt8(left), Array::UInt8(right)) => $prim_fn(left, right),
            (Array::UInt16(left), Array::UInt16(right)) => $prim_fn(left, right),
            (Array::UInt32(left), Array::UInt32(right)) => $prim_fn(left, right),
            (Array::UInt64(left), Array::UInt64(right)) => $prim_fn(left, right),
            (Array::Utf8(left), Array::Utf8(right)) => $varlen_fn(left, right),
            (Array::Binary(left), Array::Binary(right)) => $varlen_fn(left, right),
            (Array::LargeUtf8(left), Array::LargeUtf8(right)) => $varlen_fn(left, right),
            (Array::LargeBinary(left), Array::LargeBinary(right)) => $varlen_fn(left, right),
            _ => Err(RayexecError::new(
                "Unsupported comparison operation on array",
            )),
        }
    }};
}

pub fn eq(left: &Array, right: &Array) -> Result<BooleanArray> {
    array_cmp_dispatch!(left, right, eq_primtive, eq_varlen)
}

pub fn neq(left: &Array, right: &Array) -> Result<BooleanArray> {
    array_cmp_dispatch!(left, right, neq_primtive, neq_varlen)
}

pub fn lt(left: &Array, right: &Array) -> Result<BooleanArray> {
    array_cmp_dispatch!(left, right, lt_primtive, lt_varlen)
}

pub fn lt_eq(left: &Array, right: &Array) -> Result<BooleanArray> {
    array_cmp_dispatch!(left, right, lt_eq_primtive, lt_eq_varlen)
}

pub fn gt(left: &Array, right: &Array) -> Result<BooleanArray> {
    array_cmp_dispatch!(left, right, gt_primtive, gt_varlen)
}

pub fn gt_eq(left: &Array, right: &Array) -> Result<BooleanArray> {
    array_cmp_dispatch!(left, right, gt_eq_primtive, gt_eq_varlen)
}

pub fn eq_primtive<T: PartialEq>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray> {
    primitive_array_cmp(left, right, PartialEq::eq)
}

pub fn neq_primtive<T: PartialEq>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray> {
    primitive_array_cmp(left, right, PartialEq::ne)
}

pub fn lt_primtive<T: PartialOrd>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray> {
    primitive_array_cmp(left, right, PartialOrd::lt)
}

pub fn lt_eq_primtive<T: PartialOrd>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray> {
    primitive_array_cmp(left, right, PartialOrd::le)
}

pub fn gt_primtive<T: PartialOrd>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray> {
    primitive_array_cmp(left, right, PartialOrd::gt)
}

pub fn gt_eq_primtive<T: PartialOrd>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray> {
    primitive_array_cmp(left, right, PartialOrd::ge)
}

pub fn eq_varlen<T: VarlenType + ?Sized, O: OffsetIndex>(
    left: &VarlenArray<T, O>,
    right: &VarlenArray<T, O>,
) -> Result<BooleanArray> {
    varlen_array_cmp(left, right, PartialEq::eq)
}

pub fn neq_varlen<T: VarlenType + ?Sized, O: OffsetIndex>(
    left: &VarlenArray<T, O>,
    right: &VarlenArray<T, O>,
) -> Result<BooleanArray> {
    varlen_array_cmp(left, right, PartialEq::ne)
}

pub fn lt_varlen<T: VarlenType + ?Sized, O: OffsetIndex>(
    left: &VarlenArray<T, O>,
    right: &VarlenArray<T, O>,
) -> Result<BooleanArray> {
    varlen_array_cmp(left, right, PartialOrd::lt)
}

pub fn lt_eq_varlen<T: VarlenType + ?Sized, O: OffsetIndex>(
    left: &VarlenArray<T, O>,
    right: &VarlenArray<T, O>,
) -> Result<BooleanArray> {
    varlen_array_cmp(left, right, PartialOrd::le)
}

pub fn gt_varlen<T: VarlenType + ?Sized, O: OffsetIndex>(
    left: &VarlenArray<T, O>,
    right: &VarlenArray<T, O>,
) -> Result<BooleanArray> {
    varlen_array_cmp(left, right, PartialOrd::gt)
}

pub fn gt_eq_varlen<T: VarlenType + ?Sized, O: OffsetIndex>(
    left: &VarlenArray<T, O>,
    right: &VarlenArray<T, O>,
) -> Result<BooleanArray> {
    varlen_array_cmp(left, right, PartialOrd::ge)
}

// TODO: Null comparisions (those should always return null)
macro_rules! scalar_cmp_dispatch {
    ($left:ident, $right:ident, $fn:expr) => {{
        match ($left, $right) {
            (ScalarValue::Boolean(left), ScalarValue::Boolean(right)) => $fn(left, right),
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
            (ScalarValue::Utf8(left), ScalarValue::Utf8(right)) => $fn(left, right),
            (ScalarValue::Binary(left), ScalarValue::Binary(right)) => $fn(left, right),
            (ScalarValue::LargeUtf8(left), ScalarValue::LargeUtf8(right)) => $fn(left, right),
            (ScalarValue::LargeBinary(left), ScalarValue::LargeBinary(right)) => $fn(left, right),
            _ => {
                return Err(RayexecError::new(
                    "Unsupported comparison operation on scalar",
                ))
            }
        }
    }};
}

pub fn eq_scalar(left: &ScalarValue, right: &ScalarValue) -> Result<bool> {
    scalar_cmp_dispatch!(left, right, |l, r| { Ok(PartialEq::eq(l, r)) })
}

pub fn neq_scalar(left: &ScalarValue, right: &ScalarValue) -> Result<bool> {
    scalar_cmp_dispatch!(left, right, |l, r| { Ok(PartialEq::ne(l, r)) })
}

pub fn lt_scalar(left: &ScalarValue, right: &ScalarValue) -> Result<bool> {
    scalar_cmp_dispatch!(left, right, |l, r| { Ok(PartialOrd::lt(l, r)) })
}

pub fn lt_eq_scalar(left: &ScalarValue, right: &ScalarValue) -> Result<bool> {
    scalar_cmp_dispatch!(left, right, |l, r| { Ok(PartialOrd::le(l, r)) })
}

pub fn gt_scalar(left: &ScalarValue, right: &ScalarValue) -> Result<bool> {
    scalar_cmp_dispatch!(left, right, |l, r| { Ok(PartialOrd::gt(l, r)) })
}

pub fn gt_eq_scalar(left: &ScalarValue, right: &ScalarValue) -> Result<bool> {
    scalar_cmp_dispatch!(left, right, |l, r| { Ok(PartialOrd::ge(l, r)) })
}

fn primitive_array_cmp<T, F>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    cmp_fn: F,
) -> Result<BooleanArray>
where
    F: Fn(&T, &T) -> bool,
{
    if left.len() != right.len() {
        return Err(RayexecError::new(
            "Left and right arrays have different lengths",
        ));
    }

    let left = left.values().as_ref().iter();
    let right = right.values().as_ref().iter();

    let bools = compare_value_iters(left, right, cmp_fn);

    // TODO: Nulls

    Ok(bools)
}

fn varlen_array_cmp<T, O, F>(
    left: &VarlenArray<T, O>,
    right: &VarlenArray<T, O>,
    cmp_fn: F,
) -> Result<BooleanArray>
where
    T: VarlenType + ?Sized,
    O: OffsetIndex,
    F: Fn(&T, &T) -> bool,
{
    if left.len() != right.len() {
        return Err(RayexecError::new(
            "Left and right arrays have different lengths",
        ));
    }

    let left = left.values_iter();
    let right = right.values_iter();

    let bools = compare_value_iters(left, right, cmp_fn);

    // TODO: Nulls

    Ok(bools)
}

/// Compare the the values from two iterators with some comparison function and
/// return a boolean array containing the results.
fn compare_value_iters<T, F>(
    left: impl Iterator<Item = T>,
    right: impl Iterator<Item = T>,
    cmp_fn: F,
) -> BooleanArray
where
    F: Fn(T, T) -> bool,
{
    let iter = left.zip(right).map(|(left, right)| cmp_fn(left, right));
    let bitmap = Bitmap::from_iter(iter);
    BooleanArray::new_with_values(bitmap)
}

#[cfg(test)]
mod tests {
    use crate::array::{Int64Array, Utf8Array};

    use super::*;

    #[test]
    fn primitive_lt() {
        let left = Int64Array::from_iter([4, 5, 6]);
        let right = Int64Array::from_iter([6, 5, 4]);

        let out = lt_primtive(&left, &right).unwrap();
        assert_eq!(BooleanArray::from_iter([true, false, false]), out);
    }

    #[test]
    fn varlen_lt() {
        let left = Utf8Array::from_iter(["aaa", "bbb", "ccc"]);
        let right = Utf8Array::from_iter(["ccc", "bbb", "aaa"]);

        let out = lt_varlen(&left, &right).unwrap();
        assert_eq!(BooleanArray::from_iter([true, false, false]), out);
    }
}
