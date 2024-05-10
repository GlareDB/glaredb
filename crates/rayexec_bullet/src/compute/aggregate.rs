use crate::array::{
    Array, BooleanArray, OffsetIndex, PrimitiveArray, PrimitiveNumeric, VarlenArray, VarlenType,
};
use crate::scalar::ScalarValue;
use rayexec_error::{RayexecError, Result};
use std::ops::Add;

/// Get the minumum value of an array.
///
/// If an array contains on nulls, a null scalar value will be returned.
pub fn min(arr: &Array) -> ScalarValue {
    match arr {
        Array::Null(_) => ScalarValue::Null,
        Array::Boolean(arr) => min_boolean(arr).into(),
        Array::Float32(arr) => min_primitive(arr).into(),
        Array::Float64(arr) => min_primitive(arr).into(),
        Array::Int8(arr) => min_primitive(arr).into(),
        Array::Int16(arr) => min_primitive(arr).into(),
        Array::Int32(arr) => min_primitive(arr).into(),
        Array::Int64(arr) => min_primitive(arr).into(),
        Array::UInt8(arr) => min_primitive(arr).into(),
        Array::UInt16(arr) => min_primitive(arr).into(),
        Array::UInt32(arr) => min_primitive(arr).into(),
        Array::UInt64(arr) => min_primitive(arr).into(),
        Array::Utf8(arr) => min_varlen(arr).into(),
        Array::LargeUtf8(arr) => min_varlen(arr).into(),
        Array::Binary(arr) => min_varlen(arr).into(),
        Array::LargeBinary(arr) => min_varlen(arr).into(),
        _ => unimplemented!(),
    }
}

/// Get the maximum value of an array.
///
/// If an array contains on nulls, a null scalar value will be returned.
pub fn max(arr: &Array) -> ScalarValue {
    match arr {
        Array::Null(_) => ScalarValue::Null,
        Array::Boolean(arr) => max_boolean(arr).into(),
        Array::Float32(arr) => max_primitive(arr).into(),
        Array::Float64(arr) => max_primitive(arr).into(),
        Array::Int8(arr) => max_primitive(arr).into(),
        Array::Int16(arr) => max_primitive(arr).into(),
        Array::Int32(arr) => max_primitive(arr).into(),
        Array::Int64(arr) => max_primitive(arr).into(),
        Array::UInt8(arr) => max_primitive(arr).into(),
        Array::UInt16(arr) => max_primitive(arr).into(),
        Array::UInt32(arr) => max_primitive(arr).into(),
        Array::UInt64(arr) => max_primitive(arr).into(),
        Array::Utf8(arr) => max_varlen(arr).into(),
        Array::LargeUtf8(arr) => max_varlen(arr).into(),
        Array::Binary(arr) => max_varlen(arr).into(),
        Array::LargeBinary(arr) => max_varlen(arr).into(),
        _ => unimplemented!(),
    }
}

/// Compute the sum of an array.
///
/// If an array only contains nulls, a null scalar value will be returned.
///
/// Errors if the array type is not a numeric type.
pub fn sum(arr: &Array) -> Result<ScalarValue> {
    Ok(match arr {
        Array::Null(_) => ScalarValue::Null,
        Array::Float32(arr) => sum_primitive(arr).into(),
        Array::Float64(arr) => sum_primitive(arr).into(),
        Array::Int8(arr) => sum_primitive(arr).into(),
        Array::Int16(arr) => sum_primitive(arr).into(),
        Array::Int32(arr) => sum_primitive(arr).into(),
        Array::Int64(arr) => sum_primitive(arr).into(),
        Array::UInt8(arr) => sum_primitive(arr).into(),
        Array::UInt16(arr) => sum_primitive(arr).into(),
        Array::UInt32(arr) => sum_primitive(arr).into(),
        Array::UInt64(arr) => sum_primitive(arr).into(),
        _ => return Err(RayexecError::new("Cannot sum a non-numeric array")),
    })
}

pub fn min_primitive<T: Copy + Default + PartialOrd + PrimitiveNumeric>(
    arr: &PrimitiveArray<T>,
) -> Option<T> {
    primitive_reduce(
        T::MAX_VALUE,
        arr,
        |acc, val| if acc < val { acc } else { val },
    )
}

pub fn max_primitive<T: Copy + Default + PartialOrd + PrimitiveNumeric>(
    arr: &PrimitiveArray<T>,
) -> Option<T> {
    primitive_reduce(
        T::MIN_VALUE,
        arr,
        |acc, val| if acc > val { acc } else { val },
    )
}

pub fn sum_primitive<T: Copy + Default + Add<Output = T> + PrimitiveNumeric>(
    arr: &PrimitiveArray<T>,
) -> Option<T> {
    primitive_reduce(T::ZERO_VALUE, arr, |acc, val| acc + val)
}

pub fn min_boolean(arr: &BooleanArray) -> Option<bool> {
    let values = arr.values();
    match arr.validity() {
        Some(bitmap) => {
            if bitmap.popcnt() == 0 {
                None
            } else {
                Some(!bitmap.index_iter().any(|idx| !values.value(idx)))
            }
        }
        None => Some(!arr.values().iter().any(|v| !v)),
    }
}

pub fn max_boolean(arr: &BooleanArray) -> Option<bool> {
    let values = arr.values();
    match arr.validity() {
        Some(bitmap) => {
            if bitmap.popcnt() == 0 {
                None
            } else {
                Some(bitmap.index_iter().any(|idx| values.value(idx)))
            }
        }
        None => Some(arr.values().iter().any(|v| v)),
    }
}

pub fn min_varlen<T: VarlenType + PartialOrd + ?Sized, O: OffsetIndex>(
    arr: &VarlenArray<T, O>,
) -> Option<&T> {
    varlen_reduce(arr, |acc, val| if acc < val { acc } else { val })
}

pub fn max_varlen<T: VarlenType + PartialOrd + ?Sized, O: OffsetIndex>(
    arr: &VarlenArray<T, O>,
) -> Option<&T> {
    varlen_reduce(arr, |acc, val| if acc > val { acc } else { val })
}

fn primitive_reduce<T: Copy>(
    start: T,
    arr: &PrimitiveArray<T>,
    reduce_fn: impl Fn(T, T) -> T,
) -> Option<T> {
    let values = arr.values();

    match &arr.validity() {
        Some(bitmap) => {
            if bitmap.popcnt() == 0 {
                // No "valid" values.
                return None;
            }

            let out = bitmap.index_iter().fold(start, |acc, idx| {
                let value = values.as_ref().get(idx).unwrap();
                reduce_fn(acc, *value)
            });

            Some(out)
        }
        None => {
            let out = values
                .as_ref()
                .iter()
                .fold(start, |acc, val| reduce_fn(acc, *val));
            Some(out)
        }
    }
}

fn varlen_reduce<'a, T: VarlenType + ?Sized, O: OffsetIndex>(
    arr: &'a VarlenArray<T, O>,
    reduce_fn: impl Fn(&'a T, &'a T) -> &'a T,
) -> Option<&T> {
    match &arr.validity() {
        Some(bitmap) => {
            if bitmap.popcnt() == 0 {
                // No "valid" values.
                return None;
            }

            let out = bitmap
                .index_iter()
                .map(|idx| arr.value(idx).unwrap())
                .reduce(|acc, val| reduce_fn(acc, val));

            out
        }
        None => arr.values_iter().reduce(reduce_fn),
    }
}

#[cfg(test)]
mod tests {
    use crate::array::Int32Array;

    use super::*;

    #[test]
    fn primitive_min() {
        let arr = Int32Array::from_iter([7, 8, 2, 1, 3, 4, 5]);
        let scalar = min_primitive(&arr);
        assert_eq!(Some(1), scalar);
    }

    #[test]
    fn primitive_min_nulls() {
        let arr = Int32Array::from_iter([Some(3), None, Some(4)]);
        let scalar = min_primitive(&arr);
        assert_eq!(Some(3), scalar);
    }

    #[test]
    fn primitive_min_all_nulls() {
        let arr = Int32Array::from_iter([None, None, None]);
        let scalar = min_primitive(&arr);
        assert_eq!(None, scalar);
    }

    #[test]
    fn bool_min_mixed_true_false() {
        let arr = BooleanArray::from_iter([true, false, true]);
        let got = min_boolean(&arr);
        assert_eq!(Some(false), got);
    }

    #[test]
    fn bool_min_all_false() {
        let arr = BooleanArray::from_iter([false, false, false]);
        let got = min_boolean(&arr);
        assert_eq!(Some(false), got);
    }

    #[test]
    fn bool_min_all_true() {
        let arr = BooleanArray::from_iter([true, true, true]);
        let got = min_boolean(&arr);
        assert_eq!(Some(true), got);
    }

    #[test]
    fn bool_min_mixed_nulls() {
        let arr = BooleanArray::from_iter([Some(true), Some(false), None]);
        let got = min_boolean(&arr);
        assert_eq!(Some(false), got);
    }

    #[test]
    fn bool_min_all_nulls() {
        let arr = BooleanArray::from_iter([None, None, None]);
        let got = min_boolean(&arr);
        assert_eq!(None, got);
    }

    #[test]
    fn bool_max_mixed_true_false() {
        let arr = BooleanArray::from_iter([true, false, true]);
        let got = max_boolean(&arr);
        assert_eq!(Some(true), got);
    }

    #[test]
    fn bool_max_all_false() {
        let arr = BooleanArray::from_iter([false, false, false]);
        let got = max_boolean(&arr);
        assert_eq!(Some(false), got);
    }

    #[test]
    fn bool_max_all_true() {
        let arr = BooleanArray::from_iter([true, true, true]);
        let got = max_boolean(&arr);
        assert_eq!(Some(true), got);
    }

    #[test]
    fn bool_max_mixed_nulls() {
        let arr = BooleanArray::from_iter([Some(true), Some(false), None]);
        let got = max_boolean(&arr);
        assert_eq!(Some(true), got);
    }

    #[test]
    fn bool_max_all_nulls() {
        let arr = BooleanArray::from_iter([None, None, None]);
        let got = max_boolean(&arr);
        assert_eq!(None, got);
    }

    #[test]
    fn primitive_max() {
        let arr = Int32Array::from_iter([7, 8, 2, 1, 3, 4, 5]);
        let scalar = max_primitive(&arr);
        assert_eq!(Some(8), scalar);
    }

    #[test]
    fn primitive_max_nulls() {
        let arr = Int32Array::from_iter([Some(3), None, Some(4)]);
        let scalar = max_primitive(&arr);
        assert_eq!(Some(4), scalar);
    }

    #[test]
    fn primitive_max_all_nulls() {
        let arr = Int32Array::from_iter([None, None, None]);
        let scalar = max_primitive(&arr);
        assert_eq!(None, scalar);
    }

    #[test]
    fn primitive_sum() {
        let arr = Int32Array::from_iter([7, 8, 2, 1, 3, 4, 5]);
        let scalar = sum_primitive(&arr);
        assert_eq!(Some(30), scalar);
    }

    #[test]
    fn primitive_sum_nulls() {
        let arr = Int32Array::from_iter([Some(3), None, Some(4)]);
        let scalar = sum_primitive(&arr);
        assert_eq!(Some(7), scalar);
    }

    #[test]
    fn primitive_sum_all_nulls() {
        let arr = Int32Array::from_iter([None, None, None]);
        let scalar = sum_primitive(&arr);
        assert_eq!(None, scalar);
    }
}
