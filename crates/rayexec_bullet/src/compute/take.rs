use crate::{
    array::{
        Array, BooleanArray, DecimalArray, NullArray, OffsetIndex, PrimitiveArray, TimestampArray,
        VarlenArray, VarlenType, VarlenValuesBuffer,
    },
    bitmap::Bitmap,
};
use rayexec_error::{not_implemented, RayexecError, Result};

/// Take values from an array at the provided indices, and return a new array.
///
/// An index may appear multiple times.
pub fn take(arr: &Array, indices: &[usize]) -> Result<Array> {
    Ok(match arr {
        Array::Null(_) => Array::Null(NullArray::new(indices.len())),
        Array::Boolean(arr) => Array::Boolean(take_boolean(arr, indices)?),
        Array::Float32(arr) => Array::Float32(take_primitive(arr, indices)?),
        Array::Float64(arr) => Array::Float64(take_primitive(arr, indices)?),
        Array::Int8(arr) => Array::Int8(take_primitive(arr, indices)?),
        Array::Int16(arr) => Array::Int16(take_primitive(arr, indices)?),
        Array::Int32(arr) => Array::Int32(take_primitive(arr, indices)?),
        Array::Int64(arr) => Array::Int64(take_primitive(arr, indices)?),
        Array::UInt8(arr) => Array::UInt8(take_primitive(arr, indices)?),
        Array::UInt16(arr) => Array::UInt16(take_primitive(arr, indices)?),
        Array::UInt32(arr) => Array::UInt32(take_primitive(arr, indices)?),
        Array::UInt64(arr) => Array::UInt64(take_primitive(arr, indices)?),
        Array::Decimal64(arr) => {
            let new_primitive = take_primitive(arr.get_primitive(), indices)?;
            Array::Decimal64(DecimalArray::new(
                arr.precision(),
                arr.scale(),
                new_primitive,
            ))
        }
        Array::Decimal128(arr) => {
            let new_primitive = take_primitive(arr.get_primitive(), indices)?;
            Array::Decimal128(DecimalArray::new(
                arr.precision(),
                arr.scale(),
                new_primitive,
            ))
        }
        Array::Date32(arr) => Array::Date32(take_primitive(arr, indices)?),
        Array::Date64(arr) => Array::Date64(take_primitive(arr, indices)?),
        Array::Timestamp(arr) => {
            let primitive = take_primitive(arr.get_primitive(), indices)?;
            Array::Timestamp(TimestampArray::new(arr.unit(), primitive))
        }
        Array::Utf8(arr) => Array::Utf8(take_varlen(arr, indices)?),
        Array::LargeUtf8(arr) => Array::LargeUtf8(take_varlen(arr, indices)?),
        Array::Binary(arr) => Array::Binary(take_varlen(arr, indices)?),
        Array::LargeBinary(arr) => Array::LargeBinary(take_varlen(arr, indices)?),
        other => not_implemented!("other: {}", other.datatype()),
    })
}

pub fn take_boolean(arr: &BooleanArray, indices: &[usize]) -> Result<BooleanArray> {
    if !indices.iter().all(|&idx| idx < arr.len()) {
        return Err(RayexecError::new("Index out of bounds"));
    }

    let values = arr.values();
    let new_values = Bitmap::from_iter(indices.iter().map(|idx| values.value(*idx)));

    let validity = arr
        .validity()
        .map(|validity| Bitmap::from_iter(indices.iter().map(|idx| validity.value(*idx))));

    Ok(BooleanArray::new(new_values, validity))
}

pub fn take_primitive<T: Copy>(
    arr: &PrimitiveArray<T>,
    indices: &[usize],
) -> Result<PrimitiveArray<T>> {
    if !indices.iter().all(|&idx| idx < arr.len()) {
        return Err(RayexecError::new("Index out of bounds"));
    }

    let values = arr.values();
    let new_values: Vec<_> = indices
        .iter()
        .map(|idx| *values.as_ref().get(*idx).unwrap())
        .collect();

    let validity = arr
        .validity()
        .map(|validity| Bitmap::from_iter(indices.iter().map(|idx| validity.value(*idx))));

    let taken = PrimitiveArray::new(new_values, validity);

    Ok(taken)
}

pub fn take_varlen<T: VarlenType + ?Sized, O: OffsetIndex>(
    arr: &VarlenArray<T, O>,
    indices: &[usize],
) -> Result<VarlenArray<T, O>> {
    if !indices.iter().all(|&idx| idx < arr.len()) {
        return Err(RayexecError::new("Index out of bounds"));
    }

    let new_values: VarlenValuesBuffer<_> =
        indices.iter().map(|idx| arr.value(*idx).unwrap()).collect();

    let validity = arr
        .validity()
        .map(|validity| Bitmap::from_iter(indices.iter().map(|idx| validity.value(*idx))));

    let taken = VarlenArray::new(new_values, validity);

    Ok(taken)
}

#[cfg(test)]
mod tests {
    use crate::array::{Int32Array, Utf8Array};

    use super::*;

    #[test]
    fn simple_take_primitive() {
        let arr = Int32Array::from_iter([6, 7, 8, 9]);
        let indices = [1, 1, 3, 0];
        let out = take_primitive(&arr, &indices).unwrap();

        let expected = Int32Array::from_iter([7, 7, 9, 6]);
        assert_eq!(expected, out);
    }

    #[test]
    fn take_primitive_out_of_bounds() {
        let arr = Int32Array::from_iter([6, 7, 8, 9]);
        let indices = [1, 1, 3, 4];

        let _ = take_primitive(&arr, &indices).unwrap_err();
    }

    #[test]
    fn simple_take_varlen() {
        let arr = Utf8Array::from_iter(["aaa", "bbb", "ccc", "ddd"]);
        let indices = [1, 1, 3, 0];
        let out = take_varlen(&arr, &indices).unwrap();

        let expected = Utf8Array::from_iter(["bbb", "bbb", "ddd", "aaa"]);
        assert_eq!(expected, out);
    }

    #[test]
    fn take_varlen_out_of_bounds() {
        let arr = Utf8Array::from_iter(["aaa", "bbb", "ccc", "ddd"]);
        let indices = [1, 1, 3, 4];

        let _ = take_varlen(&arr, &indices).unwrap_err();
    }
}
