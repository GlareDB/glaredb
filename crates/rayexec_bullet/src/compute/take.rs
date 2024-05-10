use crate::array::{Array, NullArray, OffsetIndex, PrimitiveArray, VarlenArray, VarlenType};
use rayexec_error::{RayexecError, Result};

pub fn take(arr: &Array, indices: &[usize]) -> Result<Array> {
    Ok(match arr {
        Array::Null(_) => Array::Null(NullArray::new(indices.len())),
        Array::Boolean(_arr) => unimplemented!(), // TODO
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
        Array::Utf8(arr) => Array::Utf8(take_varlen(arr, indices)?),
        Array::LargeUtf8(arr) => Array::LargeUtf8(take_varlen(arr, indices)?),
        Array::Binary(arr) => Array::Binary(take_varlen(arr, indices)?),
        Array::LargeBinary(arr) => Array::LargeBinary(take_varlen(arr, indices)?),
        _ => unimplemented!(),
    })
}

pub fn take_primitive<T: Copy>(
    arr: &PrimitiveArray<T>,
    indices: &[usize],
) -> Result<PrimitiveArray<T>> {
    if !indices.iter().all(|&idx| idx < arr.len()) {
        return Err(RayexecError::new("Index out of bounds"));
    }

    let values = arr.values();
    // TODO: validity
    let iter = indices
        .iter()
        .map(|idx| *values.as_ref().get(*idx).unwrap());
    let taken = PrimitiveArray::from_iter(iter);

    Ok(taken)
}

pub fn take_varlen<T: VarlenType + ?Sized, O: OffsetIndex>(
    arr: &VarlenArray<T, O>,
    indices: &[usize],
) -> Result<VarlenArray<T, O>> {
    if !indices.iter().all(|&idx| idx < arr.len()) {
        return Err(RayexecError::new("Index out of bounds"));
    }

    // TODO: Validity
    let iter = indices.iter().map(|idx| arr.value(*idx).unwrap());
    let taken = VarlenArray::from_iter(iter);

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
