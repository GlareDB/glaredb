use crate::{
    array::{
        Array, ArrayAccessor, ArrayBuilder, OffsetIndex, PrimitiveArray, PrimitiveArrayBuilder,
        VarlenArray, VarlenType,
    },
    bitmap::Bitmap,
};
use rayexec_error::{RayexecError, Result};

/// Slice an array at the given range.
///
/// Not zero-copy.
///
/// A full zero-copy implementation will come in the future and may make use of
/// "view" type arrays.
pub fn slice(arr: &Array, start: usize, count: usize) -> Result<Array> {
    Ok(match arr {
        Array::Null(_) => unimplemented!(),       // TODO
        Array::Boolean(_arr) => unimplemented!(), // TODO
        Array::Float32(arr) => Array::Float32(slice_primitive(arr, start, count)?),
        Array::Float64(arr) => Array::Float64(slice_primitive(arr, start, count)?),
        Array::Int8(arr) => Array::Int8(slice_primitive(arr, start, count)?),
        Array::Int16(arr) => Array::Int16(slice_primitive(arr, start, count)?),
        Array::Int32(arr) => Array::Int32(slice_primitive(arr, start, count)?),
        Array::Int64(arr) => Array::Int64(slice_primitive(arr, start, count)?),
        Array::UInt8(arr) => Array::UInt8(slice_primitive(arr, start, count)?),
        Array::UInt16(arr) => Array::UInt16(slice_primitive(arr, start, count)?),
        Array::UInt32(arr) => Array::UInt32(slice_primitive(arr, start, count)?),
        Array::UInt64(arr) => Array::UInt64(slice_primitive(arr, start, count)?),
        Array::Utf8(arr) => Array::Utf8(slice_varlen(arr, start, count)?),
        Array::LargeUtf8(arr) => Array::LargeUtf8(slice_varlen(arr, start, count)?),
        Array::Binary(arr) => Array::Binary(slice_varlen(arr, start, count)?),
        Array::LargeBinary(arr) => Array::LargeBinary(slice_varlen(arr, start, count)?),
        _ => unimplemented!(),
    })
}

pub fn slice_primitive<T: Copy>(
    arr: &PrimitiveArray<T>,
    start: usize,
    count: usize,
) -> Result<PrimitiveArray<T>> {
    if start + count > arr.len() {
        return Err(RayexecError::new(format!(
            "Range end out of bounds, start: {start}, count: {count}, len: {}",
            arr.len()
        )));
    }

    let vals = arr.values_iter();

    let mut builder = PrimitiveArrayBuilder::with_capacity(arr.len());
    vals.skip(start)
        .take(count)
        .for_each(|val| builder.push_value(val));

    if let Some(validity) = arr.validity() {
        let new_validity = Bitmap::from_iter(validity.iter().skip(start).take(count));
        builder.put_validity(new_validity);
    }

    Ok(builder.into_typed_array())
}

pub fn slice_varlen<T: VarlenType + ?Sized, O: OffsetIndex>(
    arr: &VarlenArray<T, O>,
    start: usize,
    count: usize,
) -> Result<VarlenArray<T, O>> {
    if start + count > arr.len() {
        return Err(RayexecError::new(format!(
            "Range end out of bounds, start: {start}, count: {count}, len: {}",
            arr.len()
        )));
    }

    let vals = arr.values_iter();
    let mut new_arr = VarlenArray::from_iter(vals.skip(start).take(count));

    if let Some(validity) = arr.validity() {
        let new_validity = Bitmap::from_iter(validity.iter().skip(start).take(count));
        new_arr.put_validity(new_validity);
    }

    Ok(new_arr)
}

#[cfg(test)]
mod tests {
    use crate::array::{Int32Array, Utf8Array};

    use super::*;

    #[test]
    fn slice_primitive_from_middle() {
        let arr = Int32Array::from_iter([1, 2, 3, 4]);
        let out = slice_primitive(&arr, 1, 2).unwrap();

        let expected = Int32Array::from_iter([2, 3]);
        assert_eq!(expected, out);
    }

    #[test]
    fn slice_varlen_from_middle() {
        let arr = Utf8Array::from_iter(["hello", "world", "goodbye", "world"]);
        let out = slice_varlen(&arr, 1, 2).unwrap();

        let expected = Utf8Array::from_iter(["world", "goodbye"]);
        assert_eq!(expected, out);
    }
}
