use crate::{
    array::{
        Array, BooleanArray, BooleanValuesBuffer, Decimal128Array, Decimal64Array, NullArray,
        OffsetIndex, PrimitiveArray, ValuesBuffer, VarlenArray, VarlenType, VarlenValuesBuffer,
    },
    bitmap::Bitmap,
    compute::macros::collect_arrays_of_type,
    datatype::DataType,
};
use rayexec_error::{not_implemented, RayexecError, Result};

/// Interleave multiple arrays into a single array.
///
/// The provided indices should be (array, row) pairs which are used to build
/// the final array. (array, row) pairs may be provided more than once.
///
/// Errors if no arrays are provided, or if not all arrays are of the same type.
pub fn interleave(arrays: &[&Array], indices: &[(usize, usize)]) -> Result<Array> {
    let datatype = match arrays.first() {
        Some(arr) => arr.datatype(),
        None => return Err(RayexecError::new("Cannot interleave zero arrays")),
    };

    match datatype {
        DataType::Null => {
            let _arrs = collect_arrays_of_type!(arrays, Null, datatype)?; // Done just for error checking.
            Ok(Array::Null(NullArray::new(indices.len())))
        }
        DataType::Boolean => {
            let arrs = collect_arrays_of_type!(arrays, Boolean, datatype)?;
            Ok(Array::Boolean(interleave_boolean(&arrs, indices)?))
        }
        DataType::Int8 => {
            let arrs = collect_arrays_of_type!(arrays, Int8, datatype)?;
            Ok(Array::Int8(interleave_primitive(&arrs, indices)?))
        }
        DataType::Int16 => {
            let arrs = collect_arrays_of_type!(arrays, Int16, datatype)?;
            Ok(Array::Int16(interleave_primitive(&arrs, indices)?))
        }
        DataType::Int32 => {
            let arrs = collect_arrays_of_type!(arrays, Int32, datatype)?;
            Ok(Array::Int32(interleave_primitive(&arrs, indices)?))
        }
        DataType::Int64 => {
            let arrs = collect_arrays_of_type!(arrays, Int64, datatype)?;
            Ok(Array::Int64(interleave_primitive(&arrs, indices)?))
        }
        DataType::UInt8 => {
            let arrs = collect_arrays_of_type!(arrays, UInt8, datatype)?;
            Ok(Array::UInt8(interleave_primitive(&arrs, indices)?))
        }
        DataType::UInt16 => {
            let arrs = collect_arrays_of_type!(arrays, UInt16, datatype)?;
            Ok(Array::UInt16(interleave_primitive(&arrs, indices)?))
        }
        DataType::UInt32 => {
            let arrs = collect_arrays_of_type!(arrays, UInt32, datatype)?;
            Ok(Array::UInt32(interleave_primitive(&arrs, indices)?))
        }
        DataType::UInt64 => {
            let arrs = collect_arrays_of_type!(arrays, UInt64, datatype)?;
            Ok(Array::UInt64(interleave_primitive(&arrs, indices)?))
        }
        DataType::Float32 => {
            let arrs = collect_arrays_of_type!(arrays, Float32, datatype)?;
            Ok(Array::Float32(interleave_primitive(&arrs, indices)?))
        }
        DataType::Float64 => {
            let arrs = collect_arrays_of_type!(arrays, Float64, datatype)?;
            Ok(Array::Float64(interleave_primitive(&arrs, indices)?))
        }
        DataType::Decimal64(meta) => {
            let arrs = collect_arrays_of_type!(arrays, Decimal64, datatype)?;
            let primitives: Vec<_> = arrs.iter().map(|arr| arr.get_primitive()).collect();
            let interleaved = interleave_primitive(&primitives, indices)?;
            Ok(Array::Decimal64(Decimal64Array::new(
                meta.precision,
                meta.scale,
                interleaved,
            )))
        }
        DataType::Decimal128(meta) => {
            let arrs = collect_arrays_of_type!(arrays, Decimal128, datatype)?;
            let primitives: Vec<_> = arrs.iter().map(|arr| arr.get_primitive()).collect();
            let interleaved = interleave_primitive(&primitives, indices)?;
            Ok(Array::Decimal128(Decimal128Array::new(
                meta.precision,
                meta.scale,
                interleaved,
            )))
        }
        DataType::Utf8 => {
            let arrs = collect_arrays_of_type!(arrays, Utf8, datatype)?;
            Ok(Array::Utf8(interleave_varlen(&arrs, indices)?))
        }
        DataType::LargeUtf8 => {
            let arrs = collect_arrays_of_type!(arrays, LargeUtf8, datatype)?;
            Ok(Array::LargeUtf8(interleave_varlen(&arrs, indices)?))
        }
        DataType::Binary => {
            let arrs = collect_arrays_of_type!(arrays, Binary, datatype)?;
            Ok(Array::Binary(interleave_varlen(&arrs, indices)?))
        }
        DataType::LargeBinary => {
            let arrs = collect_arrays_of_type!(arrays, LargeBinary, datatype)?;
            Ok(Array::LargeBinary(interleave_varlen(&arrs, indices)?))
        }
        other => not_implemented!("interleave {other}"),
    }
}

pub fn interleave_boolean(
    arrays: &[&BooleanArray],
    indices: &[(usize, usize)],
) -> Result<BooleanArray> {
    let mut buffer = BooleanValuesBuffer::with_capacity(indices.len());
    for (arr_idx, row_idx) in indices {
        let v = arrays[*arr_idx].value(*row_idx).expect("row to exist");
        buffer.push_value(v);
    }

    let validities: Vec<_> = arrays.iter().map(|arr| arr.validity()).collect();
    let validities = interleave_validities(&validities, indices);

    Ok(BooleanArray::new(buffer, validities))
}

pub fn interleave_primitive<T: Copy + Default>(
    arrays: &[&PrimitiveArray<T>],
    indices: &[(usize, usize)],
) -> Result<PrimitiveArray<T>> {
    let mut buffer = Vec::with_capacity(indices.len());
    for (arr_idx, row_idx) in indices {
        let v = arrays[*arr_idx].value(*row_idx).expect("row to exist");
        buffer.push_value(*v);
    }

    let validities: Vec<_> = arrays.iter().map(|arr| arr.validity()).collect();
    let validities = interleave_validities(&validities, indices);

    Ok(PrimitiveArray::new(buffer, validities))
}

pub fn interleave_varlen<T, O>(
    arrays: &[&VarlenArray<T, O>],
    indices: &[(usize, usize)],
) -> Result<VarlenArray<T, O>>
where
    T: VarlenType + ?Sized,
    O: OffsetIndex,
{
    let mut buffer = VarlenValuesBuffer::default();
    for (arr_idx, row_idx) in indices {
        let v = arrays[*arr_idx].value(*row_idx).expect("row to exist");
        buffer.push_value(v);
    }

    let validities: Vec<_> = arrays.iter().map(|arr| arr.validity()).collect();
    let validities = interleave_validities(&validities, indices);

    Ok(VarlenArray::new(buffer, validities))
}

fn interleave_validities(
    validities: &[Option<&Bitmap>],
    indices: &[(usize, usize)],
) -> Option<Bitmap> {
    let all_none = validities.iter().all(|v| v.is_none());
    if all_none {
        return None;
    }

    let mut validity = Bitmap::default();
    for (arr_idx, row_idx) in indices {
        let v = validities[*arr_idx]
            .map(|bm| bm.value(*row_idx))
            .unwrap_or(true);

        validity.push(v);
    }

    Some(validity)
}

#[cfg(test)]
mod tests {
    use crate::array::{Int32Array, Utf8Array};

    use super::*;

    #[test]
    fn simple_interleave_primitive() {
        let arr1 = Int32Array::from_iter([1, 2, 3]);
        let arr2 = Int32Array::from_iter([4, 5, 6, 7]);

        #[rustfmt::skip]
        let indices = vec![
            (0, 0),
            (1, 2),
            (1, 3),
            (0, 2),
            (0, 1),
        ];

        let out = interleave_primitive(&[&arr1, &arr2], &indices).unwrap();

        let expected = Int32Array::from_iter([1, 6, 7, 3, 2]);
        assert_eq!(expected, out);
    }

    #[test]
    fn simple_interleave_varlen() {
        let arr1 = Utf8Array::from_iter(["cat", "dog", "fish"]);
        let arr2 = Utf8Array::from_iter(["mario", "wario", "yoshi", "peach"]);

        #[rustfmt::skip]
        let indices = vec![
            (0, 0),
            (1, 2),
            (1, 3),
            (0, 2),
            (0, 1),
        ];

        let out = interleave_varlen(&[&arr1, &arr2], &indices).unwrap();

        let expected = Utf8Array::from_iter(["cat", "yoshi", "peach", "fish", "dog"]);
        assert_eq!(expected, out);
    }
}
