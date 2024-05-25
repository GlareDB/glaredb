use crate::array::{Array, NullArray, OffsetIndex, PrimitiveArray, VarlenArray, VarlenType};
use crate::field::DataType;
use rayexec_error::{RayexecError, Result};

use super::macros::collect_arrays_of_type;

/// Concat multiple arrays into a single array.
///
/// All arrays must be of the same type.
pub fn concat(arrays: &[&Array]) -> Result<Array> {
    if arrays.is_empty() {
        return Err(RayexecError::new("Cannot concat zero arrays"));
    }

    let datatype = arrays[0].datatype();

    match datatype {
        DataType::Null => {
            let arrs = collect_arrays_of_type!(arrays, Null, datatype)?;
            Ok(Array::Null(NullArray::new(
                arrs.iter().map(|arr| arr.len()).sum(),
            )))
        }

        DataType::Boolean => {
            let _arrs = collect_arrays_of_type!(arrays, Boolean, datatype)?;
            unimplemented!()
        }
        DataType::Float32 => {
            let arrs = collect_arrays_of_type!(arrays, Float32, datatype)?;
            Ok(Array::Float32(concat_primitive(arrs.as_slice())))
        }
        DataType::Float64 => {
            let arrs = collect_arrays_of_type!(arrays, Float64, datatype)?;
            Ok(Array::Float64(concat_primitive(arrs.as_slice())))
        }
        DataType::Int8 => {
            let arrs = collect_arrays_of_type!(arrays, Int8, datatype)?;
            Ok(Array::Int8(concat_primitive(arrs.as_slice())))
        }
        DataType::Int16 => {
            let arrs = collect_arrays_of_type!(arrays, Int16, datatype)?;
            Ok(Array::Int16(concat_primitive(arrs.as_slice())))
        }
        DataType::Int32 => {
            let arrs = collect_arrays_of_type!(arrays, Int32, datatype)?;
            Ok(Array::Int32(concat_primitive(arrs.as_slice())))
        }
        DataType::Int64 => {
            let arrs = collect_arrays_of_type!(arrays, Int64, datatype)?;
            Ok(Array::Int64(concat_primitive(arrs.as_slice())))
        }
        DataType::UInt8 => {
            let arrs = collect_arrays_of_type!(arrays, UInt8, datatype)?;
            Ok(Array::UInt8(concat_primitive(arrs.as_slice())))
        }
        DataType::UInt16 => {
            let arrs = collect_arrays_of_type!(arrays, UInt16, datatype)?;
            Ok(Array::UInt16(concat_primitive(arrs.as_slice())))
        }
        DataType::UInt32 => {
            let arrs = collect_arrays_of_type!(arrays, UInt32, datatype)?;
            Ok(Array::UInt32(concat_primitive(arrs.as_slice())))
        }
        DataType::UInt64 => {
            let arrs = collect_arrays_of_type!(arrays, UInt64, datatype)?;
            Ok(Array::UInt64(concat_primitive(arrs.as_slice())))
        }
        DataType::Utf8 => {
            let arrs = collect_arrays_of_type!(arrays, Utf8, datatype)?;
            Ok(Array::Utf8(concat_varlen(arrs.as_slice())))
        }
        DataType::LargeUtf8 => {
            let arrs = collect_arrays_of_type!(arrays, LargeUtf8, datatype)?;
            Ok(Array::LargeUtf8(concat_varlen(arrs.as_slice())))
        }
        DataType::Binary => {
            let arrs = collect_arrays_of_type!(arrays, Binary, datatype)?;
            Ok(Array::Binary(concat_varlen(arrs.as_slice())))
        }
        DataType::LargeBinary => {
            let arrs = collect_arrays_of_type!(arrays, LargeBinary, datatype)?;
            Ok(Array::LargeBinary(concat_varlen(arrs.as_slice())))
        }
        DataType::Struct { .. } => unimplemented!(),
    }
}

pub fn concat_primitive<T: Copy>(arrays: &[&PrimitiveArray<T>]) -> PrimitiveArray<T> {
    // TODO: Nulls
    let values_iters = arrays.iter().map(|arr| arr.values().as_ref());
    let values: Vec<T> = values_iters.flat_map(|v| v.iter().copied()).collect();
    PrimitiveArray::from(values)
}

pub fn concat_varlen<T, O>(arrays: &[&VarlenArray<T, O>]) -> VarlenArray<T, O>
where
    T: VarlenType + ?Sized,
    O: OffsetIndex,
{
    // TODO: Nulls
    // TODO: We should probably preallocate the values buffer.
    let values_iters = arrays.iter().map(|arr| arr.values_iter());
    VarlenArray::from_iter(values_iters.flat_map(|iter| iter))
}

#[cfg(test)]
mod tests {
    use crate::array::{Int64Array, Utf8Array};

    use super::*;

    #[test]
    fn concat_primitive() {
        let arrs = [
            &Array::Int64(Int64Array::from_iter([1])),
            &Array::Int64(Int64Array::from_iter([2, 3])),
            &Array::Int64(Int64Array::from_iter([4, 5, 6])),
        ];

        let got = concat(&arrs).unwrap();
        let expected = Array::Int64(Int64Array::from_iter([1, 2, 3, 4, 5, 6]));

        assert_eq!(expected, got);
    }

    #[test]
    fn concat_varlen() {
        let arrs = [
            &Array::Utf8(Utf8Array::from_iter(["a"])),
            &Array::Utf8(Utf8Array::from_iter(["bb", "ccc"])),
            &Array::Utf8(Utf8Array::from_iter(["dddd", "eeeee", "ffffff"])),
        ];

        let got = concat(&arrs).unwrap();
        let expected = Array::Utf8(Utf8Array::from_iter([
            "a", "bb", "ccc", "dddd", "eeeee", "ffffff",
        ]));

        assert_eq!(expected, got);
    }
}
