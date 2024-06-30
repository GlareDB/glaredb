use crate::array::validity::concat_validities;
use crate::array::{
    Array, BooleanArray, BooleanValuesBuffer, DecimalArray, NullArray, OffsetIndex, PrimitiveArray,
    VarlenArray, VarlenType, VarlenValuesBuffer,
};
use crate::batch::Batch;
use crate::datatype::DataType;
use rayexec_error::{RayexecError, Result};

use super::macros::collect_arrays_of_type;

/// Concat multiple batches into a single batch.
///
/// Errors if the batches do not have the same schema.
pub fn concat_batches(batches: &[Batch]) -> Result<Batch> {
    if batches.is_empty() {
        return Ok(Batch::empty());
    }

    let num_cols = batches[0].num_columns();
    let mut concatted = Vec::with_capacity(num_cols);

    for col_idx in 0..num_cols {
        let cols = batches
            .iter()
            .map(|batch| batch.column(col_idx).map(|a| a.as_ref()))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| RayexecError::new("Missing column"))?;

        concatted.push(concat(&cols)?);
    }

    Batch::try_new(concatted)
}

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
            let arrs = collect_arrays_of_type!(arrays, Boolean, datatype)?;
            Ok(Array::Boolean(concat_boolean(arrs.as_slice())))
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
        DataType::Int128 => {
            let arrs = collect_arrays_of_type!(arrays, Int128, datatype)?;
            Ok(Array::Int128(concat_primitive(arrs.as_slice())))
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
        DataType::UInt128 => {
            let arrs = collect_arrays_of_type!(arrays, UInt128, datatype)?;
            Ok(Array::UInt128(concat_primitive(arrs.as_slice())))
        }
        DataType::Decimal64(meta) => {
            let arrs = collect_arrays_of_type!(arrays, Decimal64, datatype)?;
            let arrs: Vec<_> = arrs.iter().map(|arr| arr.get_primitive()).collect();
            Ok(Array::Decimal64(DecimalArray::new(
                meta.precision,
                meta.scale,
                concat_primitive(arrs.as_slice()),
            )))
        }
        DataType::Decimal128(meta) => {
            let arrs = collect_arrays_of_type!(arrays, Decimal128, datatype)?;
            let arrs: Vec<_> = arrs.iter().map(|arr| arr.get_primitive()).collect();
            Ok(Array::Decimal128(DecimalArray::new(
                meta.precision,
                meta.scale,
                concat_primitive(arrs.as_slice()),
            )))
        }
        DataType::Date32 => {
            let arrs = collect_arrays_of_type!(arrays, Date32, datatype)?;
            Ok(Array::Date32(concat_primitive(arrs.as_slice())))
        }
        DataType::Date64 => {
            let arrs = collect_arrays_of_type!(arrays, Date64, datatype)?;
            Ok(Array::Date64(concat_primitive(arrs.as_slice())))
        }
        DataType::TimestampSeconds => {
            let arrs = collect_arrays_of_type!(arrays, TimestampSeconds, datatype)?;
            Ok(Array::TimestampSeconds(concat_primitive(arrs.as_slice())))
        }
        DataType::TimestampMilliseconds => {
            let arrs = collect_arrays_of_type!(arrays, TimestampMilliseconds, datatype)?;
            Ok(Array::TimestampMilliseconds(concat_primitive(
                arrs.as_slice(),
            )))
        }
        DataType::TimestampMicroseconds => {
            let arrs = collect_arrays_of_type!(arrays, TimestampMicroseconds, datatype)?;
            Ok(Array::TimestampMicroseconds(concat_primitive(
                arrs.as_slice(),
            )))
        }
        DataType::TimestampNanoseconds => {
            let arrs = collect_arrays_of_type!(arrays, TimestampNanoseconds, datatype)?;
            Ok(Array::TimestampNanoseconds(concat_primitive(
                arrs.as_slice(),
            )))
        }
        DataType::Interval => {
            let arrs = collect_arrays_of_type!(arrays, Interval, datatype)?;
            Ok(Array::Interval(concat_primitive(arrs.as_slice())))
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
        DataType::Struct(_) => unimplemented!(),
        DataType::List(_) => unimplemented!(),
    }
}

pub fn concat_boolean(arrays: &[&BooleanArray]) -> BooleanArray {
    let validity = concat_validities(arrays.iter().map(|arr| (arr.len(), arr.validity())));
    let values_iters = arrays.iter().map(|arr| arr.values());
    let values: BooleanValuesBuffer = values_iters.flat_map(|v| v.iter()).collect();
    BooleanArray::new(values, validity)
}

pub fn concat_primitive<T: Copy>(arrays: &[&PrimitiveArray<T>]) -> PrimitiveArray<T> {
    let validity = concat_validities(arrays.iter().map(|arr| (arr.len(), arr.validity())));
    let values_iters = arrays.iter().map(|arr| arr.values().as_ref());
    let values: Vec<T> = values_iters.flat_map(|v| v.iter().copied()).collect();
    PrimitiveArray::new(values, validity)
}

pub fn concat_varlen<T, O>(arrays: &[&VarlenArray<T, O>]) -> VarlenArray<T, O>
where
    T: VarlenType + ?Sized,
    O: OffsetIndex,
{
    let validity = concat_validities(arrays.iter().map(|arr| (arr.len(), arr.validity())));
    let values_iters = arrays.iter().map(|arr| arr.values_iter());
    let values: VarlenValuesBuffer<_> = values_iters.flatten().collect();
    VarlenArray::new(values, validity)
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
