use crate::{
    array::{Array, OffsetIndex, PrimitiveArray, PrimitiveNumeric, VarlenArray},
    datatype::DataType,
    scalar::ScalarValue,
};
use rayexec_error::{RayexecError, Result};

// TODO: This is woefully incomplete.
pub fn cast(arr: Array, to: DataType) -> Result<Array> {
    if arr.datatype() == to {
        return Ok(arr);
    }

    Ok(match (arr, to) {
        // Nulls casted to anything always returns nulls.
        (Array::Null(arr), _) => Array::Null(arr),
        (Array::Utf8(arr), to) if to.is_numeric() => utf8_array_to_numeric(arr, to)?,
        (Array::LargeUtf8(arr), to) if to.is_numeric() => utf8_array_to_numeric(arr, to)?,
        (arr, to) => {
            return Err(RayexecError::new(format!(
                "Unhandled cast for array of type {:?} to {:?}",
                arr.datatype(),
                to
            )))
        }
    })
}

pub fn cast_scalar(scalar: ScalarValue, to: DataType) -> Result<ScalarValue> {
    if scalar.datatype() == to {
        return Ok(scalar);
    }

    Ok(match (scalar, to) {
        (ScalarValue::Null, _) => ScalarValue::Null,
        (ScalarValue::Utf8(val), to) if to.is_numeric() => utf8_scalar_to_numeric(val, to)?,
        (ScalarValue::LargeUtf8(val), to) if to.is_numeric() => utf8_scalar_to_numeric(val, to)?,
        (arr, to) => {
            return Err(RayexecError::new(format!(
                "Unhandled cast for scalar of type {:?} to {:?}",
                arr.datatype(),
                to
            )))
        }
    })
}

fn utf8_scalar_to_numeric(val: impl AsRef<str>, to: DataType) -> Result<ScalarValue<'static>> {
    fn inner<T>(val: impl AsRef<str>) -> Result<T>
    where
        T: PrimitiveNumeric,
    {
        let val = val.as_ref();
        T::from_str(val)
            .ok_or_else(|| RayexecError::new(format!("Unable to cast '{val}' to a number")))
    }

    Ok(match to {
        DataType::Float32 => ScalarValue::Float32(inner(val)?),
        DataType::Float64 => ScalarValue::Float64(inner(val)?),
        DataType::Int8 => ScalarValue::Int8(inner(val)?),
        DataType::Int16 => ScalarValue::Int16(inner(val)?),
        DataType::Int32 => ScalarValue::Int32(inner(val)?),
        DataType::Int64 => ScalarValue::Int64(inner(val)?),
        DataType::UInt8 => ScalarValue::UInt8(inner(val)?),
        DataType::UInt16 => ScalarValue::UInt16(inner(val)?),
        DataType::UInt32 => ScalarValue::UInt32(inner(val)?),
        DataType::UInt64 => ScalarValue::UInt64(inner(val)?),
        other => {
            return Err(RayexecError::new(format!(
                "Data type {other:?} is not numeric"
            )))
        }
    })
}

/// Cast an array of utf strings to a numeric array.
fn utf8_array_to_numeric<O>(arr: VarlenArray<str, O>, to: DataType) -> Result<Array>
where
    O: OffsetIndex,
{
    fn inner<T, O>(arr: VarlenArray<str, O>) -> Result<PrimitiveArray<T>>
    where
        T: PrimitiveNumeric,
        O: OffsetIndex,
    {
        let mut values: Vec<T> = Vec::with_capacity(arr.len());
        for val in arr.values_iter() {
            // TODO: If null...
            // TODO: Allow ignoring error? (implicit cast to null?)

            let val = T::from_str(val)
                .ok_or_else(|| RayexecError::new(format!("Unable to cast '{val}' to a number")))?;

            values.push(val);
        }

        // TODO: Nulls

        Ok(PrimitiveArray::from(values))
    }

    Ok(match to {
        DataType::Float32 => Array::Float32(inner(arr)?),
        DataType::Float64 => Array::Float64(inner(arr)?),
        DataType::Int8 => Array::Int8(inner(arr)?),
        DataType::Int16 => Array::Int16(inner(arr)?),
        DataType::Int32 => Array::Int32(inner(arr)?),
        DataType::Int64 => Array::Int64(inner(arr)?),
        DataType::UInt8 => Array::UInt8(inner(arr)?),
        DataType::UInt16 => Array::UInt16(inner(arr)?),
        DataType::UInt32 => Array::UInt32(inner(arr)?),
        DataType::UInt64 => Array::UInt64(inner(arr)?),
        other => {
            return Err(RayexecError::new(format!(
                "Data type {other:?} is not numeric"
            )))
        }
    })
}

#[cfg(test)]
mod tests {
    use crate::array::{Float32Array, Int32Array, Utf8Array};

    use super::*;

    #[test]
    fn utf8_arr_to_float() {
        let arr = Array::Utf8(Utf8Array::from_iter(["1", "2.1", "3.5"]));
        let out = cast(arr.into(), DataType::Float32).unwrap();

        assert_eq!(
            Array::Float32(Float32Array::from_iter([1.0, 2.1, 3.5])),
            out
        )
    }

    #[test]
    fn utf8_arr_to_int() {
        let arr = Array::Utf8(Utf8Array::from_iter(["1", "2", "3"]));
        let out = cast(arr.into(), DataType::Int32).unwrap();

        assert_eq!(Array::Int32(Int32Array::from_iter([1, 2, 3])), out)
    }
}
