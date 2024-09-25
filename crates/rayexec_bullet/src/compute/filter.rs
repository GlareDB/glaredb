use crate::{
    array::{
        Array, BooleanArray, Decimal128Array, Decimal64Array, NullArray, OffsetIndex,
        PrimitiveArray, TimestampArray, VarlenArray, VarlenType, VarlenValuesBuffer,
    },
    bitmap::Bitmap,
};
use rayexec_error::{not_implemented, RayexecError, Result};

use super::util::IntoExtactSizeIterator;

/// A trait for determining which rows should be selected during a filter.
///
/// This implements `Copy`, as the iterator needs to be ran twice, once for the
/// values and once for the validity. The `Copy` essentially enforces that we
/// only pass in a reference which we can use to create two separate iterators
/// for the same data.
pub trait FilterSelection: IntoExtactSizeIterator<Item = bool> + Copy {
    /// Returns the exact size of the iterator that will be created after a call
    /// to `into_iter`.
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Bitmap can be used to filter.
impl FilterSelection for &Bitmap {
    fn len(&self) -> usize {
        Bitmap::len(self)
    }
}

// TODO: Probably make this just accept an iterator.
pub fn filter(arr: &Array, selection: impl FilterSelection) -> Result<Array> {
    Ok(match arr {
        Array::Null(_) => {
            let len = selection.into_iter().filter(|&b| b).count();
            Array::Null(NullArray::new(len))
        }
        Array::Boolean(arr) => Array::Boolean(filter_boolean(arr, selection)?),
        Array::Float32(arr) => Array::Float32(filter_primitive(arr, selection)?),
        Array::Float64(arr) => Array::Float64(filter_primitive(arr, selection)?),
        Array::Int8(arr) => Array::Int8(filter_primitive(arr, selection)?),
        Array::Int16(arr) => Array::Int16(filter_primitive(arr, selection)?),
        Array::Int32(arr) => Array::Int32(filter_primitive(arr, selection)?),
        Array::Int64(arr) => Array::Int64(filter_primitive(arr, selection)?),
        Array::Int128(arr) => Array::Int128(filter_primitive(arr, selection)?),
        Array::UInt8(arr) => Array::UInt8(filter_primitive(arr, selection)?),
        Array::UInt16(arr) => Array::UInt16(filter_primitive(arr, selection)?),
        Array::UInt32(arr) => Array::UInt32(filter_primitive(arr, selection)?),
        Array::UInt64(arr) => Array::UInt64(filter_primitive(arr, selection)?),
        Array::UInt128(arr) => Array::UInt128(filter_primitive(arr, selection)?),
        Array::Decimal64(arr) => {
            let primitive = filter_primitive(arr.get_primitive(), selection)?;
            Array::Decimal64(Decimal64Array::new(arr.precision(), arr.scale(), primitive))
        }
        Array::Decimal128(arr) => {
            let primitive = filter_primitive(arr.get_primitive(), selection)?;
            Array::Decimal128(Decimal128Array::new(
                arr.precision(),
                arr.scale(),
                primitive,
            ))
        }
        Array::Date32(arr) => Array::Date32(filter_primitive(arr, selection)?),
        Array::Date64(arr) => Array::Date64(filter_primitive(arr, selection)?),
        Array::Timestamp(arr) => {
            let primitive = filter_primitive(arr.get_primitive(), selection)?;
            Array::Timestamp(TimestampArray::new(arr.unit(), primitive))
        }
        Array::Interval(arr) => Array::Interval(filter_primitive(arr, selection)?),
        Array::Utf8(arr) => Array::Utf8(filter_varlen(arr, selection)?),
        Array::LargeUtf8(arr) => Array::LargeUtf8(filter_varlen(arr, selection)?),
        Array::Binary(arr) => Array::Binary(filter_varlen(arr, selection)?),
        Array::LargeBinary(arr) => Array::LargeBinary(filter_varlen(arr, selection)?),
        Array::List(_) => not_implemented!("list filter"),
        Array::Struct(_) => not_implemented!("struct filter"),
    })
}

pub fn filter_boolean(arr: &BooleanArray, selection: impl FilterSelection) -> Result<BooleanArray> {
    if arr.len() != selection.len() {
        return Err(RayexecError::new(format!(
            "Selection array length doesn't equal array length, got {}, want {}",
            selection.len(),
            arr.len()
        )));
    }

    let values_iter = arr.values().iter();

    let values: Bitmap = values_iter
        .zip(selection.into_iter())
        .filter_map(|(v, take)| if take { Some(v) } else { None })
        .collect();

    let validity = filter_validity(arr.validity(), selection);

    Ok(BooleanArray::new(values, validity))
}

pub fn filter_primitive<T: Copy>(
    arr: &PrimitiveArray<T>,
    selection: impl FilterSelection,
) -> Result<PrimitiveArray<T>> {
    if arr.len() != selection.len() {
        return Err(RayexecError::new(format!(
            "Selection array length doesn't equal array length, got {}, want {}",
            selection.len(),
            arr.len()
        )));
    }

    let values_iter = arr.values().as_ref().iter();

    let values: Vec<_> = values_iter
        .zip(selection.into_iter())
        .filter_map(|(v, take)| if take { Some(*v) } else { None })
        .collect();

    let validity = filter_validity(arr.validity(), selection);

    let arr = PrimitiveArray::new(values, validity);

    Ok(arr)
}

pub fn filter_varlen<T: VarlenType + ?Sized, O: OffsetIndex>(
    arr: &VarlenArray<T, O>,
    selection: impl FilterSelection,
) -> Result<VarlenArray<T, O>> {
    if arr.len() != selection.len() {
        return Err(RayexecError::new(format!(
            "Selection array length doesn't equal array length, got {}, want {}",
            selection.len(),
            arr.len()
        )));
    }

    let values_iter = arr.values_iter();

    let values: VarlenValuesBuffer<O> = values_iter
        .zip(selection.into_iter())
        .filter_map(|(v, take)| if take { Some(v) } else { None })
        .collect();

    let validity = filter_validity(arr.validity(), selection);

    let arr = VarlenArray::new(values, validity);

    Ok(arr)
}

fn filter_validity(validity: Option<&Bitmap>, selection: impl FilterSelection) -> Option<Bitmap> {
    validity.map(|validity| {
        validity
            .iter()
            .zip(selection.into_iter())
            .filter_map(|(v, take)| if take { Some(v) } else { None })
            .collect()
    })
}

#[cfg(test)]
mod tests {
    use crate::array::{Int32Array, Utf8Array};

    use super::*;

    #[test]
    fn simple_filter_primitive() {
        let arr = Int32Array::from_iter([6, 7, 8, 9]);
        let selection = Bitmap::from_iter([true, false, true, false]);

        let filtered = filter_primitive(&arr, &selection).unwrap();
        let expected = Int32Array::from_iter([6, 8]);
        assert_eq!(expected, filtered);
    }

    #[test]
    fn simple_filter_varlen() {
        let arr = Utf8Array::from_iter(["aaa", "bbb", "ccc", "ddd"]);
        let selection = Bitmap::from_iter([true, false, true, false]);

        let filtered = filter_varlen(&arr, &selection).unwrap();
        let expected = Utf8Array::from_iter(["aaa", "ccc"]);
        assert_eq!(expected, filtered);
    }

    #[test]
    fn filter_primitive_with_nulls() {
        let arr = Int32Array::from_iter([Some(6), Some(7), None, None]);
        let selection = Bitmap::from_iter([true, false, true, false]);

        let filtered = filter_primitive(&arr, &selection).unwrap();
        let expected = Int32Array::from_iter([Some(6), None]);
        assert_eq!(expected, filtered);
    }
}
