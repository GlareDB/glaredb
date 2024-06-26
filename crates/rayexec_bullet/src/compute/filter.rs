use crate::array::{
    Array, BooleanArray, Decimal128Array, Decimal64Array, OffsetIndex, PrimitiveArray, VarlenArray,
    VarlenType,
};
use rayexec_error::{RayexecError, Result};

pub fn filter(arr: &Array, selection: &BooleanArray) -> Result<Array> {
    Ok(match arr {
        Array::Boolean(arr) => Array::Boolean(filter_boolean(arr, selection)?),
        Array::Float32(arr) => Array::Float32(filter_primitive(arr, selection)?),
        Array::Float64(arr) => Array::Float64(filter_primitive(arr, selection)?),
        Array::Int8(arr) => Array::Int8(filter_primitive(arr, selection)?),
        Array::Int16(arr) => Array::Int16(filter_primitive(arr, selection)?),
        Array::Int32(arr) => Array::Int32(filter_primitive(arr, selection)?),
        Array::Int64(arr) => Array::Int64(filter_primitive(arr, selection)?),
        Array::UInt8(arr) => Array::UInt8(filter_primitive(arr, selection)?),
        Array::UInt16(arr) => Array::UInt16(filter_primitive(arr, selection)?),
        Array::UInt32(arr) => Array::UInt32(filter_primitive(arr, selection)?),
        Array::UInt64(arr) => Array::UInt64(filter_primitive(arr, selection)?),
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
        Array::Utf8(arr) => Array::Utf8(filter_varlen(arr, selection)?),
        Array::LargeUtf8(arr) => Array::LargeUtf8(filter_varlen(arr, selection)?),
        Array::Binary(arr) => Array::Binary(filter_varlen(arr, selection)?),
        Array::LargeBinary(arr) => Array::LargeBinary(filter_varlen(arr, selection)?),
        other => unimplemented!("{}", other.datatype()), // TODO
    })
}

pub fn filter_boolean(arr: &BooleanArray, selection: &BooleanArray) -> Result<BooleanArray> {
    if arr.len() != selection.len() {
        return Err(RayexecError::new(
            "Selection array length doesn't equal array length",
        ));
    }

    // TODO: validity

    let values_iter = arr.values().iter();
    let select_iter = selection.values().iter();

    let iter = values_iter
        .zip(select_iter)
        .filter_map(|(v, take)| if take { Some(v) } else { None });

    Ok(BooleanArray::from_iter(iter))
}

pub fn filter_primitive<T: Copy>(
    arr: &PrimitiveArray<T>,
    selection: &BooleanArray,
) -> Result<PrimitiveArray<T>> {
    if arr.len() != selection.len() {
        return Err(RayexecError::new(
            "Selection array length doesn't equal array length",
        ));
    }

    // TODO: validity

    let values_iter = arr.values().as_ref().iter();
    let select_iter = selection.values().iter();

    let iter = values_iter
        .zip(select_iter)
        .filter_map(|(v, take)| if take { Some(*v) } else { None });

    let arr = PrimitiveArray::from_iter(iter);

    Ok(arr)
}

pub fn filter_varlen<T: VarlenType + ?Sized, O: OffsetIndex>(
    arr: &VarlenArray<T, O>,
    selection: &BooleanArray,
) -> Result<VarlenArray<T, O>> {
    if arr.len() != selection.len() {
        return Err(RayexecError::new(
            "Selection array length doesn't equal array length",
        ));
    }

    // TODO: Validity

    let values_iter = arr.values_iter();
    let select_iter = selection.values().iter();

    let iter = values_iter
        .zip(select_iter)
        .filter_map(|(v, take)| if take { Some(v) } else { None });

    let arr = VarlenArray::from_iter(iter);

    Ok(arr)
}

#[cfg(test)]
mod tests {
    use crate::array::{Int32Array, Utf8Array};

    use super::*;

    #[test]
    fn simple_filter_primitive() {
        let arr = Int32Array::from_iter([6, 7, 8, 9]);
        let selection = BooleanArray::from_iter([true, false, true, false]);

        let filtered = filter_primitive(&arr, &selection).unwrap();
        let expected = Int32Array::from_iter([6, 8]);
        assert_eq!(expected, filtered);
    }

    #[test]
    fn simple_filter_varlen() {
        let arr = Utf8Array::from_iter(["aaa", "bbb", "ccc", "ddd"]);
        let selection = BooleanArray::from_iter([true, false, true, false]);

        let filtered = filter_varlen(&arr, &selection).unwrap();
        let expected = Utf8Array::from_iter(["aaa", "ccc"]);
        assert_eq!(expected, filtered);
    }
}
