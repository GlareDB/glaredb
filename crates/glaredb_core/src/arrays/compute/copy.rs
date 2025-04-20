use glaredb_error::{Result, not_implemented};

use crate::arrays::array::Array;
use crate::arrays::array::array_buffer::{AnyArrayBuffer, ArrayBuffer2};
use crate::arrays::array::execution_format::ExecutionFormat;
use crate::arrays::array::physical_type::{
    Addressable,
    AddressableMut,
    MutableScalarStorage,
    PhysicalBinary,
    PhysicalBool,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI8,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI128,
    PhysicalInterval,
    PhysicalType,
    PhysicalU8,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU128,
    PhysicalUntypedNull,
    PhysicalUtf8,
};
use crate::arrays::array::validity::Validity;

/// Copy rows from `src` to `dest` using mapping providing (from, to) indices.
pub fn copy_rows_array(
    src: &Array,
    mapping: impl IntoIterator<Item = (usize, usize)>,
    dest: &mut Array,
) -> Result<()> {
    let phys_type = src.datatype().physical_type();
    copy_rows_raw(
        phys_type,
        &src.data,
        &src.validity,
        mapping,
        &mut dest.data,
        &mut dest.validity,
    )
}

/// Copy rows from a src buf/validity mask to a destination buf/validity.
///
/// `phys_type` is the physical type for both the source and destination
/// buffers.
///
/// `mapping` provides (from, to) pairs to determine how copy rows from source
/// to destination.
///
/// An optional source selection can be provided which applies to `src_buf`
///
/// This is exposed to allow shared copy implementations between `Array`s and
/// array-type collections like buffers in `ColumnarCollection`.
pub(crate) fn copy_rows_raw(
    phys_type: PhysicalType,
    src_buf: &AnyArrayBuffer,
    src_validity: &Validity,
    mapping: impl IntoIterator<Item = (usize, usize)>,
    dest_buf: &mut AnyArrayBuffer,
    dest_validity: &mut Validity,
) -> Result<()> {
    match phys_type {
        PhysicalType::UntypedNull => copy_rows_scalar::<PhysicalUntypedNull>(
            src_buf,
            src_validity,
            mapping,
            dest_buf,
            dest_validity,
        ),
        PhysicalType::Boolean => copy_rows_scalar::<PhysicalBool>(
            src_buf,
            src_validity,
            mapping,
            dest_buf,
            dest_validity,
        ),
        PhysicalType::Int8 => {
            copy_rows_scalar::<PhysicalI8>(src_buf, src_validity, mapping, dest_buf, dest_validity)
        }
        PhysicalType::Int16 => {
            copy_rows_scalar::<PhysicalI16>(src_buf, src_validity, mapping, dest_buf, dest_validity)
        }
        PhysicalType::Int32 => {
            copy_rows_scalar::<PhysicalI32>(src_buf, src_validity, mapping, dest_buf, dest_validity)
        }
        PhysicalType::Int64 => {
            copy_rows_scalar::<PhysicalI64>(src_buf, src_validity, mapping, dest_buf, dest_validity)
        }
        PhysicalType::Int128 => copy_rows_scalar::<PhysicalI128>(
            src_buf,
            src_validity,
            mapping,
            dest_buf,
            dest_validity,
        ),
        PhysicalType::UInt8 => {
            copy_rows_scalar::<PhysicalU8>(src_buf, src_validity, mapping, dest_buf, dest_validity)
        }
        PhysicalType::UInt16 => {
            copy_rows_scalar::<PhysicalU16>(src_buf, src_validity, mapping, dest_buf, dest_validity)
        }
        PhysicalType::UInt32 => {
            copy_rows_scalar::<PhysicalU32>(src_buf, src_validity, mapping, dest_buf, dest_validity)
        }
        PhysicalType::UInt64 => {
            copy_rows_scalar::<PhysicalU64>(src_buf, src_validity, mapping, dest_buf, dest_validity)
        }
        PhysicalType::UInt128 => copy_rows_scalar::<PhysicalU128>(
            src_buf,
            src_validity,
            mapping,
            dest_buf,
            dest_validity,
        ),
        PhysicalType::Float16 => {
            copy_rows_scalar::<PhysicalF16>(src_buf, src_validity, mapping, dest_buf, dest_validity)
        }
        PhysicalType::Float32 => {
            copy_rows_scalar::<PhysicalF32>(src_buf, src_validity, mapping, dest_buf, dest_validity)
        }
        PhysicalType::Float64 => {
            copy_rows_scalar::<PhysicalF64>(src_buf, src_validity, mapping, dest_buf, dest_validity)
        }
        PhysicalType::Interval => copy_rows_scalar::<PhysicalInterval>(
            src_buf,
            src_validity,
            mapping,
            dest_buf,
            dest_validity,
        ),
        PhysicalType::Utf8 => copy_rows_scalar::<PhysicalUtf8>(
            src_buf,
            src_validity,
            mapping,
            dest_buf,
            dest_validity,
        ),
        PhysicalType::Binary => copy_rows_scalar::<PhysicalBinary>(
            src_buf,
            src_validity,
            mapping,
            dest_buf,
            dest_validity,
        ),
        other => not_implemented!("copy rows raw: {other}"),
    }
}

fn copy_rows_scalar<S>(
    src_buf: &AnyArrayBuffer,
    src_validity: &Validity,
    mapping: impl IntoIterator<Item = (usize, usize)>,
    dest_buf: &mut AnyArrayBuffer,
    dest_validity: &mut Validity,
) -> Result<()>
where
    S: MutableScalarStorage,
{
    let mut dest_buf = S::get_addressable_mut(dest_buf)?;
    match S::downcast_execution_format(src_buf)? {
        ExecutionFormat::Flat(src) => {
            let src_buf = S::addressable(src);

            if src_validity.all_valid() {
                for (src_idx, dest_idx) in mapping {
                    let v = src_buf.get(src_idx).unwrap();
                    dest_buf.put(dest_idx, v);
                }
            } else {
                for (src_idx, dest_idx) in mapping {
                    if src_validity.is_valid(src_idx) {
                        let v = src_buf.get(src_idx).unwrap();
                        dest_buf.put(dest_idx, v);
                    } else {
                        dest_validity.set_invalid(dest_idx);
                    }
                }
            }

            Ok(())
        }
        ExecutionFormat::Selection(src) => {
            let src_buf = S::addressable(src.buffer);

            if src_validity.all_valid() {
                for (src_idx, dest_idx) in mapping {
                    let sel_idx = src.selection.get(src_idx).unwrap();
                    let v = src_buf.get(sel_idx).unwrap();
                    dest_buf.put(dest_idx, v);
                }
            } else {
                for (src_idx, dest_idx) in mapping {
                    if src_validity.is_valid(src_idx) {
                        let sel_idx = src.selection.get(src_idx).unwrap();
                        let v = src_buf.get(sel_idx).unwrap();
                        dest_buf.put(dest_idx, v);
                    } else {
                        dest_validity.set_invalid(dest_idx);
                    }
                }
            }

            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_manager::DefaultBufferManager;
    use crate::testutil::arrays::assert_arrays_eq;
    use crate::util::iter::TryFromExactSizeIterator;

    #[test]
    fn copy_rows_simple() {
        let from = Array::try_from_iter(["a", "b", "c"]).unwrap();
        let mut to = Array::try_from_iter(["d", "d", "d"]).unwrap();

        copy_rows_array(&from, [(0, 1), (1, 2)], &mut to).unwrap();

        let expected = Array::try_from_iter(["d", "a", "b"]).unwrap();
        assert_arrays_eq(&expected, &to);
    }

    #[test]
    fn copy_rows_from_dict() {
        let mut from = Array::try_from_iter(["a", "b", "c"]).unwrap();
        // => '["b", "a", "c"]
        from.select(&DefaultBufferManager, [1, 0, 2]).unwrap();

        let mut to = Array::try_from_iter(["d", "d", "d"]).unwrap();

        copy_rows_array(&from, [(0, 1), (1, 2)], &mut to).unwrap();

        let expected = Array::try_from_iter(["d", "b", "a"]).unwrap();
        assert_arrays_eq(&expected, &to);
    }

    #[test]
    fn copy_rows_from_dict_invalid() {
        let mut from = Array::try_from_iter([Some("a"), None, Some("c")]).unwrap();
        // => '[NULL, "a", "c"]
        from.select(&DefaultBufferManager, [1, 0, 2]).unwrap();

        let mut to = Array::try_from_iter(["d", "d", "d"]).unwrap();

        copy_rows_array(&from, [(0, 2), (1, 1)], &mut to).unwrap();

        let expected = Array::try_from_iter([Some("d"), Some("a"), None]).unwrap();
        assert_arrays_eq(&expected, &to);
    }
}
