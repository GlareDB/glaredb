use rayexec_error::{RayexecError, Result};

use crate::arrays::array::array_buffer::ArrayBuffer;
use crate::arrays::array::physical_type::{
    Addressable,
    AddressableMut,
    MutableScalarStorage,
    PhysicalBinary,
    PhysicalBool,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalInterval,
    PhysicalType,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUntypedNull,
    PhysicalUtf8,
};
use crate::arrays::array::selection::Selection;
use crate::arrays::array::validity::Validity;
use crate::arrays::array::Array;
use crate::buffer::buffer_manager::BufferManager;

/// Copy rows from `src` to `dest` using mapping providing (from, to) indices.
pub fn copy_rows_array(
    src: &Array,
    mapping: impl IntoIterator<Item = (usize, usize)>,
    dest: &mut Array,
) -> Result<()> {
    if dest.should_flatten_for_execution() {
        return Err(RayexecError::new(
            "Cannot copy to array that needs to be flattened",
        ));
    }

    let phys_type = src.datatype().physical_type();

    if src.should_flatten_for_execution() {
        let src = src.flatten()?;
        copy_rows_raw(
            phys_type,
            src.array_buffer,
            src.validity,
            Some(src.selection),
            mapping,
            &mut dest.data,
            &mut dest.validity,
        )
    } else {
        copy_rows_raw(
            phys_type,
            &src.data,
            &src.validity,
            None,
            mapping,
            &mut dest.data,
            &mut dest.validity,
        )
    }
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
pub(in crate::arrays) fn copy_rows_raw(
    phys_type: PhysicalType,
    src_buf: &ArrayBuffer,
    src_validity: &Validity,
    src_sel: Option<Selection>,
    mapping: impl IntoIterator<Item = (usize, usize)>,
    dest_buf: &mut ArrayBuffer,
    dest_validity: &mut Validity,
) -> Result<()> {
    match phys_type {
        PhysicalType::UntypedNull => copy_rows_scalar::<PhysicalUntypedNull>(
            src_buf,
            src_validity,
            src_sel,
            mapping,
            dest_buf,
            dest_validity,
        ),
        PhysicalType::Boolean => copy_rows_scalar::<PhysicalBool>(
            src_buf,
            src_validity,
            src_sel,
            mapping,
            dest_buf,
            dest_validity,
        ),
        PhysicalType::Int8 => copy_rows_scalar::<PhysicalI8>(
            src_buf,
            src_validity,
            src_sel,
            mapping,
            dest_buf,
            dest_validity,
        ),
        PhysicalType::Int16 => copy_rows_scalar::<PhysicalI16>(
            src_buf,
            src_validity,
            src_sel,
            mapping,
            dest_buf,
            dest_validity,
        ),
        PhysicalType::Int32 => copy_rows_scalar::<PhysicalI32>(
            src_buf,
            src_validity,
            src_sel,
            mapping,
            dest_buf,
            dest_validity,
        ),
        PhysicalType::Int64 => copy_rows_scalar::<PhysicalI64>(
            src_buf,
            src_validity,
            src_sel,
            mapping,
            dest_buf,
            dest_validity,
        ),
        PhysicalType::Int128 => copy_rows_scalar::<PhysicalI128>(
            src_buf,
            src_validity,
            src_sel,
            mapping,
            dest_buf,
            dest_validity,
        ),
        PhysicalType::UInt8 => copy_rows_scalar::<PhysicalU8>(
            src_buf,
            src_validity,
            src_sel,
            mapping,
            dest_buf,
            dest_validity,
        ),
        PhysicalType::UInt16 => copy_rows_scalar::<PhysicalU16>(
            src_buf,
            src_validity,
            src_sel,
            mapping,
            dest_buf,
            dest_validity,
        ),
        PhysicalType::UInt32 => copy_rows_scalar::<PhysicalU32>(
            src_buf,
            src_validity,
            src_sel,
            mapping,
            dest_buf,
            dest_validity,
        ),
        PhysicalType::UInt64 => copy_rows_scalar::<PhysicalU64>(
            src_buf,
            src_validity,
            src_sel,
            mapping,
            dest_buf,
            dest_validity,
        ),
        PhysicalType::UInt128 => copy_rows_scalar::<PhysicalU128>(
            src_buf,
            src_validity,
            src_sel,
            mapping,
            dest_buf,
            dest_validity,
        ),
        PhysicalType::Float16 => copy_rows_scalar::<PhysicalF16>(
            src_buf,
            src_validity,
            src_sel,
            mapping,
            dest_buf,
            dest_validity,
        ),
        PhysicalType::Float32 => copy_rows_scalar::<PhysicalF32>(
            src_buf,
            src_validity,
            src_sel,
            mapping,
            dest_buf,
            dest_validity,
        ),
        PhysicalType::Float64 => copy_rows_scalar::<PhysicalF64>(
            src_buf,
            src_validity,
            src_sel,
            mapping,
            dest_buf,
            dest_validity,
        ),
        PhysicalType::Interval => copy_rows_scalar::<PhysicalInterval>(
            src_buf,
            src_validity,
            src_sel,
            mapping,
            dest_buf,
            dest_validity,
        ),
        PhysicalType::Utf8 => copy_rows_scalar::<PhysicalUtf8>(
            src_buf,
            src_validity,
            src_sel,
            mapping,
            dest_buf,
            dest_validity,
        ),
        PhysicalType::Binary => copy_rows_scalar::<PhysicalBinary>(
            src_buf,
            src_validity,
            src_sel,
            mapping,
            dest_buf,
            dest_validity,
        ),
        _ => unimplemented!(),
    }
}

fn copy_rows_scalar<S>(
    src_buf: &ArrayBuffer,
    src_validity: &Validity,
    src_sel: Option<Selection>,
    mapping: impl IntoIterator<Item = (usize, usize)>,
    dest_buf: &mut ArrayBuffer,
    dest_validity: &mut Validity,
) -> Result<()>
where
    S: MutableScalarStorage,
{
    let src_buf = S::get_addressable(src_buf)?;
    let mut dest_buf = S::get_addressable_mut(dest_buf)?;

    match src_sel {
        Some(src_sel) => {
            if src_validity.all_valid() {
                for (src_idx, dest_idx) in mapping {
                    let sel_idx = src_sel.get(src_idx).unwrap();
                    let v = src_buf.get(sel_idx).unwrap();
                    dest_buf.put(dest_idx, v);
                }
            } else {
                for (src_idx, dest_idx) in mapping {
                    if src_validity.is_valid(src_idx) {
                        let sel_idx = src_sel.get(src_idx).unwrap();
                        let v = src_buf.get(sel_idx).unwrap();
                        dest_buf.put(dest_idx, v);
                    } else {
                        dest_validity.set_invalid(dest_idx);
                    }
                }
            }
        }
        None => {
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
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::buffer::buffer_manager::NopBufferManager;
    use crate::testutil::arrays::assert_arrays_eq;

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
        from.select(&NopBufferManager, [1, 0, 2]).unwrap();

        let mut to = Array::try_from_iter(["d", "d", "d"]).unwrap();

        copy_rows_array(&from, [(0, 1), (1, 2)], &mut to).unwrap();

        let expected = Array::try_from_iter(["d", "b", "a"]).unwrap();
        assert_arrays_eq(&expected, &to);
    }

    #[test]
    fn copy_rows_from_dict_invalid() {
        let mut from = Array::try_from_iter([Some("a"), None, Some("c")]).unwrap();
        // => '[NULL, "a", "c"]
        from.select(&NopBufferManager, [1, 0, 2]).unwrap();

        let mut to = Array::try_from_iter(["d", "d", "d"]).unwrap();

        copy_rows_array(&from, [(0, 2), (1, 1)], &mut to).unwrap();

        let expected = Array::try_from_iter([Some("d"), Some("a"), None]).unwrap();
        assert_arrays_eq(&expected, &to);
    }
}
