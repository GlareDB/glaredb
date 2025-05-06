use glaredb_error::{Result, not_implemented};

use crate::arrays::array::Array;
use crate::arrays::array::array_buffer::{ArrayBufferDowncast, ListBuffer};
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
use crate::util::iter::IntoExactSizeIterator;

/// Extract an element from each list within a list array.
///
/// If the element index falls outside the bounds of a list, the result for that
/// row will be NULL.
///
/// If the element within the list for a given row is NULL, then the output row
/// will be NULL.
pub fn list_extract(
    list: &Array,
    sel: impl IntoExactSizeIterator<Item = usize> + Clone,
    output: &mut Array,
    elem_idx: usize,
) -> Result<()> {
    match output.datatype().physical_type()? {
        PhysicalType::UntypedNull => {
            list_extract_scalar::<PhysicalUntypedNull>(list, sel, output, elem_idx)
        }
        PhysicalType::Boolean => list_extract_scalar::<PhysicalBool>(list, sel, output, elem_idx),
        PhysicalType::Int8 => list_extract_scalar::<PhysicalI8>(list, sel, output, elem_idx),
        PhysicalType::Int16 => list_extract_scalar::<PhysicalI16>(list, sel, output, elem_idx),
        PhysicalType::Int32 => list_extract_scalar::<PhysicalI32>(list, sel, output, elem_idx),
        PhysicalType::Int64 => list_extract_scalar::<PhysicalI64>(list, sel, output, elem_idx),
        PhysicalType::Int128 => list_extract_scalar::<PhysicalI128>(list, sel, output, elem_idx),
        PhysicalType::UInt8 => list_extract_scalar::<PhysicalU8>(list, sel, output, elem_idx),
        PhysicalType::UInt16 => list_extract_scalar::<PhysicalU16>(list, sel, output, elem_idx),
        PhysicalType::UInt32 => list_extract_scalar::<PhysicalU32>(list, sel, output, elem_idx),
        PhysicalType::UInt64 => list_extract_scalar::<PhysicalU64>(list, sel, output, elem_idx),
        PhysicalType::UInt128 => list_extract_scalar::<PhysicalU128>(list, sel, output, elem_idx),
        PhysicalType::Float16 => list_extract_scalar::<PhysicalF16>(list, sel, output, elem_idx),
        PhysicalType::Float32 => list_extract_scalar::<PhysicalF32>(list, sel, output, elem_idx),
        PhysicalType::Float64 => list_extract_scalar::<PhysicalF64>(list, sel, output, elem_idx),
        PhysicalType::Interval => {
            list_extract_scalar::<PhysicalInterval>(list, sel, output, elem_idx)
        }
        PhysicalType::Utf8 => list_extract_scalar::<PhysicalUtf8>(list, sel, output, elem_idx),
        PhysicalType::Binary => list_extract_scalar::<PhysicalBinary>(list, sel, output, elem_idx),
        other => not_implemented!("List extract for datatype {other}"),
    }
}

fn list_extract_scalar<S: MutableScalarStorage>(
    list: &Array,
    sel: impl IntoExactSizeIterator<Item = usize> + Clone,
    output: &mut Array,
    elem_idx: usize,
) -> Result<()> {
    let sel = sel.into_exact_size_iter();
    let output_len = sel.len();
    output.validity = Validity::new_all_valid(output_len);

    let list_buffer = ListBuffer::downcast_execution_format(&list.data)?.into_selection_format()?;
    let metadata = list_buffer.buffer.metadata.as_slice();
    let child_validity = &list_buffer.buffer.child.validity;
    let child_buffer = S::buffer_downcast_ref(&list_buffer.buffer.child.buffer)?;
    let child_addr = S::addressable(child_buffer);

    let output_buffer = S::buffer_downcast_mut(&mut output.data)?;
    let mut output_addr = S::addressable_mut(output_buffer);

    for (output_idx, input_idx) in sel.enumerate() {
        if !list.validity.is_valid(input_idx) {
            // List value at this index is NULL, output is NULL.
            output.validity.set_invalid(output_idx);
            continue;
        }

        let sel_idx = list_buffer.selection.get(input_idx).unwrap();
        let metadata = metadata[sel_idx];

        if metadata.len as usize <= elem_idx {
            // List doesn't have enough elements in it. Output is NULL.
            //
            // E.g. trying in to extract index 5 from '[1,2,3]'.
            output.validity.set_invalid(output_idx);
            continue;
        }

        let phys_idx = (metadata.offset as usize) + elem_idx;

        if !child_validity.is_valid(phys_idx) {
            // Element within the list is NULL, output is NULL.
            output.validity.set_invalid(output_idx);
            continue;
        }

        // Finally we can read the actual value.
        let val = child_addr.get(phys_idx).unwrap();
        output_addr.put(output_idx, val);
    }

    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::arrays::compute::make_list::make_list;
    use crate::arrays::datatype::DataType;
    use crate::buffer::buffer_manager::DefaultBufferManager;
    use crate::testutil::arrays::assert_arrays_eq;
    use crate::util::iter::TryFromExactSizeIterator;

    #[test]
    fn list_extract_primitive() {
        let a = Array::try_from_iter([1, 2, 3]).unwrap();
        let b = Array::try_from_iter([4, 5, 6]).unwrap();

        let mut lists =
            Array::new(&DefaultBufferManager, DataType::list(DataType::int32()), 3).unwrap();

        make_list(&[a, b], 0..3, &mut lists).unwrap();

        let mut second_elements = Array::new(&DefaultBufferManager, DataType::int32(), 3).unwrap();
        list_extract(&lists, 0..3, &mut second_elements, 1).unwrap();

        let expected = Array::try_from_iter([4, 5, 6]).unwrap();
        assert_arrays_eq(&expected, &second_elements);
    }

    #[test]
    fn list_extract_out_of_bounds() {
        let a = Array::try_from_iter([1, 2, 3]).unwrap();
        let b = Array::try_from_iter([4, 5, 6]).unwrap();

        let mut lists =
            Array::new(&DefaultBufferManager, DataType::list(DataType::int32()), 3).unwrap();

        make_list(&[a, b], 0..3, &mut lists).unwrap();

        let mut extracted_elements =
            Array::new(&DefaultBufferManager, DataType::int32(), 3).unwrap();
        list_extract(&lists, 0..3, &mut extracted_elements, 2).unwrap();

        let expected = Array::try_from_iter([None as Option<i32>, None, None]).unwrap();
        assert_arrays_eq(&expected, &extracted_elements);
    }

    #[test]
    fn list_extract_child_invalid() {
        let a = Array::try_from_iter([1, 2, 3]).unwrap();
        let b = Array::try_from_iter([Some(4), None, Some(6)]).unwrap();

        let mut lists =
            Array::new(&DefaultBufferManager, DataType::list(DataType::int32()), 3).unwrap();

        make_list(&[a, b], 0..3, &mut lists).unwrap();

        let mut second_elements = Array::new(&DefaultBufferManager, DataType::int32(), 3).unwrap();
        list_extract(&lists, 0..3, &mut second_elements, 1).unwrap();

        let expected = Array::try_from_iter([Some(4), None, Some(6)]).unwrap();
        assert_arrays_eq(&expected, &second_elements);

        // Elements as index 0 should still be all non-null.
        let mut first_elements = Array::new(&DefaultBufferManager, DataType::int32(), 3).unwrap();
        list_extract(&lists, 0..3, &mut first_elements, 0).unwrap();

        let expected = Array::try_from_iter([1, 2, 3]).unwrap();
        assert_arrays_eq(&expected, &first_elements);
    }

    #[test]
    fn list_extract_parent_invalid() {
        let a = Array::try_from_iter([1, 2, 3]).unwrap();
        let b = Array::try_from_iter([4, 5, 6]).unwrap();

        let mut lists =
            Array::new(&DefaultBufferManager, DataType::list(DataType::int32()), 3).unwrap();

        make_list(&[a, b], 0..3, &mut lists).unwrap();
        lists.validity.set_invalid(1); // [2, 5] => NULL

        let mut second_elements = Array::new(&DefaultBufferManager, DataType::int32(), 3).unwrap();
        list_extract(&lists, 0..3, &mut second_elements, 1).unwrap();

        let expected = Array::try_from_iter([Some(4), None, Some(6)]).unwrap();
        assert_arrays_eq(&expected, &second_elements);
    }
}
