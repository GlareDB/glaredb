use std::borrow::{Borrow, BorrowMut};

use glaredb_error::{Result, not_implemented};
use half::f16;

use super::row_blocks::BlockAppendState;
use crate::arrays::array::Array;
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
    ScalarStorage,
    UntypedNull,
};
use crate::arrays::bitmap::view::{BitmapViewMut, num_bytes_for_bitmap};
use crate::arrays::datatype::DataType;
use crate::arrays::scalar::interval::Interval;
use crate::arrays::string::StringPtr;
use crate::util::iter::IntoExactSizeIterator;

/// Describes the layout of a row for use with a row collection.
///
/// Row layout:
/// [validity_bytes, row_encoded_values]
///
/// The validity bytes are just a bitmap padded out to the nearest byte which
/// each bit indicating the corresponding value's validity.
///
/// The row encoded values are just unaligned writes of the value. Varlen data
/// will have a fixed sized prefix/marker written inline, while the rest is
/// written to a heap block somewhere.
///
/// Row encoding has no comparison properties.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowLayout {
    /// Data types for each column in the row.
    pub(crate) types: Vec<DataType>,
    /// Byte offsets within the encoded row to the start of the value.
    pub(crate) offsets: Vec<usize>,
    /// Size in bytes for the (inline) encoded row. Does not include the size
    /// encoded and pushed to the string heap.
    ///
    /// Includes the validity width.
    pub(crate) row_width: usize,
    /// If the row encoding requires writing parts of the data to a heap.
    pub(crate) requires_heap: bool,
    /// Size in bytes for the validity mask.
    ///
    /// Each value's validity is a single bit.
    pub(crate) validity_width: usize,
}

impl RowLayout {
    pub fn try_new(types: impl IntoIterator<Item = DataType>) -> Result<Self> {
        let types: Vec<_> = types.into_iter().collect();
        let validity_width = num_bytes_for_bitmap(types.len());

        let mut offset = validity_width;
        let mut offsets = Vec::with_capacity(types.len());

        let mut requires_heap = false;

        for typ in &types {
            let phys_type = typ.physical_type()?;
            let width = row_width_for_physical_type(phys_type);
            offsets.push(offset);
            offset += width;

            requires_heap = requires_heap || row_encoding_requires_heap(phys_type);
        }

        Ok(RowLayout {
            types,
            offsets,
            row_width: offset,
            requires_heap,
            validity_width,
        })
    }

    pub fn num_columns(&self) -> usize {
        self.types.len()
    }

    /// Returns the buffer size needed to store `rows` in the encoded row
    /// format.
    ///
    /// This includes the width of each value in a row alongside the bytes
    /// needed for the validity mask;
    pub const fn buffer_size(&self, rows: usize) -> usize {
        self.row_width * rows
    }

    /// Compute the byte offset needed to get a pointer to the colum in the
    /// given row.
    pub fn byte_offset(&self, row: usize, column: usize) -> usize {
        self.row_width * row + self.offsets[column]
    }

    /// Returns the validity buffer for a row.
    ///
    /// # Safety
    ///
    /// `row_ptr` must point the beginning of the row, and the row must conform
    /// to this row layout.
    pub(crate) unsafe fn validity_buffer(&self, row_ptr: *const u8) -> &[u8] {
        unsafe { std::slice::from_raw_parts(row_ptr, self.validity_width) }
    }

    /// Returns a mutable validity buffer for a row.
    ///
    /// # Safety
    ///
    /// Same as `validity_buffer`, and the pointer must not have been
    /// invalidated.
    ///
    /// # Clippy lint
    ///
    /// Clippy thinks we're producing a mut ref from `&self`, but we're not.
    /// We're producing a mut ref from the pointer, and `&self` is just letting
    /// us know how long the slice is.
    #[allow(clippy::mut_from_ref)]
    unsafe fn validity_buffer_mut(&self, row_ptr: *mut u8) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(row_ptr, self.validity_width) }
    }

    /// Computes the heap sizes needed for each row.
    ///
    /// All arrays should be provided so that it matches this row layout.
    ///
    /// `sizes` will initially be set to all zeros.
    pub fn compute_heap_sizes<A>(
        &self,
        arrays: &[A],
        rows: impl IntoExactSizeIterator<Item = usize> + Clone,
        sizes: &mut [usize],
    ) -> Result<()>
    where
        A: Borrow<Array>,
    {
        let num_rows = rows.clone().into_exact_size_iter().len();
        debug_assert_eq!(sizes.len(), num_rows);
        sizes.fill(0); // Reset all sizes initially to zero.

        for array in arrays {
            let rows = rows.clone();

            let array = array.borrow();

            match array.physical_type()? {
                PhysicalType::Binary | PhysicalType::Utf8 => {
                    let buffer = PhysicalBinary::downcast_execution_format(&array.data)?
                        .into_selection_format()?;
                    let metadatas = buffer.buffer.metadata.as_slice();

                    for (output, row) in rows.into_iter().enumerate() {
                        if array.validity.is_valid(row) {
                            let sel = buffer.selection.get(row).unwrap();
                            let view = metadatas[sel];
                            if !view.is_inline() {
                                // Only increase size if we actually need to
                                // write to a heap block.
                                sizes[output] += view.data_len() as usize;
                            }
                        }
                    }
                }
                PhysicalType::Struct => not_implemented!("compute heap sizes for struct"),
                PhysicalType::List => not_implemented!("compute heap sizes for list"),
                _ => (),
            }
        }

        Ok(())
    }

    pub(crate) unsafe fn write_arrays<A>(
        &self,
        state: &mut BlockAppendState,
        arrays: &[A],
        rows: impl IntoExactSizeIterator<Item = usize> + Clone,
    ) -> Result<()>
    where
        A: Borrow<Array>,
    {
        unsafe {
            for (array_idx, array) in arrays.iter().enumerate() {
                let rows = rows.clone();
                let array = array.borrow();
                write_array(
                    self,
                    array.physical_type()?,
                    array_idx,
                    array,
                    &state.row_pointers,
                    &mut state.heap_pointers,
                    rows,
                )?;
            }

            Ok(())
        }
    }

    /// Reads arrays from the provided row pointers.
    ///
    /// The `arrays` iterator indices which columns to scan, and the associate
    /// array to write the result to.
    pub(crate) unsafe fn read_arrays<'a, A>(
        &self,
        row_ptrs: impl IntoIterator<Item = *const u8> + Clone,
        arrays: impl IntoIterator<Item = (usize, &'a mut A)>,
        write_offset: usize,
    ) -> Result<()>
    where
        A: BorrowMut<Array> + 'a,
    {
        unsafe {
            for (array_idx, array) in arrays {
                let array = array.borrow_mut();
                let phys_type = array.physical_type()?;
                read_array(
                    self,
                    phys_type,
                    row_ptrs.clone(),
                    array_idx,
                    array,
                    write_offset,
                )?;
            }

            Ok(())
        }
    }
}

pub(crate) const fn row_encoding_requires_heap(phys_type: PhysicalType) -> bool {
    matches!(
        phys_type,
        PhysicalType::Utf8 | PhysicalType::Binary | PhysicalType::List | PhysicalType::Struct
    )
}

pub(crate) const fn row_width_for_physical_type(phys_type: PhysicalType) -> usize {
    match phys_type {
        PhysicalType::UntypedNull => std::mem::size_of::<UntypedNull>(), // Zero
        PhysicalType::Boolean => std::mem::size_of::<bool>(),
        PhysicalType::Int8 => std::mem::size_of::<i8>(),
        PhysicalType::Int16 => std::mem::size_of::<i16>(),
        PhysicalType::Int32 => std::mem::size_of::<i32>(),
        PhysicalType::Int64 => std::mem::size_of::<i64>(),
        PhysicalType::Int128 => std::mem::size_of::<i128>(),
        PhysicalType::UInt8 => std::mem::size_of::<u8>(),
        PhysicalType::UInt16 => std::mem::size_of::<u16>(),
        PhysicalType::UInt32 => std::mem::size_of::<u32>(),
        PhysicalType::UInt64 => std::mem::size_of::<u64>(),
        PhysicalType::UInt128 => std::mem::size_of::<u128>(),
        PhysicalType::Float16 => std::mem::size_of::<f16>(),
        PhysicalType::Float32 => std::mem::size_of::<f32>(),
        PhysicalType::Float64 => std::mem::size_of::<f64>(),
        PhysicalType::Interval => std::mem::size_of::<Interval>(),
        PhysicalType::Binary => std::mem::size_of::<StringPtr>(),
        PhysicalType::Utf8 => std::mem::size_of::<StringPtr>(),
        PhysicalType::List => 0,   // TODO: Probably metadata
        PhysicalType::Struct => 0, // TODO: Probaby metadata
    }
}

/// Writes an array to each row.
///
/// `rows` selects which rows from the array to write. This must match the
/// length of `row_pointers`. If the type we're writing requires writing to heap
/// blocks, then it should also match the length of `heap_pointers`.
unsafe fn write_array(
    layout: &RowLayout,
    phys_type: PhysicalType,
    array_idx: usize,
    array: &Array,
    row_pointers: &[*mut u8],
    heap_pointers: &mut [*mut u8],
    rows: impl IntoExactSizeIterator<Item = usize>,
) -> Result<()> {
    unsafe {
        match phys_type {
            PhysicalType::UntypedNull => {
                write_scalar::<PhysicalUntypedNull>(layout, array_idx, array, row_pointers, rows)
            }
            PhysicalType::Boolean => {
                write_scalar::<PhysicalBool>(layout, array_idx, array, row_pointers, rows)
            }
            PhysicalType::Int8 => {
                write_scalar::<PhysicalI8>(layout, array_idx, array, row_pointers, rows)
            }
            PhysicalType::Int16 => {
                write_scalar::<PhysicalI16>(layout, array_idx, array, row_pointers, rows)
            }
            PhysicalType::Int32 => {
                write_scalar::<PhysicalI32>(layout, array_idx, array, row_pointers, rows)
            }
            PhysicalType::Int64 => {
                write_scalar::<PhysicalI64>(layout, array_idx, array, row_pointers, rows)
            }
            PhysicalType::Int128 => {
                write_scalar::<PhysicalI128>(layout, array_idx, array, row_pointers, rows)
            }
            PhysicalType::UInt8 => {
                write_scalar::<PhysicalU8>(layout, array_idx, array, row_pointers, rows)
            }
            PhysicalType::UInt16 => {
                write_scalar::<PhysicalU16>(layout, array_idx, array, row_pointers, rows)
            }
            PhysicalType::UInt32 => {
                write_scalar::<PhysicalU32>(layout, array_idx, array, row_pointers, rows)
            }
            PhysicalType::UInt64 => {
                write_scalar::<PhysicalU64>(layout, array_idx, array, row_pointers, rows)
            }
            PhysicalType::UInt128 => {
                write_scalar::<PhysicalU128>(layout, array_idx, array, row_pointers, rows)
            }
            PhysicalType::Float16 => {
                write_scalar::<PhysicalF16>(layout, array_idx, array, row_pointers, rows)
            }
            PhysicalType::Float32 => {
                write_scalar::<PhysicalF32>(layout, array_idx, array, row_pointers, rows)
            }
            PhysicalType::Float64 => {
                write_scalar::<PhysicalF64>(layout, array_idx, array, row_pointers, rows)
            }
            PhysicalType::Interval => {
                write_scalar::<PhysicalInterval>(layout, array_idx, array, row_pointers, rows)
            }
            PhysicalType::Utf8 | PhysicalType::Binary => {
                write_binary(layout, array_idx, array, row_pointers, heap_pointers, rows)
            }
            other => not_implemented!("Write array for row layout: {other}"),
        }
    }
}

/// Write a string/binary array ot the given row pointers.
///
/// The heap pointers will be written to if the string view for a value is no
/// inline. Thea heap pointers will also be updated to point to the end of what
/// was just written such that the varlen array that we write can use the same
/// pointers directly without having to compute the appropriate offset.
///
/// Row pointers will remain unchanged.
unsafe fn write_binary(
    layout: &RowLayout,
    array_idx: usize,
    array: &Array,
    row_pointers: &[*mut u8],
    heap_pointers: &mut [*mut u8],
    rows: impl IntoExactSizeIterator<Item = usize>,
) -> Result<()> {
    unsafe {
        let rows = rows.into_exact_size_iter();

        debug_assert_eq!(rows.len(), row_pointers.len());
        debug_assert_eq!(rows.len(), heap_pointers.len());

        let buffer =
            PhysicalBinary::downcast_execution_format(&array.data)?.into_selection_format()?;
        let data = PhysicalBinary::addressable(buffer.buffer);

        let validity = &array.validity;

        if validity.all_valid() {
            for (output, row_idx) in rows.into_iter().enumerate() {
                let sel_idx = buffer.selection.get(row_idx).unwrap();
                let view = data.metadata.get(sel_idx).unwrap();

                if !view.is_inline() {
                    let heap_ptr = heap_pointers[output];

                    // Write data to heap, inline an updated string view reference.
                    let value = data.get(sel_idx).unwrap();
                    std::ptr::copy_nonoverlapping(value.as_ptr(), heap_ptr, value.len());
                    let bs = std::slice::from_raw_parts(heap_ptr, value.len());

                    // Create new view that we write to the row.
                    let string_ptr = StringPtr::new_reference(bs);
                    let ptr = row_pointers[output].byte_add(layout.offsets[array_idx]);
                    ptr.cast::<StringPtr>().write_unaligned(string_ptr);

                    // Update heap offset for next column.
                    heap_pointers[output] = heap_ptr.byte_add(value.len());
                } else {
                    // Otherwise we can just write the inline string directly.
                    let ptr = row_pointers[output].byte_add(layout.offsets[array_idx]);
                    ptr.cast::<StringPtr>()
                        .write_unaligned(StringPtr::from(*view.as_inline()));
                }
            }
        } else {
            for (output, row_idx) in rows.into_iter().enumerate() {
                if validity.is_valid(row_idx) {
                    let sel_idx = buffer.selection.get(row_idx).unwrap();
                    let view = data.metadata.get(sel_idx).unwrap();

                    if !view.is_inline() {
                        let heap_ptr = heap_pointers[output];

                        // Write data to heap, inline an updated string view reference.
                        let value = data.get(sel_idx).unwrap();
                        std::ptr::copy_nonoverlapping(value.as_ptr(), heap_ptr, value.len());
                        let bs = std::slice::from_raw_parts(heap_ptr, value.len());

                        // Create new view that we write to the row.
                        let string_ptr = StringPtr::new_reference(bs);
                        let ptr = row_pointers[output].byte_add(layout.offsets[array_idx]);
                        ptr.cast::<StringPtr>().write_unaligned(string_ptr);

                        // Update heap offset for next column.
                        heap_pointers[output] = heap_ptr.byte_add(value.len());
                    } else {
                        // Otherwise we can just write the inline string directly.
                        let ptr = row_pointers[output].byte_add(layout.offsets[array_idx]);
                        ptr.cast::<StringPtr>()
                            .write_unaligned(StringPtr::from(*view.as_inline()));
                    }
                } else {
                    // Write an empty value.
                    //
                    // We do this since we want to be able to read the value _and_
                    // validity during row matching as it makes comparison logic
                    // easier.
                    //
                    // If we didn't write an empty value, we might read in garbage
                    // data which could lead to out of bounds access if we try to
                    // interpret it.
                    let ptr = row_pointers[output].byte_add(layout.offsets[array_idx]);
                    ptr.cast::<StringPtr>().write_unaligned(StringPtr::EMPTY);

                    let validity_buf = layout.validity_buffer_mut(row_pointers[output]);
                    BitmapViewMut::new(validity_buf, layout.num_columns()).unset(array_idx);
                }
            }
        }

        Ok(())
    }
}

/// Write a scalar array to the specified row pointers.
unsafe fn write_scalar<S>(
    layout: &RowLayout,
    array_idx: usize,
    array: &Array,
    row_pointers: &[*mut u8],
    rows: impl IntoExactSizeIterator<Item = usize>,
) -> Result<()>
where
    S: ScalarStorage,
    S::StorageType: Default + Copy + Sized,
{
    unsafe {
        let rows = rows.into_exact_size_iter();
        debug_assert_eq!(rows.len(), row_pointers.len());

        let null_val = <S::StorageType>::default();

        let buffer = S::downcast_execution_format(&array.data)?.into_selection_format()?;
        let data = S::addressable(buffer.buffer);

        let validity = &array.validity;

        if validity.all_valid() {
            for (output, row_idx) in rows.into_iter().enumerate() {
                let sel_idx = buffer.selection.get(row_idx).unwrap();
                let v = data.get(sel_idx).unwrap();

                let ptr = row_pointers[output].byte_add(layout.offsets[array_idx]);
                ptr.cast::<S::StorageType>().write_unaligned(*v);
            }
        } else {
            for (output, row_idx) in rows.into_iter().enumerate() {
                if validity.is_valid(row_idx) {
                    let sel_idx = buffer.selection.get(row_idx).unwrap();
                    let v = data.get(sel_idx).unwrap();

                    let ptr = row_pointers[output].byte_add(layout.offsets[array_idx]);
                    ptr.cast::<S::StorageType>().write_unaligned(*v);
                } else {
                    // Ensure memory is initialized since we always read it in the
                    // row matcher.
                    let ptr = row_pointers[output].byte_add(layout.offsets[array_idx]);
                    ptr.cast::<S::StorageType>().write_unaligned(null_val);

                    let validity_buf = layout.validity_buffer_mut(row_pointers[output]);
                    BitmapViewMut::new(validity_buf, layout.num_columns()).unset(array_idx);
                }
            }
        }

        Ok(())
    }
}

unsafe fn read_array(
    layout: &RowLayout,
    phys_type: PhysicalType,
    row_pointers: impl IntoIterator<Item = *const u8>,
    array_idx: usize,
    out: &mut Array,
    write_offset: usize,
) -> Result<()> {
    unsafe {
        match phys_type {
            PhysicalType::UntypedNull => read_scalar::<PhysicalUntypedNull>(
                layout,
                row_pointers,
                array_idx,
                out,
                write_offset,
            ),
            PhysicalType::Boolean => {
                read_scalar::<PhysicalBool>(layout, row_pointers, array_idx, out, write_offset)
            }
            PhysicalType::Int8 => {
                read_scalar::<PhysicalI8>(layout, row_pointers, array_idx, out, write_offset)
            }
            PhysicalType::Int16 => {
                read_scalar::<PhysicalI16>(layout, row_pointers, array_idx, out, write_offset)
            }
            PhysicalType::Int32 => {
                read_scalar::<PhysicalI32>(layout, row_pointers, array_idx, out, write_offset)
            }
            PhysicalType::Int64 => {
                read_scalar::<PhysicalI64>(layout, row_pointers, array_idx, out, write_offset)
            }
            PhysicalType::Int128 => {
                read_scalar::<PhysicalI128>(layout, row_pointers, array_idx, out, write_offset)
            }
            PhysicalType::UInt8 => {
                read_scalar::<PhysicalU8>(layout, row_pointers, array_idx, out, write_offset)
            }
            PhysicalType::UInt16 => {
                read_scalar::<PhysicalU16>(layout, row_pointers, array_idx, out, write_offset)
            }
            PhysicalType::UInt32 => {
                read_scalar::<PhysicalU32>(layout, row_pointers, array_idx, out, write_offset)
            }
            PhysicalType::UInt64 => {
                read_scalar::<PhysicalU64>(layout, row_pointers, array_idx, out, write_offset)
            }
            PhysicalType::UInt128 => {
                read_scalar::<PhysicalU128>(layout, row_pointers, array_idx, out, write_offset)
            }
            PhysicalType::Float16 => {
                read_scalar::<PhysicalF16>(layout, row_pointers, array_idx, out, write_offset)
            }
            PhysicalType::Float32 => {
                read_scalar::<PhysicalF32>(layout, row_pointers, array_idx, out, write_offset)
            }
            PhysicalType::Float64 => {
                read_scalar::<PhysicalF64>(layout, row_pointers, array_idx, out, write_offset)
            }
            PhysicalType::Interval => {
                read_scalar::<PhysicalInterval>(layout, row_pointers, array_idx, out, write_offset)
            }
            PhysicalType::Utf8 | PhysicalType::Binary => {
                read_binary(layout, row_pointers, array_idx, out, write_offset)
            }
            _ => unimplemented!(),
        }
    }
}

// TODO: We'll want to pass in the blocks directly to allow for strided scans
// instead of randomly accessing pointers.
//
// Same with varlen.
unsafe fn read_scalar<S>(
    layout: &RowLayout,
    row_pointers: impl IntoIterator<Item = *const u8>,
    array_idx: usize,
    out: &mut Array,
    write_offset: usize,
) -> Result<()>
where
    S: MutableScalarStorage,
    S::StorageType: Copy + Sized,
{
    let mut data = S::get_addressable_mut(&mut out.data)?;
    let validity = &mut out.validity;

    // Bit ops for the validity bit buffer.
    let bit_offset = array_idx;
    let byte_offset = bit_offset >> 3; // Equivalent to `idx / 8`
    let bit_mask = 1u8 << (bit_offset & 7); // Equivalent to `1 << (idx % 8)`

    for (row_ptr, output_idx) in row_pointers.into_iter().zip(write_offset..) {
        // Validity buffer starts at the beginning of the row, then we move to
        // the required byte.
        let validity_byte = unsafe { row_ptr.byte_add(byte_offset).read() };

        let is_valid = validity_byte & bit_mask != 0;

        if is_valid {
            let v = unsafe {
                let ptr = row_ptr.byte_add(layout.offsets[array_idx]);
                ptr.cast::<S::StorageType>().read_unaligned()
            };

            data.put(output_idx, &v);
        } else {
            validity.set_invalid(output_idx);
        }
    }

    Ok(())
}

unsafe fn read_binary(
    layout: &RowLayout,
    row_pointers: impl IntoIterator<Item = *const u8>,
    array_idx: usize,
    out: &mut Array,
    write_offset: usize,
) -> Result<()> {
    let mut data = PhysicalBinary::get_addressable_mut(&mut out.data)?;
    let validity = &mut out.validity;

    // Bit ops for the validity bit buffer.
    let bit_offset = array_idx;
    let byte_offset = bit_offset >> 3; // Equivalent to `idx / 8`
    let bit_mask = 1u8 << (bit_offset & 7); // Equivalent to `1 << (idx % 8)`

    for (row_ptr, output_idx) in row_pointers.into_iter().zip(write_offset..) {
        // Validity buffer starts at the beginning of the row, then we move to
        // the required byte.
        let validity_byte = unsafe { row_ptr.byte_add(byte_offset).read() };

        let is_valid = validity_byte & bit_mask != 0;

        if is_valid {
            let ptr = unsafe { row_ptr.byte_add(layout.offsets[array_idx]) };
            let string_ptr = unsafe { ptr.cast::<StringPtr>().read_unaligned() };

            let bs = string_ptr.as_bytes();
            data.put(output_idx, bs);
        } else {
            validity.set_invalid(output_idx);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::row::block_scan::BlockScanState;
    use crate::arrays::row::row_blocks::RowBlocks;
    use crate::buffer::buffer_manager::DefaultBufferManager;
    use crate::testutil::arrays::assert_arrays_eq;
    use crate::util::iter::TryFromExactSizeIterator;

    #[test]
    fn new_empty() {
        let layout = RowLayout::try_new([]).unwrap();
        assert_eq!(0, layout.num_columns());
        assert_eq!(0, layout.row_width);
    }

    #[test]
    fn buffer_size_i32() {
        let layout = RowLayout::try_new(vec![DataType::int32()]).unwrap();

        // Note +1 for validity.
        assert_eq!(5, layout.buffer_size(1));
        assert_eq!(10, layout.buffer_size(2));
        assert_eq!(15, layout.buffer_size(3));
    }

    #[test]
    fn buffer_size_i32_f64() {
        let layout = RowLayout::try_new(vec![DataType::int32(), DataType::float64()]).unwrap();

        // Note +1 for validity.
        assert_eq!(13, layout.buffer_size(1));
        assert_eq!(26, layout.buffer_size(2));
        assert_eq!(39, layout.buffer_size(3));
    }

    #[test]
    fn buffer_size_multi_byte_validity() {
        // Create layout with >8 types to force an additional byte needed for
        // validity.
        let layout = RowLayout::try_new(vec![DataType::int32(); 9]).unwrap();

        // +2 for validity
        assert_eq!((9 * 4 + 2), layout.buffer_size(1));
        assert_eq!((9 * 4 + 2) * 2, layout.buffer_size(2));
        assert_eq!((9 * 4 + 2) * 3, layout.buffer_size(3));
    }

    #[test]
    fn compute_heap_size_for_fixed_size() {
        let layout = RowLayout::try_new([DataType::int32()]).unwrap();
        let arr = Array::try_from_iter([1, 2, 3]).unwrap();
        let mut heap_sizes = vec![1, 2, 3]; // Start with dummy data to ensure it gets zeroed out.

        layout
            .compute_heap_sizes(&[arr], 0..3, &mut heap_sizes)
            .unwrap();

        assert_eq!(&[0, 0, 0], heap_sizes.as_slice());
    }

    #[test]
    fn compute_heap_size_all_inlineable_strings() {
        let layout = RowLayout::try_new([DataType::int32()]).unwrap();
        let arr = Array::try_from_iter(["a", "b", "c"]).unwrap();
        let mut heap_sizes = vec![1, 2, 3]; // Start with dummy data to ensure it gets zeroed out.

        layout
            .compute_heap_sizes(&[arr], 0..3, &mut heap_sizes)
            .unwrap();

        assert_eq!(&[0, 0, 0], heap_sizes.as_slice());
    }

    #[test]
    fn compute_heap_size_all_heap_strings() {
        let layout = RowLayout::try_new([DataType::int32()]).unwrap();
        let arr =
            Array::try_from_iter(["aaaaaaaaaaaaa", "bbbbbbbbbbbbbb", "ccccccccccccccc"]).unwrap();
        let mut heap_sizes = vec![1, 2, 3]; // Start with dummy data to ensure it gets zeroed out.

        layout
            .compute_heap_sizes(&[arr], 0..3, &mut heap_sizes)
            .unwrap();

        assert_eq!(&[13, 14, 15], heap_sizes.as_slice());
    }

    #[test]
    fn compute_heap_size_all_heap_strings_with_selection() {
        let layout = RowLayout::try_new([DataType::int32()]).unwrap();
        let arr =
            Array::try_from_iter(["aaaaaaaaaaaaa", "bbbbbbbbbbbbbb", "ccccccccccccccc"]).unwrap();
        let mut heap_sizes = vec![1, 3]; // Start with dummy data to ensure it gets zeroed out.

        layout
            .compute_heap_sizes(&[arr], [0, 2], &mut heap_sizes)
            .unwrap();

        assert_eq!(&[13, 15], heap_sizes.as_slice());
    }

    /// Writes an array to a row layout, and reads it back.
    ///
    /// The provided selection determines which rows to write. The resulting
    /// array with have the same length as the selection.
    fn write_read_array(
        array: &Array,
        rows: impl IntoExactSizeIterator<Item = usize> + Clone,
    ) -> Array {
        let sel_len = rows.clone().into_exact_size_iter().len();
        let layout = RowLayout::try_new([array.datatype().clone()]).unwrap();

        let heap_sizes = if layout.requires_heap {
            let mut heap_sizes = vec![0; sel_len];
            layout
                .compute_heap_sizes(&[array], rows.clone(), &mut heap_sizes)
                .unwrap();
            Some(heap_sizes)
        } else {
            None
        };

        let mut blocks = RowBlocks::new_using_row_layout(&DefaultBufferManager, &layout, 16);
        let mut state = BlockAppendState {
            row_pointers: Vec::new(),
            heap_pointers: Vec::new(),
        };

        blocks
            .prepare_append(&mut state, sel_len, heap_sizes.as_deref())
            .unwrap();

        unsafe {
            layout.write_arrays(&mut state, &[array], rows).unwrap();
        }

        let mut out = Array::new(&DefaultBufferManager, array.datatype().clone(), sel_len).unwrap();

        let state = BlockScanState {
            row_pointers: state
                .row_pointers
                .iter()
                .map(|ptr| ptr.cast_const())
                .collect(),
        };

        unsafe {
            layout
                .read_arrays(state.row_pointers_iter(), [(0, &mut out)], 0)
                .unwrap();
        }

        out
    }

    #[test]
    fn write_read_i32() {
        let array = Array::try_from_iter([1, 2, 3]).unwrap();
        let got = write_read_array(&array, 0..3);
        assert_arrays_eq(&array, &got);
    }

    #[test]
    fn write_read_i32_with_selection() {
        let array = Array::try_from_iter([1, 2, 3]).unwrap();
        let got = write_read_array(&array, [0, 2]);
        let expected = Array::try_from_iter([1, 3]).unwrap();
        assert_arrays_eq(&expected, &got);
    }

    #[test]
    fn write_read_i32_with_invalid() {
        let array = Array::try_from_iter([Some(1), None, Some(3)]).unwrap();
        let got = write_read_array(&array, 0..3);
        assert_arrays_eq(&array, &got);
    }

    #[test]
    fn write_read_utf8() {
        let array = Array::try_from_iter(["cat", "dog", "goose"]).unwrap();
        let got = write_read_array(&array, 0..3);
        assert_arrays_eq(&array, &got);
    }

    #[test]
    fn write_read_utf8_with_no_inlineable() {
        let array = Array::try_from_iter(["cat", "dog", "goosegoosegoosemoosecatdog"]).unwrap();
        let got = write_read_array(&array, 0..3);
        assert_arrays_eq(&array, &got);
    }

    #[test]
    fn write_read_utf8_with_no_inlineable_with_selection() {
        let array = Array::try_from_iter(["cat", "dog", "goosegoosegoosemoosecatdog"]).unwrap();
        let got = write_read_array(&array, [0, 2]);
        let expected = Array::try_from_iter(["cat", "goosegoosegoosemoosecatdog"]).unwrap();
        assert_arrays_eq(&expected, &got);
    }

    #[test]
    fn write_read_utf8_with_invalid() {
        let array = Array::try_from_iter([Some("cat"), None, Some("goose")]).unwrap();
        let got = write_read_array(&array, 0..3);
        assert_arrays_eq(&array, &got);
    }

    // #[test]
    // fn encode_decode_multiple_fixed_size() {
    //     let layout = RowLayout::new(vec![DataType::int32(), DataType::float64()]);
    //     let mut heap = RowHeap::with_capacity(&NopBufferManager, 0).unwrap();

    //     let mut buf = vec![0; layout.buffer_size(3)];
    //     let batch = generate_batch!([1, 2, 3], [1.0, 2.0, 3.0]);

    //     layout
    //         .encode_arrays(&batch.arrays, batch.selection(), &mut buf, &mut heap)
    //         .unwrap();

    //     let mut output = Batch::try_new([DataType::int32(), DataType::float64()], 16).unwrap();
    //     layout
    //         .decode_arrays(&buf, &heap, 0..3, output.arrays.iter_mut().enumerate())
    //         .unwrap();
    //     output.set_num_rows(3).unwrap();

    //     let expected = generate_batch!([1, 2, 3], [1.0, 2.0, 3.0]);
    //     assert_batches_eq(&expected, &output);
    // }

    // #[test]
    // fn encode_decode_multiple_fixed_size_with_invalid() {
    //     let layout = RowLayout::new(vec![DataType::int32(), DataType::float64()]);
    //     let mut heap = RowHeap::with_capacity(&NopBufferManager, 0).unwrap();

    //     let mut buf = vec![0; layout.buffer_size(3)];
    //     let batch = generate_batch!([None, Some(2), Some(3)], [Some(1.0), None, Some(3.0)]);

    //     layout
    //         .encode_arrays(&batch.arrays, batch.selection(), &mut buf, &mut heap)
    //         .unwrap();

    //     let mut output = Batch::try_new([DataType::int32(), DataType::float64()], 16).unwrap();
    //     layout
    //         .decode_arrays(&buf, &heap, 0..3, output.arrays.iter_mut().enumerate())
    //         .unwrap();
    //     output.set_num_rows(3).unwrap();

    //     let expected = generate_batch!([None, Some(2), Some(3)], [Some(1.0), None, Some(3.0)]);
    //     assert_batches_eq(&expected, &output);
    // }
}
