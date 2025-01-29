use std::borrow::Borrow;

use half::f16;
use rayexec_error::{RayexecError, Result};
use stdutil::iter::IntoExactSizeIterator;

use super::row_heap::{RowHeap, RowHeapMetadataUnion};
use crate::arrays::array::buffer_manager::BufferManager;
use crate::arrays::array::flat::FlattenedArray;
use crate::arrays::array::physical_type::{
    Addressable, AddressableMut, MutableScalarStorage, PhysicalBinary, PhysicalBool, PhysicalF16,
    PhysicalF32, PhysicalF64, PhysicalI128, PhysicalI16, PhysicalI32, PhysicalI64, PhysicalI8,
    PhysicalInterval, PhysicalType, PhysicalU128, PhysicalU16, PhysicalU32, PhysicalU64,
    PhysicalU8, PhysicalUntypedNull, ScalarStorage, UntypedNull,
};
use crate::arrays::array::Array;
use crate::arrays::bitmap::view::{num_bytes_for_bitmap, BitmapView, BitmapViewMut};
use crate::arrays::datatype::DataType;
use crate::arrays::scalar::interval::Interval;

/// Describes the layout of a row for use with a row collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowLayout {
    /// Data types for each column in the row.
    pub(crate) types: Vec<DataType>,
    /// Offsets within the encoded row to the start of the value.
    pub(crate) offsets: Vec<usize>,
    /// Sized in bytes for the (inline) encoded row. Does not include the size
    /// encoded and pushed to the string heap.
    pub(crate) row_width: usize,
    /// If the row encoding requires writing parts of the data to a heap.
    pub(crate) requires_heap: bool,
    /// Size in bytes for the validity mask.
    ///
    /// Each value's validity is a single bit.
    pub(crate) validity_width: usize,
}

impl RowLayout {
    pub fn new(types: impl IntoIterator<Item = DataType>) -> Self {
        let types: Vec<_> = types.into_iter().collect();
        let validity_width = num_bytes_for_bitmap(types.len());

        let mut offset = validity_width;
        let mut offsets = Vec::with_capacity(types.len());

        let mut requires_heap = false;

        for typ in &types {
            let phys_type = typ.physical_type();
            let width = row_width_for_physical_type(phys_type);
            offsets.push(offset);
            offset += width;

            requires_heap = requires_heap || row_encoding_requires_heap(phys_type);
        }

        RowLayout {
            types,
            offsets,
            row_width: offset,
            requires_heap,
            validity_width,
        }
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

    /// Encodes arrays into a buffer using this layout.
    ///
    /// The buffer must be the exact size for encoding the length of the selection.
    pub(crate) fn encode_arrays<A, B>(
        &self,
        arrays: &[A],
        selection: impl IntoExactSizeIterator<Item = usize> + Clone,
        buffer: &mut [u8],
        heap: &mut RowHeap<B>,
    ) -> Result<()>
    where
        A: Borrow<Array<B>>,
        B: BufferManager,
    {
        let num_rows = selection.clone().into_exact_size_iter().len();
        init_row_validities(self, num_rows, buffer);

        for (array_idx, array) in arrays.iter().enumerate() {
            let array = array.borrow();
            let phys_type = array.datatype().physical_type();
            let array = array.flatten()?;
            encode_array(
                self,
                phys_type,
                array_idx,
                array,
                selection.clone(),
                buffer,
                heap,
            )?;
        }

        Ok(())
    }

    /// Decodes a buffer into the output arrays using this layout.
    ///
    /// The selection is used to determine which rows to decode from the buffer
    /// into the output.
    pub(crate) fn decode_arrays<'a, B>(
        &self,
        buffer: &[u8],
        heap: &RowHeap<B>,
        selection: impl IntoExactSizeIterator<Item = usize> + Clone,
        output_arrays: impl IntoIterator<Item = &'a mut Array<B>>,
    ) -> Result<()>
    where
        B: BufferManager + 'a,
    {
        for (array_idx, output) in output_arrays.into_iter().enumerate() {
            let phys_type = output.datatype().physical_type();
            decode_array(
                self,
                phys_type,
                array_idx,
                buffer,
                heap,
                selection.clone(),
                output,
            )?;
        }

        Ok(())
    }
}

pub(crate) const fn row_encoding_requires_heap(phys_type: PhysicalType) -> bool {
    matches!(
        phys_type,
        PhysicalType::Utf8 | PhysicalType::Binary | PhysicalType::List
    )
}

pub(crate) const fn row_width_for_physical_type(phys_type: PhysicalType) -> usize {
    match phys_type {
        PhysicalType::UntypedNull => UntypedNull::ENCODE_WIDTH, // Zero
        PhysicalType::Boolean => bool::ENCODE_WIDTH,
        PhysicalType::Int8 => i8::ENCODE_WIDTH,
        PhysicalType::Int16 => i16::ENCODE_WIDTH,
        PhysicalType::Int32 => i32::ENCODE_WIDTH,
        PhysicalType::Int64 => i64::ENCODE_WIDTH,
        PhysicalType::Int128 => i128::ENCODE_WIDTH,
        PhysicalType::UInt8 => u8::ENCODE_WIDTH,
        PhysicalType::UInt16 => u16::ENCODE_WIDTH,
        PhysicalType::UInt32 => u32::ENCODE_WIDTH,
        PhysicalType::UInt64 => u64::ENCODE_WIDTH,
        PhysicalType::UInt128 => u128::ENCODE_WIDTH,
        PhysicalType::Float16 => f16::ENCODE_WIDTH,
        PhysicalType::Float32 => f32::ENCODE_WIDTH,
        PhysicalType::Float64 => f64::ENCODE_WIDTH,
        PhysicalType::Interval => Interval::ENCODE_WIDTH,
        PhysicalType::Binary => RowHeapMetadataUnion::ENCODE_WIDTH,
        PhysicalType::Utf8 => RowHeapMetadataUnion::ENCODE_WIDTH,
        _ => unimplemented!(),
    }
}

/// Initializes row validities for some number of rows in the buffer.
///
/// This serves two purposes:
///
/// - Ensures that the memory is initialized
/// - Prevent needing to explicitly set columns as valid
fn init_row_validities(layout: &RowLayout, num_rows: usize, buffer: &mut [u8]) {
    for row in 0..num_rows {
        let validity_offset = layout.row_width * row;
        let out_buf = &mut buffer[validity_offset..(validity_offset + layout.validity_width)];
        out_buf.iter_mut().for_each(|b| *b = u8::MAX);
    }
}

/// Encodes a flattened array to a buffer.
///
/// `array_idx` is the index in the batch that this array is for.
fn encode_array<B>(
    layout: &RowLayout,
    phys_type: PhysicalType,
    array_idx: usize,
    array: FlattenedArray<B>,
    selection: impl IntoExactSizeIterator<Item = usize>,
    buffer: &mut [u8],
    heap: &mut RowHeap<B>,
) -> Result<()>
where
    B: BufferManager,
{
    let selection = selection.into_exact_size_iter();
    let expected_size = layout.buffer_size(selection.len());
    if expected_size != buffer.len() {
        return Err(RayexecError::new(
            "Buffer size does not equal expected buffer size for selection",
        )
        .with_field("expected", expected_size)
        .with_field("buffer", buffer.len()));
    }

    match phys_type {
        PhysicalType::UntypedNull => {
            encode_scalar::<PhysicalUntypedNull, B>(layout, array_idx, array, selection, buffer)
        }
        PhysicalType::Boolean => {
            encode_scalar::<PhysicalBool, B>(layout, array_idx, array, selection, buffer)
        }
        PhysicalType::Int8 => {
            encode_scalar::<PhysicalI8, B>(layout, array_idx, array, selection, buffer)
        }
        PhysicalType::Int16 => {
            encode_scalar::<PhysicalI16, B>(layout, array_idx, array, selection, buffer)
        }
        PhysicalType::Int32 => {
            encode_scalar::<PhysicalI32, B>(layout, array_idx, array, selection, buffer)
        }
        PhysicalType::Int64 => {
            encode_scalar::<PhysicalI64, B>(layout, array_idx, array, selection, buffer)
        }
        PhysicalType::Int128 => {
            encode_scalar::<PhysicalI128, B>(layout, array_idx, array, selection, buffer)
        }
        PhysicalType::UInt8 => {
            encode_scalar::<PhysicalU8, B>(layout, array_idx, array, selection, buffer)
        }
        PhysicalType::UInt16 => {
            encode_scalar::<PhysicalU16, B>(layout, array_idx, array, selection, buffer)
        }
        PhysicalType::UInt32 => {
            encode_scalar::<PhysicalU32, B>(layout, array_idx, array, selection, buffer)
        }
        PhysicalType::UInt64 => {
            encode_scalar::<PhysicalU64, B>(layout, array_idx, array, selection, buffer)
        }
        PhysicalType::UInt128 => {
            encode_scalar::<PhysicalU128, B>(layout, array_idx, array, selection, buffer)
        }
        PhysicalType::Float16 => {
            encode_scalar::<PhysicalF16, B>(layout, array_idx, array, selection, buffer)
        }
        PhysicalType::Float32 => {
            encode_scalar::<PhysicalF32, B>(layout, array_idx, array, selection, buffer)
        }
        PhysicalType::Float64 => {
            encode_scalar::<PhysicalF64, B>(layout, array_idx, array, selection, buffer)
        }
        PhysicalType::Interval => {
            encode_scalar::<PhysicalInterval, B>(layout, array_idx, array, selection, buffer)
        }
        PhysicalType::Utf8 | PhysicalType::Binary => {
            encode_binary(layout, array_idx, array, selection, buffer, heap)
        }
        _ => unimplemented!(),
    }
}

fn encode_binary<B>(
    layout: &RowLayout,
    array_idx: usize,
    array: FlattenedArray<B>,
    selection: impl IntoIterator<Item = usize>,
    buffer: &mut [u8],
    heap: &mut RowHeap<B>,
) -> Result<()>
where
    B: BufferManager,
{
    let value_width = RowHeapMetadataUnion::ENCODE_WIDTH;
    let data = PhysicalBinary::get_addressable(array.array_buffer)?;

    let validity = array.validity;

    if validity.all_valid() {
        for (output_idx, input_idx) in selection.into_iter().enumerate() {
            let sel_idx = array.selection.get(input_idx).unwrap();
            let v = data.get(sel_idx).unwrap();

            let metadata = heap.push_bytes(v)?;

            let value_offset = layout.row_width * output_idx + layout.offsets[array_idx];
            let out_buf = &mut buffer[value_offset..(value_offset + value_width)];
            metadata.encode(out_buf);
        }
    } else {
        for (output_idx, input_idx) in selection.into_iter().enumerate() {
            if validity.is_valid(input_idx) {
                let sel_idx = array.selection.get(input_idx).unwrap();
                let v = data.get(sel_idx).unwrap();

                let metadata = heap.push_bytes(v)?;

                let value_offset = layout.row_width * output_idx + layout.offsets[array_idx];
                let out_buf = &mut buffer[value_offset..(value_offset + value_width)];
                metadata.encode(out_buf);
            } else {
                let validity_offset = layout.row_width * output_idx;
                let out_buf =
                    &mut buffer[validity_offset..(validity_offset + layout.validity_width)];
                BitmapViewMut::new(out_buf, layout.num_columns()).unset(array_idx);
            }
        }
    }

    Ok(())
}

fn encode_scalar<S, B>(
    layout: &RowLayout,
    array_idx: usize,
    array: FlattenedArray<B>,
    selection: impl IntoIterator<Item = usize>,
    buffer: &mut [u8],
) -> Result<()>
where
    S: ScalarStorage,
    S::StorageType: Encode,
    B: BufferManager,
{
    let value_width = <S::StorageType>::ENCODE_WIDTH;
    let data = S::get_addressable(array.array_buffer)?;
    let validity = array.validity;

    if validity.all_valid() {
        for (output_idx, input_idx) in selection.into_iter().enumerate() {
            let sel_idx = array.selection.get(input_idx).unwrap();
            let v = data.get(sel_idx).unwrap();

            let value_offset = layout.row_width * output_idx + layout.offsets[array_idx];
            let out_buf = &mut buffer[value_offset..(value_offset + value_width)];
            v.encode(out_buf);
        }
    } else {
        for (output_idx, input_idx) in selection.into_iter().enumerate() {
            if validity.is_valid(input_idx) {
                let sel_idx = array.selection.get(input_idx).unwrap();
                let v = data.get(sel_idx).unwrap();

                let value_offset = layout.row_width * output_idx + layout.offsets[array_idx];
                let out_buf = &mut buffer[value_offset..(value_offset + value_width)];
                v.encode(out_buf);
            } else {
                let validity_offset = layout.row_width * output_idx;
                let out_buf =
                    &mut buffer[validity_offset..(validity_offset + layout.validity_width)];
                BitmapViewMut::new(out_buf, layout.num_columns()).unset(array_idx);
            }
        }
    }

    Ok(())
}

fn decode_array<B>(
    layout: &RowLayout,
    phys_type: PhysicalType,
    array_idx: usize,
    buffer: &[u8],
    heap: &RowHeap<B>,
    selection: impl IntoIterator<Item = usize>,
    out: &mut Array<B>,
) -> Result<()>
where
    B: BufferManager,
{
    match phys_type {
        PhysicalType::UntypedNull => {
            decode_scalar::<PhysicalUntypedNull, B>(layout, array_idx, buffer, selection, out)
        }
        PhysicalType::Boolean => {
            decode_scalar::<PhysicalBool, B>(layout, array_idx, buffer, selection, out)
        }
        PhysicalType::Int8 => {
            decode_scalar::<PhysicalI8, B>(layout, array_idx, buffer, selection, out)
        }
        PhysicalType::Int16 => {
            decode_scalar::<PhysicalI16, B>(layout, array_idx, buffer, selection, out)
        }
        PhysicalType::Int32 => {
            decode_scalar::<PhysicalI32, B>(layout, array_idx, buffer, selection, out)
        }
        PhysicalType::Int64 => {
            decode_scalar::<PhysicalI64, B>(layout, array_idx, buffer, selection, out)
        }
        PhysicalType::Int128 => {
            decode_scalar::<PhysicalI128, B>(layout, array_idx, buffer, selection, out)
        }
        PhysicalType::UInt8 => {
            decode_scalar::<PhysicalU8, B>(layout, array_idx, buffer, selection, out)
        }
        PhysicalType::UInt16 => {
            decode_scalar::<PhysicalU16, B>(layout, array_idx, buffer, selection, out)
        }
        PhysicalType::UInt32 => {
            decode_scalar::<PhysicalU32, B>(layout, array_idx, buffer, selection, out)
        }
        PhysicalType::UInt64 => {
            decode_scalar::<PhysicalU64, B>(layout, array_idx, buffer, selection, out)
        }
        PhysicalType::UInt128 => {
            decode_scalar::<PhysicalU128, B>(layout, array_idx, buffer, selection, out)
        }
        PhysicalType::Float16 => {
            decode_scalar::<PhysicalF16, B>(layout, array_idx, buffer, selection, out)
        }
        PhysicalType::Float32 => {
            decode_scalar::<PhysicalF32, B>(layout, array_idx, buffer, selection, out)
        }
        PhysicalType::Float64 => {
            decode_scalar::<PhysicalF64, B>(layout, array_idx, buffer, selection, out)
        }
        PhysicalType::Interval => {
            decode_scalar::<PhysicalInterval, B>(layout, array_idx, buffer, selection, out)
        }
        PhysicalType::Utf8 | PhysicalType::Binary => {
            decode_binary(layout, array_idx, buffer, heap, selection, out)
        }

        _ => unimplemented!(),
    }
}

fn decode_binary<B>(
    layout: &RowLayout,
    array_idx: usize,
    buffer: &[u8],
    heap: &RowHeap<B>,
    selection: impl IntoIterator<Item = usize>,
    out: &mut Array<B>,
) -> Result<()>
where
    B: BufferManager,
{
    let value_width = RowHeapMetadataUnion::ENCODE_WIDTH;
    let mut data = PhysicalBinary::get_addressable_mut(&mut out.data)?;
    let validity = &mut out.validity;

    for (output_idx, input_idx) in selection.into_iter().enumerate() {
        let validity_offset = layout.row_width * input_idx;
        let validity_buf = &buffer[validity_offset..(validity_offset + layout.validity_width)];
        let is_valid = BitmapView::new(validity_buf, layout.num_columns()).value(array_idx);

        if is_valid {
            let value_offset = layout.row_width * input_idx + layout.offsets[array_idx];
            let in_buf = &buffer[value_offset..(value_offset + value_width)];
            let metadata = RowHeapMetadataUnion::decode(in_buf);

            let v = heap.get(&metadata).unwrap();
            data.put(output_idx, &v);
        } else {
            validity.set_invalid(output_idx);
        }
    }

    Ok(())
}

fn decode_scalar<S, B>(
    layout: &RowLayout,
    array_idx: usize,
    buffer: &[u8],
    selection: impl IntoIterator<Item = usize>,
    out: &mut Array<B>,
) -> Result<()>
where
    S: MutableScalarStorage,
    S::StorageType: Encode + Sized,
    B: BufferManager,
{
    let value_width = <S::StorageType>::ENCODE_WIDTH;
    let mut data = S::get_addressable_mut(&mut out.data)?;
    let validity = &mut out.validity;

    for (output_idx, input_idx) in selection.into_iter().enumerate() {
        let validity_offset = layout.row_width * input_idx;
        let validity_buf = &buffer[validity_offset..(validity_offset + layout.validity_width)];
        let is_valid = BitmapView::new(validity_buf, layout.num_columns()).value(array_idx);

        if is_valid {
            let value_offset = layout.row_width * input_idx + layout.offsets[array_idx];
            let in_buf = &buffer[value_offset..(value_offset + value_width)];
            let v = <S::StorageType>::decode(in_buf);
            data.put(output_idx, &v);
        } else {
            validity.set_invalid(output_idx);
        }
    }

    Ok(())
}

/// Trait for converting self to bytes. Encodings do not require any special
/// properties other than a value always encoding to the same result.
trait Encode {
    const ENCODE_WIDTH: usize;

    fn encode(&self, buf: &mut [u8]);
    fn decode(buf: &[u8]) -> Self;
}

macro_rules! primitive_encode {
    ($type:ty, $encode_width:expr) => {
        impl Encode for $type {
            const ENCODE_WIDTH: usize = $encode_width;

            fn encode(&self, buf: &mut [u8]) {
                let b = self.to_le_bytes();
                buf.copy_from_slice(&b);
            }

            fn decode(buf: &[u8]) -> Self {
                Self::from_le_bytes(buf.try_into().unwrap())
            }
        }
    };
}

primitive_encode!(u8, 1);
primitive_encode!(u16, 2);
primitive_encode!(u32, 4);
primitive_encode!(u64, 8);
primitive_encode!(u128, 16);

primitive_encode!(i8, 1);
primitive_encode!(i16, 2);
primitive_encode!(i32, 4);
primitive_encode!(i64, 8);
primitive_encode!(i128, 16);

primitive_encode!(f16, 2);
primitive_encode!(f32, 4);
primitive_encode!(f64, 8);

impl Encode for UntypedNull {
    const ENCODE_WIDTH: usize = 0;

    fn encode(&self, _buf: &mut [u8]) {
        // Do nothing, zero sized
    }

    fn decode(_buf: &[u8]) -> Self {
        Self
    }
}

impl Encode for Interval {
    const ENCODE_WIDTH: usize = 16;

    fn encode(&self, buf: &mut [u8]) {
        // TODO: We'll probably need to ensure intervals are normalized.
        self.months.encode(&mut buf[0..4]);
        self.days.encode(&mut buf[4..8]);
        self.nanos.encode(&mut buf[8..16]);
    }

    fn decode(buf: &[u8]) -> Self {
        Interval {
            months: i32::decode(&buf[0..4]),
            days: i32::decode(&buf[4..8]),
            nanos: i64::decode(&buf[8..16]),
        }
    }
}

impl Encode for RowHeapMetadataUnion {
    const ENCODE_WIDTH: usize = 16;

    fn encode(&self, buf: &mut [u8]) {
        let b = self.to_bytes();
        buf.copy_from_slice(&b);
    }

    fn decode(buf: &[u8]) -> Self {
        Self::from_bytes(buf.try_into().unwrap())
    }
}

impl Encode for bool {
    const ENCODE_WIDTH: usize = 1;

    fn encode(&self, buf: &mut [u8]) {
        if *self {
            buf[0] = 0;
        } else {
            buf[0] = 255;
        }
    }

    fn decode(buf: &[u8]) -> Self {
        if buf[0] == 0 {
            false
        } else {
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::buffer_manager::NopBufferManager;
    use crate::arrays::batch::Batch;
    use crate::arrays::testutil::{assert_arrays_eq, assert_batches_eq, generate_batch};

    #[test]
    fn buffer_size_i32() {
        let layout = RowLayout::new(vec![DataType::Int32]);

        // Note +1 for validity.
        assert_eq!(5, layout.buffer_size(1));
        assert_eq!(10, layout.buffer_size(2));
        assert_eq!(15, layout.buffer_size(3));
    }

    #[test]
    fn buffer_size_i32_f64() {
        let layout = RowLayout::new(vec![DataType::Int32, DataType::Float64]);

        // Note +1 for validity.
        assert_eq!(13, layout.buffer_size(1));
        assert_eq!(26, layout.buffer_size(2));
        assert_eq!(39, layout.buffer_size(3));
    }

    #[test]
    fn buffer_size_multi_byte_validity() {
        // Create layout with >8 types to force an additional byte needed for
        // validity.
        let layout = RowLayout::new(vec![DataType::Int32; 9]);

        // +2 for validity
        assert_eq!((9 * 4 + 2) * 1, layout.buffer_size(1));
        assert_eq!((9 * 4 + 2) * 2, layout.buffer_size(2));
        assert_eq!((9 * 4 + 2) * 3, layout.buffer_size(3));
    }

    #[test]
    fn encode_decode_single_i32() {
        let layout = RowLayout::new(vec![DataType::Int32]);
        let mut heap = RowHeap::with_capacity(&NopBufferManager, 0).unwrap();

        let mut buf = vec![0; layout.buffer_size(3)];
        let array = Array::try_from_iter([1, 2, 3]).unwrap();

        layout
            .encode_arrays(&[array], 0..3, &mut buf, &mut heap)
            .unwrap();

        let mut output = Array::try_new(&NopBufferManager, DataType::Int32, 3).unwrap();
        layout
            .decode_arrays(&buf, &heap, 0..3, [&mut output])
            .unwrap();

        let expected = Array::try_from_iter([1, 2, 3]).unwrap();
        assert_arrays_eq(&expected, &output);
    }

    #[test]
    fn encode_decode_single_i32_with_invalid() {
        let layout = RowLayout::new(vec![DataType::Int32]);
        let mut heap = RowHeap::with_capacity(&NopBufferManager, 0).unwrap();

        let mut buf = vec![0; layout.buffer_size(3)];
        let array = Array::try_from_iter([Some(1), None, Some(3)]).unwrap();

        layout
            .encode_arrays(&[array], 0..3, &mut buf, &mut heap)
            .unwrap();

        let mut output = Array::try_new(&NopBufferManager, DataType::Int32, 3).unwrap();
        layout
            .decode_arrays(&buf, &heap, 0..3, [&mut output])
            .unwrap();

        let expected = Array::try_from_iter([Some(1), None, Some(3)]).unwrap();
        assert_arrays_eq(&expected, &output);
    }

    #[test]
    fn encode_decode_multiple_fixed_size() {
        let layout = RowLayout::new(vec![DataType::Int32, DataType::Float64]);
        let mut heap = RowHeap::with_capacity(&NopBufferManager, 0).unwrap();

        let mut buf = vec![0; layout.buffer_size(3)];
        let batch = generate_batch!([1, 2, 3], [1.0, 2.0, 3.0]);

        layout
            .encode_arrays(&batch.arrays, batch.selection(), &mut buf, &mut heap)
            .unwrap();

        let mut output = Batch::try_new([DataType::Int32, DataType::Float64], 16).unwrap();
        layout
            .decode_arrays(&buf, &heap, 0..3, &mut output.arrays)
            .unwrap();
        output.set_num_rows(3).unwrap();

        let expected = generate_batch!([1, 2, 3], [1.0, 2.0, 3.0]);
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn encode_decode_multiple_fixed_size_with_invalid() {
        let layout = RowLayout::new(vec![DataType::Int32, DataType::Float64]);
        let mut heap = RowHeap::with_capacity(&NopBufferManager, 0).unwrap();

        let mut buf = vec![0; layout.buffer_size(3)];
        let batch = generate_batch!([None, Some(2), Some(3)], [Some(1.0), None, Some(3.0)]);

        layout
            .encode_arrays(&batch.arrays, batch.selection(), &mut buf, &mut heap)
            .unwrap();

        let mut output = Batch::try_new([DataType::Int32, DataType::Float64], 16).unwrap();
        layout
            .decode_arrays(&buf, &heap, 0..3, &mut output.arrays)
            .unwrap();
        output.set_num_rows(3).unwrap();

        let expected = generate_batch!([None, Some(2), Some(3)], [Some(1.0), None, Some(3.0)]);
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn encode_decode_utf8() {
        let layout = RowLayout::new([DataType::Utf8]);
        let mut heap = RowHeap::with_capacity(&NopBufferManager, 0).unwrap();

        let mut buf = vec![0; layout.buffer_size(3)];
        let batch = generate_batch!(["cat", "dog", "goose"]);

        layout
            .encode_arrays(&batch.arrays, 0..3, &mut buf, &mut heap)
            .unwrap();

        let mut output = Batch::try_new([DataType::Utf8], 16).unwrap();
        layout
            .decode_arrays(&buf, &heap, 0..3, &mut output.arrays)
            .unwrap();
        output.set_num_rows(3).unwrap();

        let expected = generate_batch!(["cat", "dog", "goose"]);
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn encode_decode_utf8_with_invalid() {
        let layout = RowLayout::new([DataType::Utf8]);
        let mut heap = RowHeap::with_capacity(&NopBufferManager, 0).unwrap();

        let mut buf = vec![0; layout.buffer_size(3)];
        let batch = generate_batch!([Some("cat"), None, Some("goose")]);

        layout
            .encode_arrays(&batch.arrays, 0..3, &mut buf, &mut heap)
            .unwrap();

        let mut output = Batch::try_new([DataType::Utf8], 16).unwrap();
        layout
            .decode_arrays(&buf, &heap, 0..3, &mut output.arrays)
            .unwrap();
        output.set_num_rows(3).unwrap();

        let expected = generate_batch!([Some("cat"), None, Some("goose")]);
        assert_batches_eq(&expected, &output);
    }
}
