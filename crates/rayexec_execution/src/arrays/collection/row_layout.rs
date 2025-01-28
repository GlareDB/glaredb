use half::f16;
use rayexec_error::{RayexecError, Result};
use stdutil::iter::IntoExactSizeIterator;

use crate::arrays::array::buffer_manager::BufferManager;
use crate::arrays::array::flat::FlattenedArray;
use crate::arrays::array::physical_type::{
    Addressable,
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
    ScalarStorage,
    UntypedNull,
};
use crate::arrays::array::string_view::StringViewMetadataUnion;
use crate::arrays::array::Array;
use crate::arrays::bitmap::view::{num_bytes_for_bitmap, BitmapViewMut};
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
    pub fn new(types: Vec<DataType>) -> Self {
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
    pub fn buffer_size(&self, rows: usize) -> usize {
        self.row_width * rows
    }

    /// Encodes arrays into a buffer using this layout.
    ///
    /// The buffer must be the exact size for encoding the length of the selection.
    pub(crate) fn encode_arrays<B>(
        &self,
        arrays: &[Array<B>],
        selection: impl IntoExactSizeIterator<Item = usize> + Clone,
        buffer: &mut [u8],
    ) -> Result<()>
    where
        B: BufferManager,
    {
        for (array_idx, array) in arrays.iter().enumerate() {
            let phys_type = array.datatype().physical_type();
            let array = array.flatten()?;
            encode_array(phys_type, array_idx, array, selection.clone(), self, buffer)?;
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
        PhysicalType::UntypedNull => std::mem::size_of::<UntypedNull>(), // Zero
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
        PhysicalType::Binary => StringViewMetadataUnion::ENCODE_WIDTH,
        PhysicalType::Utf8 => StringViewMetadataUnion::ENCODE_WIDTH,
        _ => unimplemented!(),
    }
}

/// Encodes a flattened array to a buffer.
///
/// `array_idx` is the index in the batch that this array is for.
fn encode_array<B>(
    phys_type: PhysicalType,
    array_idx: usize,
    array: FlattenedArray<B>,
    selection: impl IntoExactSizeIterator<Item = usize>,
    layout: &RowLayout,
    buffer: &mut [u8],
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
            encode_scalar::<PhysicalUntypedNull, B>(array_idx, array, selection, layout, buffer)
        }
        PhysicalType::Boolean => {
            encode_scalar::<PhysicalBool, B>(array_idx, array, selection, layout, buffer)
        }
        PhysicalType::Int8 => {
            encode_scalar::<PhysicalI8, B>(array_idx, array, selection, layout, buffer)
        }
        PhysicalType::Int16 => {
            encode_scalar::<PhysicalI16, B>(array_idx, array, selection, layout, buffer)
        }
        PhysicalType::Int32 => {
            encode_scalar::<PhysicalI32, B>(array_idx, array, selection, layout, buffer)
        }
        PhysicalType::Int64 => {
            encode_scalar::<PhysicalI64, B>(array_idx, array, selection, layout, buffer)
        }
        PhysicalType::Int128 => {
            encode_scalar::<PhysicalI128, B>(array_idx, array, selection, layout, buffer)
        }
        PhysicalType::UInt8 => {
            encode_scalar::<PhysicalU8, B>(array_idx, array, selection, layout, buffer)
        }
        PhysicalType::UInt16 => {
            encode_scalar::<PhysicalU16, B>(array_idx, array, selection, layout, buffer)
        }
        PhysicalType::UInt32 => {
            encode_scalar::<PhysicalU32, B>(array_idx, array, selection, layout, buffer)
        }
        PhysicalType::UInt64 => {
            encode_scalar::<PhysicalU64, B>(array_idx, array, selection, layout, buffer)
        }
        PhysicalType::UInt128 => {
            encode_scalar::<PhysicalU128, B>(array_idx, array, selection, layout, buffer)
        }
        PhysicalType::Float16 => {
            encode_scalar::<PhysicalF16, B>(array_idx, array, selection, layout, buffer)
        }
        PhysicalType::Float32 => {
            encode_scalar::<PhysicalF32, B>(array_idx, array, selection, layout, buffer)
        }
        PhysicalType::Float64 => {
            encode_scalar::<PhysicalF64, B>(array_idx, array, selection, layout, buffer)
        }
        PhysicalType::Interval => {
            encode_scalar::<PhysicalInterval, B>(array_idx, array, selection, layout, buffer)
        }

        _ => unimplemented!(),
    }
}

fn encode_scalar<S, B>(
    array_idx: usize,
    array: FlattenedArray<B>,
    selection: impl IntoIterator<Item = usize>,
    layout: &RowLayout,
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

            let value_offset = layout.offsets[array_idx] * output_idx + layout.offsets[array_idx];
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

/// Trait for converting self to bytes. Encodings do not require any special
/// properties other than a value always encoding to the same result.
trait Encode {
    const ENCODE_WIDTH: usize;
    fn encode(&self, buf: &mut [u8]);
}

macro_rules! primitive_encode {
    ($type:ty, $encode_width:expr) => {
        impl Encode for $type {
            const ENCODE_WIDTH: usize = $encode_width;

            fn encode(&self, buf: &mut [u8]) {
                let b = self.to_be_bytes();
                buf.copy_from_slice(&b);
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
}

impl Encode for Interval {
    const ENCODE_WIDTH: usize = 16;

    fn encode(&self, buf: &mut [u8]) {
        // TODO: We'll probably need to ensure intervals are normalized.
        self.months.encode(buf);
        self.days.encode(buf);
        self.nanos.encode(buf);
    }
}

impl Encode for StringViewMetadataUnion {
    const ENCODE_WIDTH: usize = 16;

    fn encode(&self, buf: &mut [u8]) {
        let b = self.to_bytes();
        buf.copy_from_slice(&b);
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
}

#[cfg(test)]
mod tests {
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;

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
    fn encode_single_i32() {
        let layout = RowLayout::new(vec![DataType::Int32]);

        let mut buf = vec![0; layout.buffer_size(3)];
        let array = Array::try_from_iter([1, 2, 3]).unwrap();

        layout.encode_arrays(&[array], 0..3, &mut buf).unwrap();
    }
}
