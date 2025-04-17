use glaredb_core::arrays::array::Array;
use glaredb_core::buffer::buffer_manager::NopBufferManager;
use glaredb_core::buffer::typed::TypedBuffer;
use glaredb_error::Result;

use super::Definitions;
use crate::column::encoding::delta_binary_packed::DeltaBinaryPackedValueDecoder;
use crate::column::read_buffer::ReadCursor;

#[derive(Debug)]
pub struct DeltaLengthByteArrayDecoder {
    /// If we should verify that the bytes we read is valid utf8.
    verify_utf8: bool,
    /// Number of values we're decoding.
    num_values: usize,
    /// Decoded lengths from the beginning of the buffer.
    lengths: TypedBuffer<i32>,
    /// Cursor pointing to some byte offset.
    cursor: ReadCursor,
}

impl DeltaLengthByteArrayDecoder {
    pub fn try_new(cursor: ReadCursor, num_values: usize, verify_utf8: bool) -> Result<Self> {
        // TODO: Not Nop
        let mut lengths = TypedBuffer::<i32>::try_with_capacity(&NopBufferManager, num_values)?;

        let len_slice = &mut lengths.as_slice_mut()[..num_values]; // May overallocate
        let mut dec = DeltaBinaryPackedValueDecoder::<i32>::try_new(cursor)?;
        dec.read(len_slice)?;

        let cursor = dec.try_into_cursor()?;

        Ok(DeltaLengthByteArrayDecoder {
            verify_utf8,
            num_values,
            lengths,
            cursor,
        })
    }

    pub fn read(
        &mut self,
        definitions: Definitions,
        output: &mut Array,
        offset: usize,
        count: usize,
    ) -> Result<()> {
        unimplemented!()
    }
}
