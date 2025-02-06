use std::fmt::Debug;

use rayexec_error::Result;

use super::row_layout::RowLayout;
use crate::arrays::array::buffer_manager::BufferManager;
use crate::arrays::array::raw::TypedRawBuffer;

/// Describes how we initialize fixed sized blocks.
pub trait FixedSizedBlockInitializer: Debug {
    /// Initialize a fixed-sized block.
    fn initialize<B>(&self, block: Block<B>) -> Result<Block<B>>
    where
        B: BufferManager;
}

/// Initialize blocks based on a row layout.
///
/// This will initialize all validity metadata bytes to `u8::MAX` after creating
/// the new block. This serves two purposes:
///
/// - Ensures that the memory is initialized
/// - Prevent needing to explicitly set columns as valid
#[derive(Debug, Clone)]
pub struct RowLayoutBlockInitializer {
    pub layout: RowLayout,
}

impl RowLayoutBlockInitializer {
    pub const fn new(layout: RowLayout) -> Self {
        RowLayoutBlockInitializer { layout }
    }
}

impl FixedSizedBlockInitializer for RowLayoutBlockInitializer {
    fn initialize<B>(&self, mut block: Block<B>) -> Result<Block<B>>
    where
        B: BufferManager,
    {
        let row_capacity = block.remaing_row_capacity(self.layout.row_width);
        let buffer = block.data.as_slice_mut();

        for row in 0..row_capacity {
            let validity_offset = self.layout.row_width * row;
            let out_buf =
                &mut buffer[validity_offset..(validity_offset + self.layout.validity_width)];
            out_buf.fill(u8::MAX);
        }

        Ok(block)
    }
}

/// A no-op initializer. Just returns the block as-is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NopInitializer;

impl FixedSizedBlockInitializer for NopInitializer {
    fn initialize<B>(&self, block: Block<B>) -> Result<Block<B>>
    where
        B: BufferManager,
    {
        Ok(block)
    }
}

#[derive(Debug)]
pub struct Block<B: BufferManager> {
    /// Raw byte data.
    pub data: TypedRawBuffer<u8, B>,
    /// Bytes that have been reserved for writes.
    pub reserved_bytes: usize,
}

impl<B> Block<B>
where
    B: BufferManager,
{
    /// Concats many blocks into a single block.
    ///
    /// This will allocate a block of the exact capacity needed, and each input
    /// block will be written to the output block in order.
    pub fn concat(manager: &B, blocks: Vec<Self>) -> Result<Self> {
        let capacity: usize = blocks.iter().map(|block| block.reserved_bytes).sum();
        let mut out_buf = TypedRawBuffer::try_with_capacity(manager, capacity)?;

        let out_slice = out_buf.as_slice_mut();

        let mut write_offset = 0;
        for block in blocks {
            let src_slice = &block.data.as_slice()[0..block.reserved_bytes];
            let dest_slice = &mut out_slice[write_offset..write_offset + block.reserved_bytes];

            dest_slice.copy_from_slice(src_slice);

            write_offset += block.reserved_bytes;
        }

        Ok(Block {
            data: out_buf,
            reserved_bytes: capacity,
        })
    }

    pub fn try_new(manager: &B, byte_capacity: usize) -> Result<Self> {
        let data = TypedRawBuffer::try_with_capacity(manager, byte_capacity)?;
        Ok(Block {
            data,
            reserved_bytes: 0,
        })
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data.as_mut_ptr()
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    pub const fn num_rows(&self, row_width: usize) -> usize {
        self.reserved_bytes / row_width
    }

    pub const fn remaining_byte_capacity(&self) -> usize {
        self.data.capacity() - self.reserved_bytes
    }

    pub const fn remaing_row_capacity(&self, row_width: usize) -> usize {
        self.remaining_byte_capacity() / row_width
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::array::buffer_manager::NopBufferManager;

    #[test]
    fn concat_empty() {
        let block = Block::concat(&NopBufferManager, vec![]).unwrap();
        assert_eq!(0, block.reserved_bytes);
        assert_eq!(0, block.data.capacity());
    }

    #[test]
    fn concat_many() {
        let mut blocks = Vec::new();
        for i in 0..4 {
            let mut block = Block::try_new(&NopBufferManager, 128).unwrap();
            let s = &mut block.data.as_slice_mut()[0..i];
            for b in s {
                *b = i as u8;
            }
            block.reserved_bytes = i;
            blocks.push(block);
        }

        let concat = Block::concat(&NopBufferManager, blocks).unwrap();
        assert_eq!(6, concat.reserved_bytes);

        let s = concat.data.as_slice();
        assert_eq!(&[1, 2, 2, 3, 3, 3], s);
    }
}
