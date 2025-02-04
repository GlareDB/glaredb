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
    pub fn try_new(manager: &B, byte_capacity: usize) -> Result<Self> {
        let data = TypedRawBuffer::try_with_capacity(manager, byte_capacity)?;
        Ok(Block {
            data,
            reserved_bytes: 0,
        })
    }

    pub fn data(&self) -> &TypedRawBuffer<u8, B> {
        &self.data
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
