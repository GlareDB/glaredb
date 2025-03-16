use std::fmt::Debug;

use glaredb_error::Result;

use super::aggregate_layout::AggregateLayout;
use super::row_layout::RowLayout;
use crate::buffer::buffer_manager::AsRawBufferManager;
use crate::buffer::typed::{AlignedBuffer, TypedBuffer};

/// Describes how we initialize fixed sized blocks.
pub trait FixedSizedBlockInitializer: Debug {
    /// Initialize a fixed-sized block.
    fn initialize(&self, block: Block) -> Result<Block>;
}

/// Initialize a validity bytes in a block conforming to some row layout.
///
/// This will initialize all validity metadata bytes to `u8::MAX` after creating
/// the new block. This serves two purposes:
///
/// - Ensures that the memory is initialized
/// - Prevent needing to explicitly set columns as valid
///
/// This is suitable to use with `RowLayout` and `AggregateLayout` (which is
/// really just `RowLayout` suffixed with aggregate states).
#[derive(Debug, Clone)]
pub struct ValidityInitializer {
    /// Size in bytes of a single row.
    ///
    /// Note for aggregate layouts, this should include the aggregate state
    /// width as well.
    pub row_width: usize,
    /// Size in bytes for the validity width.
    pub validity_width: usize,
}

impl ValidityInitializer {
    pub fn from_row_layout(layout: &RowLayout) -> Self {
        ValidityInitializer {
            row_width: layout.row_width,
            validity_width: layout.validity_width,
        }
    }

    pub fn from_aggregate_layout(layout: &AggregateLayout) -> Self {
        ValidityInitializer {
            row_width: layout.row_width,
            validity_width: layout.groups.validity_width,
        }
    }
}

impl FixedSizedBlockInitializer for ValidityInitializer {
    fn initialize(&self, mut block: Block) -> Result<Block> {
        let row_capacity = block.remaing_row_capacity(self.row_width);
        let buffer = block.data.as_slice_mut();

        for row in 0..row_capacity {
            let validity_offset = self.row_width * row;
            let out_buf = &mut buffer[validity_offset..(validity_offset + self.validity_width)];
            out_buf.fill(u8::MAX);
        }

        Ok(block)
    }
}

/// A no-op initializer. Just returns the block as-is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NopInitializer;

impl FixedSizedBlockInitializer for NopInitializer {
    fn initialize(&self, block: Block) -> Result<Block> {
        Ok(block)
    }
}

#[derive(Debug)]
pub struct Block {
    /// Raw byte data.
    pub data: TypedBuffer<u8>,
    /// Bytes that have been reserved for writes.
    pub reserved_bytes: usize,
}

impl Block {
    /// Concats many blocks into a single block.
    ///
    /// This will allocate a block of the exact capacity needed, and each input
    /// block will be written to the output block in order.
    // TODO: Does this need to care about alignment? Probably want to keep a
    // bool on the block indicating if there's a custom alignment, and error if
    // there is for now.
    pub fn concat(manager: &impl AsRawBufferManager, blocks: Vec<Self>) -> Result<Self> {
        let capacity: usize = blocks.iter().map(|block| block.reserved_bytes).sum();
        let mut out_buf = TypedBuffer::try_with_capacity(manager, capacity)?;

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

    /// Try to create a new block with the given byte capacity.
    ///
    /// This sets the initial reserved bytes to 0 indicating this block is
    /// "empty".
    ///
    /// If `alignment` is provided, the buffer will be allocated with that
    /// alignment.
    pub fn try_new_reserve_none(
        manager: &impl AsRawBufferManager,
        byte_capacity: usize,
        alignment: Option<usize>,
    ) -> Result<Self> {
        let data = match alignment {
            Some(align) => {
                AlignedBuffer::try_with_capacity_and_alignment(manager, byte_capacity, align)?
                    .into_typed_raw_buffer()
            }
            None => TypedBuffer::try_with_capacity(manager, byte_capacity)?,
        };

        Ok(Block {
            data,
            reserved_bytes: 0,
        })
    }

    /// Like `try_new`, but sets reserved bytes the provided byte capacity. This
    /// marks the block as "full".
    pub fn try_new_reserve_all(
        manager: &impl AsRawBufferManager,
        byte_capacity: usize,
    ) -> Result<Self> {
        let data = TypedBuffer::try_with_capacity(manager, byte_capacity)?;
        Ok(Block {
            data,
            reserved_bytes: byte_capacity,
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
    use crate::buffer::buffer_manager::NopBufferManager;

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
            let mut block = Block::try_new_reserve_none(&NopBufferManager, 128, None).unwrap();
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
