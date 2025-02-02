use std::marker::PhantomPinned;

use rayexec_error::{RayexecError, Result};

use super::row_layout::RowLayout;
use crate::arrays::array::buffer_manager::BufferManager;
use crate::arrays::array::raw::TypedRawBuffer;

/// Wrapper around a plan pointer to a heap block to also give us information
/// about which heap block we're writing to.
///
/// This is used when constructing the string view that gets written to the row.
#[derive(Debug, Copy, Clone)]
pub struct HeapMutPtr {
    /// The pointer to the where to write in the heap.
    pub ptr: *mut u8,
    /// The index of the heap block.
    pub heap_idx: usize,
    /// The offset within the block that corresponds to the pointer.
    pub offset: usize,
}

impl HeapMutPtr {
    pub unsafe fn byte_add(self, count: usize) -> Self {
        HeapMutPtr {
            ptr: self.ptr.byte_add(count),
            heap_idx: self.heap_idx,
            offset: self.offset + count,
        }
    }
}

/// State used during appending data to the row collection.
#[derive(Debug)]
pub struct BlockAppendState {
    /// Pointers to the start of each row to write to.
    pub row_pointers: Vec<*mut u8>,
    /// Pointers to the start of each location in the heap for writing nested or
    /// varlen data.
    pub heap_pointers: Vec<HeapMutPtr>,
}

impl BlockAppendState {
    pub fn clear(&mut self) {
        self.row_pointers.clear();
        self.heap_pointers.clear();
    }
}

#[derive(Debug)]
pub struct BlockReadState {
    /// Pointers to the start of each row to read from.
    pub row_pointers: Vec<*const u8>,
}

impl BlockReadState {
    /// Clear all pointers from this state.
    pub fn clear(&mut self) {
        self.row_pointers.clear();
    }
}

#[derive(Debug)]
pub struct RowBlocks<B: BufferManager> {
    pub manager: B,
    /// Row capacity per row block. Does not impact size of heap blocks.
    pub row_capacity: usize,
    /// Layout these blocks are initialized with.
    pub layout: RowLayout,
    /// Blocks for encoded rows.
    pub row_blocks: Vec<Block<B>>,
    /// Blocks for varlen and nested data.
    pub heap_blocks: Vec<Block<B>>,
}

impl<B> RowBlocks<B>
where
    B: BufferManager,
{
    const MAX_HEAP_SIZE: usize = 1024 * 1024 * 1024 * 32; // 32GB

    pub fn new(manager: B, layout: RowLayout, row_capacity: usize) -> Self {
        RowBlocks {
            manager,
            row_capacity,
            layout,
            row_blocks: Vec::new(),
            heap_blocks: Vec::new(),
        }
    }

    pub fn reserved_row_count(&self) -> usize {
        self.row_blocks
            .iter()
            .map(|b| b.num_rows(self.layout.row_width))
            .sum()
    }

    pub fn rows_in_row_block(&self, row_block_idx: usize) -> usize {
        self.row_blocks[row_block_idx].num_rows(self.layout.row_width)
    }

    pub fn num_row_blocks(&self) -> usize {
        self.row_blocks.len()
    }

    pub fn num_heap_blocks(&self) -> usize {
        self.heap_blocks.len()
    }

    /// Prepares the read state for a single row block.
    ///
    /// `selection` selects which rows from the row block to read.
    ///
    /// This will append the pointers to the current state.
    pub fn prepare_read(
        &self,
        state: &mut BlockReadState,
        row_block_idx: usize,
        selection: impl IntoIterator<Item = usize>,
    ) -> Result<()> {
        let block = &self.row_blocks[row_block_idx];

        for sel_idx in selection {
            debug_assert!(sel_idx < block.num_rows(self.layout.row_width));

            let ptr = block.as_ptr();
            let ptr = unsafe { ptr.byte_add(self.layout.row_width * sel_idx) };
            debug_assert!(block.data.raw.contains_addr(ptr.addr()));

            state.row_pointers.push(ptr)
        }

        Ok(())
    }

    /// Prepares an append to this set of row blocks.
    ///
    /// This will allocate additional blocks to fit an additional `row` number
    /// of rows.
    ///
    /// `heap_sizes` indicates the number of bytes each row will need in the
    /// heap. May be None if columns don't require heap blocks.
    ///
    /// The pointers to the blocks will be placed in the append state. The row
    /// chunk will also be updated to indicate the set of blocks that this
    /// append will reference.
    ///
    /// This will append the pointers to the current state.
    pub fn prepare_append(
        &mut self,
        state: &mut BlockAppendState,
        rows: usize,
        heap_sizes: Option<&[usize]>,
    ) -> Result<()> {
        // Ensure we have at least one row block to work with.
        if self.row_blocks.is_empty() {
            let block = Block::try_new_row_block(&self.manager, &self.layout, self.row_capacity)?;
            self.row_blocks.push(block);
        }

        // Start with last block.
        let mut block_idx = self.row_blocks.len() - 1;

        let mut remaining = rows;

        // Handle generating pointers to the row blocks.
        while remaining > 0 {
            let block = self.row_blocks.get_mut(block_idx).expect("block to exist");
            let copy_count =
                usize::min(block.remaing_row_capacity(self.layout.row_width), remaining);

            let block_offset = block.num_rows(self.layout.row_width);

            // Create pointers to row locations.
            state
                .row_pointers
                .extend((block_offset..(block_offset + copy_count)).map(|offset| {
                    let ptr = block.as_mut_ptr();
                    // SAFETY: We checked that the block we're creating pointers
                    // for can hold `copy_count` number of rows.
                    //
                    // Assumes that we allocated the correct size for the buffer.
                    let ptr = unsafe { ptr.byte_add(self.layout.row_width * offset) };
                    debug_assert!(block.data.raw.contains_addr(ptr.addr()));

                    ptr
                }));

            remaining -= copy_count;
            block.reserved_bytes += copy_count * self.layout.row_width;

            if remaining > 0 {
                // Means we filled the block to max capacity. Allocate new block
                // and update block idx we're pointing to.
                let block =
                    Block::try_new_row_block(&self.manager, &self.layout, self.row_capacity)?;
                self.row_blocks.push(block);
                block_idx = self.row_blocks.len() - 1
            }
        }

        // Generate pointers to heap chunks if we're inserting varlen data.
        if let Some(heap_sizes) = heap_sizes {
            let total_heap_size: usize = heap_sizes.iter().sum();

            // TODO: Currently this just allocates a heap block for each set of
            // rows. Not sure if we want to try to be smarter about that.

            if total_heap_size > Self::MAX_HEAP_SIZE {
                return Err(RayexecError::new("Required heap allocation exceeds max")
                    .with_field("wanted", total_heap_size)
                    .with_field("max", Self::MAX_HEAP_SIZE));
            }

            // Create new heap block, no initialization needs to happen.
            let block = Block::try_new(&self.manager, total_heap_size)?;
            self.heap_blocks.push(block);

            let heap_idx = self.heap_blocks.len() - 1;
            let block = self.heap_blocks.last_mut().expect("heap block to exist");

            // Create pointer locations.
            let mut offset = 0;
            for &heap_size in heap_sizes {
                let ptr = block.as_mut_ptr();
                // SAFETEY: We should have allocated the exact size needed for
                // the heap block. Everything should be contained within that
                // block.
                let ptr = unsafe { ptr.byte_add(offset) };
                state.heap_pointers.push(HeapMutPtr {
                    ptr,
                    heap_idx,
                    offset,
                });
                debug_assert!(block.data.raw.contains_addr(ptr.addr()));
                block.reserved_bytes += heap_size;

                offset += heap_size;
            }
        }

        Ok(())
    }

    /// Get a pointer to a heap block at the given byte offset.
    pub(crate) unsafe fn heap_ptr(&self, heap_idx: usize, offset: usize) -> *const u8 {
        let heap_block = &self.heap_blocks[heap_idx];
        let ptr = heap_block.as_ptr().byte_add(offset);
        debug_assert!(heap_block.data.raw.contains_addr(ptr.addr()));
        ptr
    }
}

#[derive(Debug)]
pub struct Block<B: BufferManager> {
    /// Raw byte data.
    data: TypedRawBuffer<u8, B>,
    /// Bytes that have been reserved for writes.
    reserved_bytes: usize,
}

impl<B> Block<B>
where
    B: BufferManager,
{
    /// Creates a new row block.
    ///
    /// This will initialize all validity metadata bytes to `u8::MAX`. This
    /// serves two purposes:
    ///
    /// - Ensures that the memory is initialized
    /// - Prevent needing to explicitly set columns as valid
    fn try_new_row_block(manager: &B, layout: &RowLayout, row_capacity: usize) -> Result<Self> {
        let mut block = Block::try_new(manager, layout.buffer_size(row_capacity))?;
        let buffer = block.data.as_slice_mut();

        for row in 0..row_capacity {
            let validity_offset = layout.row_width * row;
            let out_buf = &mut buffer[validity_offset..(validity_offset + layout.validity_width)];
            out_buf.fill(u8::MAX);
        }

        Ok(block)
    }

    fn try_new(manager: &B, byte_capacity: usize) -> Result<Self> {
        let data = TypedRawBuffer::try_with_capacity(manager, byte_capacity)?;
        Ok(Block {
            data,
            reserved_bytes: 0,
        })
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data.as_mut_ptr()
    }

    fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    const fn num_rows(&self, row_width: usize) -> usize {
        self.reserved_bytes / row_width
    }

    const fn remaining_byte_capacity(&self) -> usize {
        self.data.capacity() - self.reserved_bytes
    }

    const fn remaing_row_capacity(&self, row_width: usize) -> usize {
        self.remaining_byte_capacity() / row_width
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::array::buffer_manager::NopBufferManager;
    use crate::arrays::datatype::DataType;

    #[test]
    fn prepare_append_allocate_single_row_block() {
        let layout = RowLayout::new([DataType::Int32]);
        let mut blocks = RowBlocks::new(NopBufferManager, layout, 16);

        let mut append_state = BlockAppendState {
            row_pointers: Vec::new(),
            heap_pointers: Vec::new(),
        };
        blocks.prepare_append(&mut append_state, 4, None).unwrap();

        assert_eq!(4, append_state.row_pointers.len());
        assert_eq!(1, blocks.num_row_blocks());
        assert_eq!(0, blocks.num_heap_blocks());
        assert_eq!(4, blocks.reserved_row_count());

        let mut read_state = BlockReadState {
            row_pointers: Vec::new(),
        };
        blocks.prepare_read(&mut read_state, 0, 0..4).unwrap();

        assert_eq!(4, read_state.row_pointers.len());
    }

    #[test]
    fn prepare_append_allocate_multiple_row_blocks() {
        let layout = RowLayout::new([DataType::Int32]);
        let mut blocks = RowBlocks::new(NopBufferManager, layout, 16);

        let mut append_state = BlockAppendState {
            row_pointers: Vec::new(),
            heap_pointers: Vec::new(),
        };

        blocks.prepare_append(&mut append_state, 24, None).unwrap();

        assert_eq!(24, append_state.row_pointers.len());
        assert_eq!(2, blocks.num_row_blocks());
        assert_eq!(0, blocks.num_heap_blocks());
        assert_eq!(24, blocks.reserved_row_count());

        let mut read_state = BlockReadState {
            row_pointers: Vec::new(),
        };
        blocks.prepare_read(&mut read_state, 0, 0..16).unwrap();

        assert_eq!(16, read_state.row_pointers.len());
    }
}
