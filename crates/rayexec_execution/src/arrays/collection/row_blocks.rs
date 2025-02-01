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

#[derive(Debug)]
pub struct BlockAppendState {
    /// Pointers to the start of each row to write to.
    pub row_pointers: Vec<*mut u8>,
    /// Pointers to the start of each location in the heap for writing nested or
    /// varlen data.
    pub heap_pointers: Vec<HeapMutPtr>,
}

#[derive(Debug)]
pub struct BlockReadState {
    /// Pointers to the start of each row to read from.
    pub row_pointers: Vec<*const u8>,
}

#[derive(Debug)]
pub struct RowBlocks<B: BufferManager> {
    pub manager: B,
    pub row_capacity: usize,
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

    /// Prepares the read state for a single row block.
    ///
    /// `selection` selects which rows from the row block to read.
    pub fn prepare_read(
        &self,
        state: &mut BlockReadState,
        row_block_idx: usize,
        selection: impl IntoIterator<Item = usize>,
    ) -> Result<()> {
        state.row_pointers.clear();

        let block = &self.row_blocks[row_block_idx];

        for sel_idx in selection {
            debug_assert!(sel_idx < block.num_rows(self.layout.row_width));

            let ptr = block.as_ptr();
            let ptr = unsafe { ptr.byte_add(self.layout.row_width * sel_idx) };

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
    pub fn prepare_append(
        &mut self,
        state: &mut BlockAppendState,
        rows: usize,
        heap_sizes: Option<&[usize]>,
    ) -> Result<()> {
        state.row_pointers.clear();
        state.heap_pointers.clear();

        // Ensure we have at least one row block to work with.
        if self.row_blocks.is_empty() {
            let block = Block::try_new_row_block(&self.manager, &self.layout, self.row_capacity)?;
            self.row_blocks.push(block);
        }

        // Start with last block.
        let mut block_idx = self.row_blocks.len() - 1;

        let mut remaining = rows;
        let mut input_offset = 0;

        // Handle generating pointers to the row blocks.
        while remaining > 0 {
            // // Track this block in the chunk.
            // chunk.row_blocks.insert(block_idx);

            let block = self.row_blocks.get_mut(block_idx).expect("block to exist");
            let copy_count =
                usize::min(block.remaing_row_capacity(self.layout.row_width), remaining);

            // Create pointers to row locations.
            state
                .row_pointers
                .extend((input_offset..(input_offset + copy_count)).map(|offset| {
                    let ptr = block.as_mut_ptr();
                    // SAFETY: We checked that the block we're creating pointers
                    // for can hold `copy_count` number of rows.
                    //
                    // Assumes that we allocated the correct size for the buffer.
                    unsafe { ptr.byte_add(self.layout.row_width * offset) }
                }));

            input_offset += copy_count;
            remaining -= copy_count;

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

                offset += heap_size;
            }
        }

        Ok(())
    }

    /// Get a pointer to a heap block at the given byte offset.
    pub(crate) unsafe fn heap_ptr(&self, heap_idx: usize, offset: usize) -> *const u8 {
        self.heap_blocks[heap_idx].as_ptr().byte_add(offset)
    }
}

#[derive(Debug)]
pub struct Block<B: BufferManager> {
    /// Raw byte data.
    data: TypedRawBuffer<u8, B>,
    /// Number of filled bytes.
    filled_bytes: usize,
    _pinned: PhantomPinned, // TODO: Probably Pin some stuff. Currently does nothing.
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
            filled_bytes: 0,
            _pinned: PhantomPinned,
        })
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data.as_mut_ptr()
    }

    fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    const fn num_rows(&self, row_width: usize) -> usize {
        self.filled_bytes / row_width
    }

    const fn remaining_byte_capacity(&self) -> usize {
        self.data.capacity() - self.filled_bytes
    }

    const fn remaing_row_capacity(&self, row_width: usize) -> usize {
        self.remaining_byte_capacity() / row_width
    }
}
