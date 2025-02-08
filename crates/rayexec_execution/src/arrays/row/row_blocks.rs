use rayexec_error::{RayexecError, Result};

use super::block::{Block, FixedSizedBlockInitializer, RowLayoutBlockInitializer};
use super::block_scanner::BlockScanState;
use super::row_layout::RowLayout;
use crate::arrays::array::buffer_manager::BufferManager;

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
pub struct RowBlocks<B: BufferManager, I: FixedSizedBlockInitializer> {
    pub manager: B,
    /// Row capacity per row block. Does not impact size of heap blocks.
    pub row_capacity: usize,
    /// Size in bytes of a single row stored in a fixed-size block.
    pub row_width: usize,
    /// Fixed size blocks initializer.
    pub initializer: I,
    /// Blocks for encoded rows.
    pub row_blocks: Vec<Block<B>>,
    /// Blocks for varlen and nested data.
    pub heap_blocks: Vec<Block<B>>,
}

impl<B> RowBlocks<B, RowLayoutBlockInitializer>
where
    B: BufferManager,
{
    pub fn new_using_row_layout(manager: B, row_layout: RowLayout, row_capacity: usize) -> Self {
        let row_width = row_layout.row_width;
        let initializer = RowLayoutBlockInitializer::new(row_layout);
        Self::new(manager, initializer, row_width, row_capacity)
    }
}

impl<B, I> RowBlocks<B, I>
where
    B: BufferManager,
    I: FixedSizedBlockInitializer,
{
    const MAX_HEAP_SIZE: usize = 1024 * 1024 * 1024 * 32; // 32GB

    pub fn new(manager: B, initializer: I, row_width: usize, row_capacity: usize) -> Self {
        RowBlocks {
            manager,
            row_capacity,
            row_width,
            initializer,
            row_blocks: Vec::new(),
            heap_blocks: Vec::new(),
        }
    }

    pub fn reserved_row_count(&self) -> usize {
        self.row_blocks
            .iter()
            .map(|b| b.num_rows(self.row_width))
            .sum()
    }

    pub fn rows_in_row_block(&self, row_block_idx: usize) -> usize {
        self.row_blocks[row_block_idx].num_rows(self.row_width)
    }

    pub fn num_row_blocks(&self) -> usize {
        self.row_blocks.len()
    }

    pub fn num_heap_blocks(&self) -> usize {
        self.heap_blocks.len()
    }

    /// Moves the blocks from other to self.
    ///
    /// This does not verify or update any data inside the blocks.
    pub fn merge_blocks(&mut self, other: Self) {
        self.row_blocks.extend(other.row_blocks);
        self.heap_blocks.extend(other.heap_blocks);
    }

    /// Allocates a new fixed-sized block based on the configure row width and
    /// capacity.
    ///
    /// This will initialize the block before returning it.
    fn allocate_and_init_fixed_size_block(&self) -> Result<Block<B>> {
        let buf_size = self.row_width * self.row_capacity;
        let block = Block::try_new(&self.manager, buf_size)?;
        self.initializer.initialize(block)
    }

    /// Prepares the read state for a single row block.
    ///
    /// `selection` selects which rows from the row block to read.
    ///
    /// This will clear any existing pointers on the scan state.
    pub fn prepare_read(
        &self,
        state: &mut BlockScanState,
        row_block_idx: usize,
        selection: impl IntoIterator<Item = usize>,
    ) -> Result<()> {
        let block = &self.row_blocks[row_block_idx];
        unsafe {
            state.prepare_block_scan(block, self.row_width, selection, true);
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
            let block = self.allocate_and_init_fixed_size_block()?;
            self.row_blocks.push(block);
        }

        // Start with last block.
        let mut block_idx = self.row_blocks.len() - 1;

        let mut remaining = rows;

        // Handle generating pointers to the row blocks.
        while remaining > 0 {
            let block = self.row_blocks.get_mut(block_idx).expect("block to exist");
            let copy_count = usize::min(block.remaing_row_capacity(self.row_width), remaining);

            let block_offset = block.num_rows(self.row_width);

            // Create pointers to row locations.
            state
                .row_pointers
                .extend((block_offset..(block_offset + copy_count)).map(|offset| {
                    let ptr = block.as_mut_ptr();
                    // SAFETY: We checked that the block we're creating pointers
                    // for can hold `copy_count` number of rows.
                    //
                    // Assumes that we allocated the correct size for the buffer.
                    let ptr = unsafe { ptr.byte_add(self.row_width * offset) };
                    debug_assert!(block.data.raw.contains_addr(ptr.addr()));

                    ptr
                }));

            remaining -= copy_count;
            block.reserved_bytes += copy_count * self.row_width;

            if remaining > 0 {
                // Means we filled the block to max capacity. Allocate new block
                // and update block idx we're pointing to.
                let block = self.allocate_and_init_fixed_size_block()?;
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

            let block_ptr = block.as_mut_ptr();
            // Create pointer locations.
            let mut offset = 0;
            for &heap_size in heap_sizes {
                // SAFETEY: We should have allocated the exact size needed for
                // the heap block. Everything should be contained within that
                // block.
                let ptr = unsafe { block_ptr.byte_add(offset) };
                state.heap_pointers.push(HeapMutPtr {
                    ptr,
                    heap_idx,
                    offset,
                });
                // Assert that this block contains the computed pointer. Note
                // that for 0-sized heap requirements, the 'contains' check may
                // fail since it may point to the end of the allocation (which
                // is fine, we're not writing to it in that case). The 0 check
                // just catches this.
                debug_assert!(
                    heap_size == 0 || block.data.raw.contains_addr(ptr.addr()),
                    "ptr: {}, block: {}",
                    ptr.addr(),
                    block_ptr.addr(),
                );
                block.reserved_bytes += heap_size;

                offset += heap_size;
            }
        }

        Ok(())
    }

    /// Takes both the row and heap blocks and returns them as (row_blocks,
    /// heap_blocks).
    ///
    /// This collection can continue to be used after taking the blocks.
    pub fn take_blocks(&mut self) -> (Vec<Block<B>>, Vec<Block<B>>) {
        let row_blocks = std::mem::take(&mut self.row_blocks);
        let heap_blocks = std::mem::take(&mut self.heap_blocks);
        (row_blocks, heap_blocks)
    }

    /// Get a pointer to a heap block at the given byte offset.
    pub(crate) unsafe fn heap_ptr(&self, heap_idx: usize, offset: usize) -> *const u8 {
        let heap_block = &self.heap_blocks[heap_idx];
        let ptr = heap_block.as_ptr().byte_add(offset);
        debug_assert!(heap_block.data.raw.contains_addr(ptr.addr()));
        ptr
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
        let mut blocks = RowBlocks::new_using_row_layout(NopBufferManager, layout, 16);

        let mut append_state = BlockAppendState {
            row_pointers: Vec::new(),
            heap_pointers: Vec::new(),
        };
        blocks.prepare_append(&mut append_state, 4, None).unwrap();

        assert_eq!(4, append_state.row_pointers.len());
        assert_eq!(1, blocks.num_row_blocks());
        assert_eq!(0, blocks.num_heap_blocks());
        assert_eq!(4, blocks.reserved_row_count());

        let mut read_state = BlockScanState {
            row_pointers: Vec::new(),
        };
        blocks.prepare_read(&mut read_state, 0, 0..4).unwrap();

        assert_eq!(4, read_state.row_pointers.len());
    }

    #[test]
    fn prepare_append_allocate_multiple_row_blocks() {
        let layout = RowLayout::new([DataType::Int32]);
        let mut blocks = RowBlocks::new_using_row_layout(NopBufferManager, layout, 16);

        let mut append_state = BlockAppendState {
            row_pointers: Vec::new(),
            heap_pointers: Vec::new(),
        };

        blocks.prepare_append(&mut append_state, 24, None).unwrap();

        assert_eq!(24, append_state.row_pointers.len());
        assert_eq!(2, blocks.num_row_blocks());
        assert_eq!(0, blocks.num_heap_blocks());
        assert_eq!(24, blocks.reserved_row_count());

        let mut read_state = BlockScanState {
            row_pointers: Vec::new(),
        };
        blocks.prepare_read(&mut read_state, 0, 0..16).unwrap();

        assert_eq!(16, read_state.row_pointers.len());
    }
}
