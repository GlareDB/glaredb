use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, VecDeque};

use rayexec_error::Result;

use super::sort_data::{SortBlock, SortData};
use super::sort_layout::SortLayout;
use crate::arrays::buffer_manager::BufferManager;

/// A block containing sorted rows that's being merged with other blocks.
#[derive(Debug)]
pub struct MergingSortBlock<B: BufferManager> {
    /// The current index in the block that we're comparing.
    pub curr_idx: usize,
    /// The block we're merging.
    pub block: SortBlock<B>,
}

#[derive(Debug)]
pub struct MergeQueue<B: BufferManager> {
    pub exhausted: bool,
    pub current: MergingSortBlock<B>,
    pub remaining: Vec<SortBlock<B>>, // Pop from back to front.
}

impl<B> MergeQueue<B>
where
    B: BufferManager,
{
    fn prepare_next_row(&mut self) {
        if self.exhausted {
            return;
        }

        self.current.curr_idx += 1;
        if self.current.curr_idx >= self.current.block.row_count() {
            // Get next block in queue.
            loop {
                match self.remaining.pop() {
                    Some(block) => {
                        if block.block.row_count() == 0 {
                            // Skip empty blocks.
                            // TODO: Check if this is even valid.
                            continue;
                        }
                        self.current = MergingSortBlock { curr_idx: 0, block };
                        return;
                    }
                    None => {
                        self.exhausted = true;
                        return;
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct Merger<B: BufferManager> {
    pub queues: Vec<MergeQueue<B>>,
    pub layout: SortLayout,
    pub out_capacity: usize,
}

impl<B> Merger<B>
where
    B: BufferManager,
{
    /// Do a single round of merging.
    pub fn merge_round(&mut self, manager: &B) -> Result<Option<SortBlock<B>>> {
        let mut out_block = SortBlock::new(manager, &self.layout, self.out_capacity)?;

        // Min heap containing at most one entry from each queue of blocks we're
        // merging.
        //
        // When we pop an entry, the next element from the queue that the popped
        // entry is from will be inserted.
        let mut min_heap: BinaryHeap<Reverse<HeapEntry<B>>> = BinaryHeap::with_capacity(self.queues.len());

        // Init heap.
        for queue in &mut self.queues {
            if queue.exhausted {
                continue;
            }

            min_heap.push(Reverse(HeapEntry {
                row_idx: queue.current.curr_idx,
                queue,
            }));
        }

        for row_idx in 0..self.out_capacity {
            let ent = match min_heap.pop() {
                Some(ent) => ent,
                None => {
                    // If heap is empty, we exhausted all queues. If out is
                    // empty, then just return None.
                    if out_block.row_count() == 0 {
                        return Ok(None);
                    } else {
                        return Ok(Some(out_block));
                    }
                }
            };

            // Copy the row to out.
            out_block.copy_row_from_other(row_idx, &ent.0.queue.current.block, ent.0.row_idx)?;

            // Get next entry for the queue and put into heap.
            let queue = ent.0.queue;
            queue.prepare_next_row();

            if queue.exhausted {
                // Do nothing, no more entries from this queue.
                continue;
            }

            let ent = Reverse(HeapEntry {
                row_idx: queue.current.curr_idx,
                queue,
            });
            min_heap.push(ent);
        }

        Ok(Some(out_block))
    }
}

/// Entry with the heap representing a block's row.
///
/// Eq and Ord comparisons delegate the key buffer this entry represents.
#[derive(Debug)]
struct HeapEntry<'a, B: BufferManager> {
    /// The queue this entry was from.
    queue: &'a mut MergeQueue<B>,
    /// Row index within the block this entry is for.
    row_idx: usize,
}

impl<'a, B> HeapEntry<'a, B>
where
    B: BufferManager,
{
    fn get_sort_key_buf(&self) -> &[u8] {
        self.queue.current.block.get_sort_key_buf(self.row_idx)
    }
}

impl<'a, B> PartialEq for HeapEntry<'a, B>
where
    B: BufferManager,
{
    fn eq(&self, other: &Self) -> bool {
        self.get_sort_key_buf().eq(other.get_sort_key_buf())
    }
}

impl<'a, B> Eq for HeapEntry<'a, B> where B: BufferManager {}

impl<'a, B> PartialOrd for HeapEntry<'a, B>
where
    B: BufferManager,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.get_sort_key_buf().partial_cmp(other.get_sort_key_buf())
    }
}

impl<'a, B> Ord for HeapEntry<'a, B>
where
    B: BufferManager,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}
