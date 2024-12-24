use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, VecDeque};

use rayexec_error::Result;

use super::sort_data::SortBlock;
use crate::arrays::batch::Batch;
use crate::arrays::buffer_manager::BufferManager;

/// Trait for allows writing the result of a merge to either a Batch or a
/// SortBlock.
///
/// Merging happens twice during a physical sort. The first is when sorting the
/// data local to a partition. The results get written to a new SortBlock.
///
/// The second is when we output the globally sorted data. We can just write
/// that to the output batch since we don't need to keep the encoded data
/// around.
///
/// This trait just facilitates using either a Batch or SortData with the
/// merger.
pub trait MergeOutput<B: BufferManager> {
    /// Return the row capacity of self.
    fn capacity(&self) -> usize;

    /// Copy the row represented by the heap entry into self at the `dest_idx`.
    fn copy_row_from_entry(&mut self, dest_idx: usize, ent: &HeapEntry<B>) -> Result<()>;
}

impl<B> MergeOutput<B> for Batch<B>
where
    B: BufferManager,
{
    fn capacity(&self) -> usize {
        self.capacity()
    }

    fn copy_row_from_entry(&mut self, dest_idx: usize, ent: &HeapEntry<B>) -> Result<()> {
        for (source, dest) in ent.queue.current.block.block.arrays().iter().zip(self.arrays_mut()) {
            let source_idx = ent.queue.current.curr_idx;
            source.copy_row(source_idx, dest, dest_idx)?;
        }

        Ok(())
    }
}

impl<B> MergeOutput<B> for SortBlock<B>
where
    B: BufferManager,
{
    fn capacity(&self) -> usize {
        self.block.capacity()
    }

    fn copy_row_from_entry(&mut self, dest_idx: usize, ent: &HeapEntry<B>) -> Result<()> {
        let source_block = &ent.queue.current.block;
        let source_idx = ent.queue.current.curr_idx;

        self.copy_row_from_other(dest_idx, source_block, source_idx)
    }
}

/// A block containing sorted rows that's being merged with other blocks.
#[derive(Debug)]
pub struct MergingSortBlock<B: BufferManager> {
    /// The current index in the block that we're comparing.
    curr_idx: usize,
    /// The block we're merging.
    block: SortBlock<B>,
}

#[derive(Debug)]
pub struct MergeQueue<B: BufferManager> {
    exhausted: bool,
    current: MergingSortBlock<B>,
    remaining: VecDeque<SortBlock<B>>,
}

impl<B> MergeQueue<B>
where
    B: BufferManager,
{
    /// Create a new queue with a single block.
    ///
    /// May return None if the sort block contains no rows.
    pub fn new_single(sort_block: SortBlock<B>) -> Option<Self> {
        if sort_block.row_count() == 0 {
            return None;
        }

        Some(MergeQueue {
            exhausted: false,
            current: MergingSortBlock {
                curr_idx: 0,
                block: sort_block,
            },
            remaining: VecDeque::new(),
        })
    }

    /// Create a new queue of blocks.
    ///
    /// Blocks should be totally ordered to from least to greatest.
    ///
    /// May return None if there's no blocks with any data.
    pub fn new(blocks: impl IntoIterator<Item = SortBlock<B>>) -> Option<Self> {
        let mut blocks: VecDeque<_> = blocks.into_iter().collect();

        loop {
            match blocks.pop_front() {
                Some(first) => {
                    if first.block.row_count() > 0 {
                        return Some(MergeQueue {
                            exhausted: false,
                            current: MergingSortBlock {
                                curr_idx: 0,
                                block: first,
                            },
                            remaining: blocks,
                        });
                    }
                }
                None => return None,
            }
        }
    }

    fn prepare_next_row(&mut self) {
        if self.exhausted {
            return;
        }

        self.current.curr_idx += 1;
        if self.current.curr_idx >= self.current.block.row_count() {
            // Get next block in queue.
            loop {
                match self.remaining.pop_front() {
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
    queues: Vec<MergeQueue<B>>,
}

impl<B> Merger<B>
where
    B: BufferManager,
{
    /// Create a new merger using the given queues.
    pub fn new(queues: Vec<MergeQueue<B>>) -> Self {
        Merger { queues }
    }

    /// Do a single round of merging, writing the output to `out`.
    ///
    /// The number of rows written will be written.
    pub fn merge_round(&mut self, out: &mut impl MergeOutput<B>) -> Result<usize> {
        // TODO: Optimization, if only a single queue remains, just drain
        // instead of building min heap.

        let out_capacity = out.capacity();

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

        for row_idx in 0..out_capacity {
            let ent = match min_heap.pop() {
                Some(ent) => ent,
                None => {
                    // If heap is empty, we exhausted all queues.
                    return Ok(row_idx);
                }
            };

            // Copy the row to out.
            out.copy_row_from_entry(row_idx, &ent.0)?;

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

        // We wrote the entire capacity of the block.
        Ok(out_capacity)
    }
}

/// Entry with the heap representing a block's row.
///
/// Eq and Ord comparisons delegate the key buffer this entry represents.
#[derive(Debug)]
pub struct HeapEntry<'a, B: BufferManager> {
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
