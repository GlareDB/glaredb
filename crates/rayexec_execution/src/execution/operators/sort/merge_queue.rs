use std::collections::VecDeque;
use std::task::{Context, Waker};

use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};

use crate::arrays::array::buffer_manager::NopBufferManager;
use crate::arrays::row::row_layout::RowLayout;
use crate::arrays::sort::binary_merge::BinaryMerger;
use crate::arrays::sort::partial_sort::PartialSortedRowCollection;
use crate::arrays::sort::sort_layout::SortLayout;
use crate::arrays::sort::sorted_run::SortedRun;

/// Result of a single merge pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollMerge {
    /// All merges complete, we have a single, totally sorted run.
    Finished,
    /// We successfully merged two runs in the queue.
    Merged,
    /// We don't have enough runs to merge. Come back later.
    Pending,
}

/// Merge queue for iteratively merging sorted runs.
///
/// When a partition completes its local collection, the sorted blocks will be
/// added to this queue. Each block initializes a single sorted run. That thread
/// will then begin the merging process.
///
/// Each round of merging will take two sorted runs from the queue, merge them
/// into a single sorted run, with that run being added to the end of the queue.
///
/// Merging will continually happen until we have a single sorted run and all
/// threads have added their local collections to the queue.
#[derive(Debug)]
pub struct MergeQueue {
    pub key_layout: SortLayout,
    pub data_layout: RowLayout,
    block_capacity: usize,
    inner: Mutex<MergeQueueInner>,
}

#[derive(Debug)]
struct MergeQueueInner {
    /// All runs we've collected and merged so far.
    runs: VecDeque<SortedRun<NopBufferManager>>,
    /// Remaining number of collections we're waiting on.
    ///
    /// If zero, all partitions completed collecting their data and have added
    /// it to the queue.
    ///
    /// Initialized to number of partitions.
    remaining_collection_count: usize,
    /// Number of merges we're currently running.
    running_merges: usize,
    /// Wakers for partitions waiting to work on a merge.
    wakers: Vec<Option<Waker>>,
}

impl MergeQueueInner {
    /// If we've completed all merges in the queue.
    ///
    /// Once this is true, we can begin to produce batches.
    fn is_complete(&self) -> bool {
        self.remaining_collection_count == 0
            && self.running_merges == 0
            && (self.runs.len() == 1 || self.runs.is_empty())
    }
}

impl MergeQueue {
    pub fn new(
        key_layout: SortLayout,
        data_layout: RowLayout,
        block_capacity: usize,
        num_partitions: usize,
    ) -> Self {
        let inner = MergeQueueInner {
            runs: VecDeque::new(),
            remaining_collection_count: num_partitions,
            running_merges: 0,
            wakers: (0..num_partitions).map(|_| None).collect(),
        };

        MergeQueue {
            key_layout,
            data_layout,
            block_capacity,
            inner: Mutex::new(inner),
        }
    }

    /// Adds the sorted blocks from a collection to this queue.
    pub fn add_collection(&self, collection: PartialSortedRowCollection) -> Result<()> {
        let sorted_blocks = collection.try_into_sorted_blocks()?;

        let mut inner = self.inner.lock();
        inner
            .runs
            .extend(sorted_blocks.into_iter().map(SortedRun::from_sorted_block));

        Ok(())
    }

    /// Try to merge the next two sorted runs in the queue.
    ///
    /// Returns a `PollMerge` indicating if we merged or if we're waiting on
    /// additional runs. If Pending is returned, then a waker is stored. The
    /// waker will be woken when we should attempt to merge again.
    pub fn poll_merge_next(&self, cx: &mut Context, partition_idx: usize) -> Result<PollMerge> {
        let mut inner = self.inner.lock();
        if inner.is_complete() {
            return Ok(PollMerge::Finished);
        }

        if inner.runs.len() < 2 {
            // Not enough runs.
            inner.wakers[partition_idx] = Some(cx.waker().clone());
            return Ok(PollMerge::Pending);
        }

        // We have merges, go ahead and take them.
        let left = inner.runs.pop_front().unwrap();
        let right = inner.runs.pop_front().unwrap();
        inner.running_merges += 1;

        // Do merge outside of lock.
        std::mem::drop(inner);

        let merger = BinaryMerger::new(
            &NopBufferManager,
            &self.key_layout,
            &self.data_layout,
            self.block_capacity,
        );

        // TODO: Should this be stored somewhere?
        let mut state = merger.init_merge_state();
        let out = merger.merge(&mut state, left, right)?;

        // Push merged run back into the queue.
        let mut inner = self.inner.lock();
        inner.runs.push_back(out);
        inner.running_merges -= 1;

        // Wake up pending wakers.
        //
        // Note we do this even if there's only a single run in the queue to
        // signal that all merging is complete.
        //
        // TODO: Possibly only wake a subset depending on number of runs in the
        // queue?
        for waker in &mut inner.wakers {
            if let Some(waker) = waker.take() {
                waker.wake();
            }
        }

        Ok(PollMerge::Merged)
    }

    /// Takes the final sorted run from the queue.
    ///
    /// Errors if the queue isn't finished.
    ///
    /// If there are no sorted runs (e.g. sorting no rows), then None will be
    /// returned.
    pub fn take_sorted_run(&self) -> Result<Option<SortedRun<NopBufferManager>>> {
        let mut inner = self.inner.lock();
        if !inner.is_complete() {
            return Err(RayexecError::new(
                "Attempted to take final run from queue before merging complete",
            ));
        }

        let run = inner.runs.pop_front();

        Ok(run)
    }
}
