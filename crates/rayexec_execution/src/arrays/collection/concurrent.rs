use std::sync::atomic::{self, AtomicUsize};
use std::sync::Arc;

use parking_lot::Mutex;
use rayexec_error::Result;

use super::segment::ColumnCollectionSegment;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::buffer::buffer_manager::NopBufferManager;
use crate::storage::projections::Projections;

#[derive(Debug)]
pub struct ColumnCollectionAppendState {
    segment: ColumnCollectionSegment,
}

#[derive(Debug)]
pub struct ColumnCollectionScanState {
    /// Index of the segment we should check next.
    next_segment_idx: usize,
    /// Current segment we're on.
    segment: Option<Arc<ColumnCollectionSegment>>,
    /// Current chunk within the segment we're on.
    chunk_idx: usize,
}

/// State for parallel scans on the collection.
///
/// All parallel states initialized at the same time will coordinate which
/// segments to scan such that every row is scanned exactly once.
#[derive(Debug)]
pub struct ParallelColumnCollectionScanState {
    /// Shared atomic indicating the next segment to scan.
    next: Arc<AtomicUsize>,
    /// Local scan state.
    state: ColumnCollectionScanState,
}

/// Data collection that can be append to/ read from concurrently.
#[derive(Debug)]
pub struct ConcurrentColumnCollection {
    /// Data types of columns in this collection.
    datatypes: Vec<DataType>,
    /// All segments that have been flushed to the collection.
    segments: Mutex<Vec<Arc<ColumnCollectionSegment>>>,
    /// Segment size in chunks. Inexact.
    segment_size: usize,
    /// Max capacity of each chunk in rows.
    chunk_capacity: usize,
}

impl ConcurrentColumnCollection {
    pub fn new(
        datatypes: impl IntoIterator<Item = DataType>,
        segment_size: usize,
        chunk_capacity: usize,
    ) -> Self {
        ConcurrentColumnCollection {
            datatypes: datatypes.into_iter().collect(),
            segments: Mutex::new(Vec::new()),
            segment_size,
            chunk_capacity,
        }
    }

    pub fn init_append_state(&self) -> ColumnCollectionAppendState {
        ColumnCollectionAppendState {
            segment: ColumnCollectionSegment::new(self.chunk_capacity),
        }
    }

    /// Initializes parallel scan states that will coordinate with each other
    /// for scanning disjoint batches.
    pub fn init_parallel_scan_states(
        &self,
        num_parallel: usize,
    ) -> impl Iterator<Item = ParallelColumnCollectionScanState> + '_ {
        CreateParallelStateIter::new(num_parallel)
    }

    /// Initializes a scan state for reading all batches in the collection.
    pub fn init_scan_state(&self) -> ColumnCollectionScanState {
        ColumnCollectionScanState {
            next_segment_idx: 0,
            segment: None,
            chunk_idx: 0,
        }
    }

    /// Gets the total number of rows in the collection.
    ///
    /// This will lock the segments while iterating.
    pub fn total_rows(&self) -> usize {
        self.segments.lock().iter().map(|s| s.num_rows()).sum()
    }

    pub fn datatypes(&self) -> &[DataType] {
        &self.datatypes
    }

    /// Appends a batch to the collection.
    ///
    /// This will write the batch to the state's segment first, then flush to
    /// the collection if the segment reaches a certain size.
    pub fn append_batch(
        &self,
        state: &mut ColumnCollectionAppendState,
        batch: &Batch,
    ) -> Result<()> {
        state
            .segment
            .append_batch(&NopBufferManager, batch, &self.datatypes)?;

        if state.segment.num_chunks() >= self.segment_size {
            self.flush(state)?;
        }

        Ok(())
    }

    /// Flushes any pending chunks in from the append state to the collection.
    ///
    /// The state may continue to be used.
    pub fn flush(&self, state: &mut ColumnCollectionAppendState) -> Result<()> {
        let mut segment = std::mem::replace(
            &mut state.segment,
            ColumnCollectionSegment::new(self.chunk_capacity),
        );
        segment.finish_append();
        // Ensure we don't add segments with zero total rows since we depend on
        // zero being a marker value for when we're done scanning.
        if segment.num_rows() == 0 {
            return Ok(());
        }

        let mut segments = self.segments.lock();
        segments.push(Arc::new(segment));

        Ok(())
    }

    /// Scans the next batch from the collection.
    ///
    /// Returns the number of rows scanned into output. Zero may be returned if
    /// there's no additional batches to scan.
    ///
    /// Note that this may be called interchangeably with `append_batch`.
    pub fn scan(
        &self,
        projections: &Projections,
        state: &mut ColumnCollectionScanState,
        output: &mut Batch,
    ) -> Result<usize> {
        self.scan_inner(projections, state, output, |curr| curr + 1)
    }

    /// Scans the next batch using a state that coordinates with sibling scan
    /// states to ensure batches are read only once.
    ///
    /// Can be called interchangeably with `append_batch`.
    pub fn parallel_scan(
        &self,
        projections: &Projections,
        state: &mut ParallelColumnCollectionScanState,
        output: &mut Batch,
    ) -> Result<usize> {
        self.scan_inner(projections, &mut state.state, output, |_curr| {
            state.next.fetch_add(1, atomic::Ordering::Relaxed)
        })
    }

    /// Scan implemenation with the next segment id to scan determined by the
    /// provided function.
    fn scan_inner(
        &self,
        projections: &Projections,
        state: &mut ColumnCollectionScanState,
        output: &mut Batch,
        next_segment_fn: impl Fn(usize) -> usize,
    ) -> Result<usize> {
        loop {
            if state.segment.is_none() {
                let segments = self.segments.lock();
                let segment = match segments.get(state.next_segment_idx) {
                    Some(segment) => segment,
                    None => {
                        // No more segments.
                        output.set_num_rows(0)?;
                        return Ok(0);
                    }
                };

                state.segment = Some(segment.clone());
                state.next_segment_idx = next_segment_fn(state.next_segment_idx);
                state.chunk_idx = 0;
            }

            let segment = state.segment.as_ref().unwrap();

            match segment.get_chunk(state.chunk_idx) {
                Some(chunk) => {
                    let num_rows = chunk.scan(projections, output)?;
                    state.chunk_idx += 1;
                    return Ok(num_rows);
                }
                None => {
                    // No more chunks in segment, need to move to next one.
                    state.segment = None;
                }
            }
        }
    }
}

/// Helper for creating parallel scan states.
#[derive(Debug)]
struct CreateParallelStateIter {
    next: Arc<AtomicUsize>,
    idx: usize,
    count: usize,
}

impl CreateParallelStateIter {
    fn new(num_parallel: usize) -> Self {
        CreateParallelStateIter {
            next: Arc::new(AtomicUsize::new(num_parallel)),
            idx: 0,
            count: num_parallel,
        }
    }
}

impl Iterator for CreateParallelStateIter {
    type Item = ParallelColumnCollectionScanState;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.count {
            return None;
        }

        let state = ParallelColumnCollectionScanState {
            next: self.next.clone(),
            state: ColumnCollectionScanState {
                next_segment_idx: self.idx,
                segment: None,
                chunk_idx: 0,
            },
        };

        self.idx += 1;

        Some(state)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.count - self.idx;
        (rem, Some(rem))
    }
}

impl ExactSizeIterator for CreateParallelStateIter {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate_batch;
    use crate::testutil::arrays::assert_batches_eq;

    #[test]
    fn append_scan_simple() {
        let collection = ConcurrentColumnCollection::new([DataType::Int32, DataType::Utf8], 16, 16);
        let projections = Projections::new([0, 1]);

        let mut append_state = collection.init_append_state();
        let mut scan_state = collection.init_scan_state();

        let input = generate_batch!([4, 5, 6, 7], ["a", "b", "c", "d"]);
        collection.append_batch(&mut append_state, &input).unwrap();
        collection.flush(&mut append_state).unwrap();

        let mut output = Batch::new([DataType::Int32, DataType::Utf8], 16).unwrap();
        collection
            .scan(&projections, &mut scan_state, &mut output)
            .unwrap();
        assert_batches_eq(&input, &output);

        // Try scan again, get nothing.
        collection
            .scan(&projections, &mut scan_state, &mut output)
            .unwrap();
        assert_eq!(0, output.num_rows());

        // Should be able to keep appending.
        let input = generate_batch!([1, 2, 3, 4], ["e", "f", "g", "h"]);
        collection.append_batch(&mut append_state, &input).unwrap();
        collection.flush(&mut append_state).unwrap();

        collection
            .scan(&projections, &mut scan_state, &mut output)
            .unwrap();
        assert_batches_eq(&input, &output);
    }

    #[test]
    fn scan_projected_column() {
        let collection = ConcurrentColumnCollection::new([DataType::Int32, DataType::Utf8], 16, 16);
        let projections = Projections::new([1]);

        let mut append_state = collection.init_append_state();
        let mut scan_state = collection.init_scan_state();

        let input = generate_batch!([4, 5, 6, 7], ["a", "b", "c", "d"]);
        collection.append_batch(&mut append_state, &input).unwrap();
        collection.flush(&mut append_state).unwrap();

        let mut output = Batch::new([DataType::Utf8], 16).unwrap();
        collection
            .scan(&projections, &mut scan_state, &mut output)
            .unwrap();

        let expected = generate_batch!(["a", "b", "c", "d"]);
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn scan_parallel() {
        // Very small segments, chunks.
        let collection = ConcurrentColumnCollection::new([DataType::Int32, DataType::Utf8], 1, 2);

        let mut append_state = collection.init_append_state();
        // TODO: Currently we allow segments to be larger than the configured
        // size if a single input batch exceeds the size of a chunk. We should
        // probably split up chunks when we flush to make sure they're all the
        // correct size.
        //
        // To properly get two segments right now, we need to append two batches.
        let input1 = generate_batch!([4, 5], ["a", "b"]);
        collection.append_batch(&mut append_state, &input1).unwrap();
        collection.flush(&mut append_state).unwrap();
        let input2 = generate_batch!([6, 7], ["c", "d"]);
        collection.append_batch(&mut append_state, &input2).unwrap();
        collection.flush(&mut append_state).unwrap();

        let projections = Projections::new([0, 1]);

        // We should have two segments now.
        let mut states: Vec<_> = collection.init_parallel_scan_states(2).collect();
        assert_eq!(2, states.len());

        let mut output1 = Batch::new([DataType::Int32, DataType::Utf8], 2).unwrap();
        collection
            .parallel_scan(&projections, &mut states[0], &mut output1)
            .unwrap();

        let expected1 = generate_batch!([4, 5], ["a", "b"]);
        assert_batches_eq(&expected1, &output1);

        let mut output2 = Batch::new([DataType::Int32, DataType::Utf8], 2).unwrap();
        collection
            .parallel_scan(&projections, &mut states[1], &mut output2)
            .unwrap();

        let expected2 = generate_batch!([6, 7], ["c", "d"]);
        assert_batches_eq(&expected2, &output2);

        // Should exhaust both scans.
        collection
            .parallel_scan(&projections, &mut states[0], &mut output1)
            .unwrap();
        assert_eq!(0, output1.num_rows());

        collection
            .parallel_scan(&projections, &mut states[1], &mut output2)
            .unwrap();
        assert_eq!(0, output2.num_rows());
    }

    #[test]
    fn scan_parallel_exhaust_refill() {
        // Very small segments, chunks.
        let collection = ConcurrentColumnCollection::new([DataType::Int32, DataType::Utf8], 1, 2);

        let projections = Projections::new([0, 1]);

        let mut append_state = collection.init_append_state();
        let mut scan_states: Vec<_> = collection.init_parallel_scan_states(2).collect();

        // Append first batch.
        let input1 = generate_batch!([4, 5], ["a", "b"]);
        collection.append_batch(&mut append_state, &input1).unwrap();
        collection.flush(&mut append_state).unwrap();

        // Scan first batch.
        //
        // Assumes knowledge of internals -- states[0] will read the first
        // segment, states[1] will read the next.
        let mut out1 = Batch::new([DataType::Int32, DataType::Utf8], 2).unwrap();
        collection
            .parallel_scan(&projections, &mut scan_states[0], &mut out1)
            .unwrap();

        let expected1 = generate_batch!([4, 5], ["a", "b"]);
        assert_batches_eq(&expected1, &out1);

        // Second scan is exhausted immediately.
        let mut out2 = Batch::new([DataType::Int32, DataType::Utf8], 2).unwrap();
        collection
            .parallel_scan(&projections, &mut scan_states[1], &mut out2)
            .unwrap();
        assert_eq!(0, out2.num_rows());

        // Push more input.
        let input2 = generate_batch!([6, 7], ["c", "d"]);
        collection.append_batch(&mut append_state, &input2).unwrap();
        collection.flush(&mut append_state).unwrap();

        // Now we should be able to scan.
        collection
            .parallel_scan(&projections, &mut scan_states[1], &mut out2)
            .unwrap();

        let expected2 = generate_batch!([6, 7], ["c", "d"]);
        assert_batches_eq(&expected2, &out2);
    }
}
