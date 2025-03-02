use std::sync::Arc;

use parking_lot::Mutex;
use rayexec_error::Result;

use super::segment::ColumnCollectionSegment;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::buffer::buffer_manager::NopBufferManager;

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
            segment: ColumnCollectionSegment::new(),
        }
    }

    pub fn init_scan_state(&self) -> ColumnCollectionScanState {
        ColumnCollectionScanState {
            next_segment_idx: 0,
            segment: None,
            chunk_idx: 0,
        }
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
        state.segment.append_batch(
            &NopBufferManager,
            batch,
            &self.datatypes,
            self.chunk_capacity,
        )?;

        if state.segment.num_chunks() >= self.segment_size {
            self.flush(state)?;
        }

        Ok(())
    }

    pub fn flush(&self, state: &mut ColumnCollectionAppendState) -> Result<()> {
        let mut segment = std::mem::replace(&mut state.segment, ColumnCollectionSegment::new());
        segment.finish_append();
        if segment.num_chunks() == 0 {
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
    pub fn scan(&self, state: &mut ColumnCollectionScanState, output: &mut Batch) -> Result<usize> {
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
                state.next_segment_idx += 1;
                state.chunk_idx = 0;
            }

            let segment = state.segment.as_ref().unwrap();

            match segment.get_chunk(state.chunk_idx) {
                Some(chunk) => {
                    let num_rows = chunk.scan(output)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate_batch;
    use crate::testutil::arrays::assert_batches_eq;

    #[test]
    fn append_scan_simple() {
        let collection = ConcurrentColumnCollection::new([DataType::Int32, DataType::Utf8], 16, 16);

        let mut append_state = collection.init_append_state();
        let mut scan_state = collection.init_scan_state();

        let input = generate_batch!([4, 5, 6, 7], ["a", "b", "c", "d"]);
        collection.append_batch(&mut append_state, &input).unwrap();
        collection.flush(&mut append_state).unwrap();

        let mut output = Batch::new([DataType::Int32, DataType::Utf8], 16).unwrap();
        collection.scan(&mut scan_state, &mut output).unwrap();
        assert_batches_eq(&input, &output);

        // Try scan again, get nothing.
        collection.scan(&mut scan_state, &mut output).unwrap();
        assert_eq!(0, output.num_rows());

        // Should be able to keep appending.
        let input = generate_batch!([1, 2, 3, 4], ["e", "f", "g", "h"]);
        collection.append_batch(&mut append_state, &input).unwrap();
        collection.flush(&mut append_state).unwrap();

        collection.scan(&mut scan_state, &mut output).unwrap();
        assert_batches_eq(&input, &output);
    }
}
