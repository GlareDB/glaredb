use rayexec_error::Result;

use super::row_heap::RowHeap;
use super::row_layout::RowLayout;
use crate::arrays::array::buffer_manager::{BufferManager, NopBufferManager};
use crate::arrays::array::raw::TypedRawBuffer;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;

/// Collects array data by first row-encoding the data and storing it in raw
/// buffers.
#[derive(Debug)]
pub struct RowCollection {
    /// Layout for the rows.
    layout: RowLayout,
    /// All chunks in this collection.
    chunks: Vec<RowChunk<NopBufferManager>>,
    /// Capacity in rows to use for each chunk.
    chunk_capacity: usize,
}

impl RowCollection {
    pub fn new(layout: RowLayout, chunk_capacity: usize) -> Self {
        RowCollection {
            layout,
            chunks: Vec::new(),
            chunk_capacity,
        }
    }

    pub fn row_count(&self) -> usize {
        self.chunks.iter().map(|c| c.filled).sum()
    }

    /// Appends a batch to the collection.
    pub fn append_batch(&mut self, batch: &Batch) -> Result<()> {
        // Init first chunk if needed.
        if self.chunks.is_empty() {
            let chunk = RowChunk::try_new(&NopBufferManager, &self.layout, self.chunk_capacity)?;
            self.chunks.push(chunk);
        }

        let mut input_offset = 0; // Offset to begin copying from in the input.
        let mut rows_remaining = batch.num_rows();

        while rows_remaining != 0 {
            let chunk = self.chunks.last_mut().expect("at least one chunk");
            let copy_count = usize::min(chunk.capacity - chunk.filled, rows_remaining);

            chunk.copy_rows(&self.layout, &batch.arrays, input_offset, copy_count)?;
            input_offset += copy_count;
            rows_remaining -= copy_count;

            if rows_remaining > 0 {
                // Create new chunk for writing to if we have more rows to
                // write.
                let chunk =
                    RowChunk::try_new(&NopBufferManager, &self.layout, self.chunk_capacity)?;
                self.chunks.push(chunk);
            }
        }

        Ok(())
    }

    pub fn init_scan(&self) -> RowCollectionScanState {
        RowCollectionScanState {
            chunk_idx: 0,
            row_idx: 0,
        }
    }

    /// Scan the collection, writing rows to `output`.
    ///
    /// The number of rows scanned is returned. Zero is returned if either the
    /// output batches capacity is zero, or if there are no more rows to scan
    /// according the scan state.
    ///
    /// This updates the scan state to allow for resuming scans.
    pub fn scan(&self, state: &mut RowCollectionScanState, output: &mut Batch) -> Result<usize> {
        let mut row_count = 0;
        let mut remaining_cap = output.capacity;

        while remaining_cap > 0 {
            let chunk = match self.chunks.get(state.chunk_idx) {
                Some(chunk) => chunk,
                None => break, // No more chunks, break out of loop to ensure batch gets updated.
            };

            if state.row_idx >= chunk.filled {
                // No more rows in this chunk, try to move to the next chunk.
                state.chunk_idx += 1;
                state.row_idx = 0;
                continue;
            }

            // We have a chunk with rows to scan.
            //
            // Compute how many rows we can scan from the chunk.
            let scan_row_count = usize::min(remaining_cap, chunk.filled - state.row_idx);
            chunk.scan(
                &self.layout,
                state.row_idx,
                scan_row_count,
                &mut output.arrays,
            )?;

            row_count += scan_row_count;
            state.row_idx += scan_row_count;
            remaining_cap -= scan_row_count;
        }

        // Ensure we set the row count in the output batch.
        output.set_num_rows(row_count)?;

        // Scan state has been update, nothing else to do here...

        Ok(row_count)
    }
}

#[derive(Debug)]
struct RowChunk<B: BufferManager> {
    /// The raw buffer holding encoded row values.
    data: TypedRawBuffer<u8, B>,
    /// Number of rows this chunk can hold.
    capacity: usize,
    /// Number of rows we've written to this chunk.
    filled: usize,
    /// Heap for varlen and nested data.
    heap: RowHeap<B>,
}

impl<B> RowChunk<B>
where
    B: BufferManager,
{
    fn try_new(manager: &B, layout: &RowLayout, capacity: usize) -> Result<Self> {
        let buffer_size = layout.buffer_size(capacity);
        let data = TypedRawBuffer::try_with_capacity(manager, buffer_size)?;
        let heap = RowHeap::with_capacity(manager, 0)?;

        Ok(RowChunk {
            data,
            capacity,
            filled: 0,
            heap,
        })
    }

    /// Copy rows to the chunk by encoding them into a row format.
    fn copy_rows(
        &mut self,
        layout: &RowLayout,
        src: &[Array<B>],
        src_offset: usize,
        count: usize,
    ) -> Result<()> {
        // Compute the exact size buffer to use for the encode.
        let offset = layout.buffer_size(self.filled);
        let size = layout.buffer_size(count);
        let buf = &mut self.data.as_slice_mut()[offset..(offset + size)];

        let selection = src_offset..(src_offset + count);
        layout.encode_arrays(src, selection, buf)?;

        self.filled += count;

        Ok(())
    }

    fn scan(
        &self,
        layout: &RowLayout,
        src_row_offset: usize,
        count: usize,
        dest: &mut [Array<B>],
    ) -> Result<()> {
        let buf = self.data.as_slice();
        let selection = src_row_offset..(src_row_offset + count);
        layout.decode_arrays(buf, selection, dest)?;

        Ok(())
    }
}

/// State for resumable scanning of the row collection.
#[derive(Debug)]
pub struct RowCollectionScanState {
    /// Index of the chunk we should resume scanning from.
    chunk_idx: usize,
    /// Row within the chunk we should resume scanning from.
    row_idx: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::datatype::DataType;
    use crate::arrays::testutil::{assert_batches_eq, generate_batch};

    #[test]
    fn append_single_batch_i32() {
        let mut collection = RowCollection::new(RowLayout::new([DataType::Int32]), 16);
        let input = generate_batch!([1, 2, 3, 4, 5, 6]);
        collection.append_batch(&input).unwrap();

        let mut output = Batch::try_new([DataType::Int32], 16).unwrap();

        let mut state = collection.init_scan();
        collection.scan(&mut state, &mut output).unwrap();

        let expected = generate_batch!([1, 2, 3, 4, 5, 6]);
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn append_single_batch_i32_with_invalid() {
        let mut collection = RowCollection::new(RowLayout::new([DataType::Int32]), 16);
        let input = generate_batch!([Some(1), Some(2), None, Some(4), None, Some(6)]);
        collection.append_batch(&input).unwrap();
        assert_eq!(6, collection.row_count());

        let mut output = Batch::try_new([DataType::Int32], 16).unwrap();

        let mut state = collection.init_scan();
        let scan_count = collection.scan(&mut state, &mut output).unwrap();
        assert_eq!(6, scan_count);

        let expected = generate_batch!([Some(1), Some(2), None, Some(4), None, Some(6)]);
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn append_multiple_batches_i32_with_invalid() {
        let mut collection = RowCollection::new(RowLayout::new([DataType::Int32]), 16);
        let input1 = generate_batch!(0..16);
        collection.append_batch(&input1).unwrap();
        let input2 = generate_batch!(16..32);
        collection.append_batch(&input2).unwrap();
        assert_eq!(32, collection.row_count());

        let mut output = Batch::try_new([DataType::Int32], 16).unwrap();
        let mut state = collection.init_scan();

        let scan_count = collection.scan(&mut state, &mut output).unwrap();
        assert_eq!(16, scan_count);

        let expected1 = generate_batch!(0..16);
        assert_batches_eq(&expected1, &output);

        let scan_count = collection.scan(&mut state, &mut output).unwrap();
        assert_eq!(16, scan_count);

        let expected2 = generate_batch!(16..32);
        assert_batches_eq(&expected2, &output);
    }
}
