use std::borrow::{Borrow, BorrowMut};
use std::collections::VecDeque;

use rayexec_error::Result;

use super::row_heap::RowHeap;
use super::row_layout::RowLayout;
use crate::arrays::array::buffer_manager::{BufferManager, NopBufferManager};
use crate::arrays::array::raw::TypedRawBuffer;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;

/// Address to a single row in the collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowAddress {
    pub chunk_idx: u32,
    pub row_idx: u16,
}

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

    pub fn layout(&self) -> &RowLayout {
        &self.layout
    }

    pub fn row_count(&self) -> usize {
        self.chunks.iter().map(|c| c.filled).sum()
    }

    /// Appends a batch to the collection.
    ///
    /// The array types for this batch should match the types specified in the
    /// row layout.
    pub fn append_batch(&mut self, batch: &Batch) -> Result<()> {
        self.append_arrays(&batch.arrays, batch.num_rows)
    }

    /// Internal method for appending arrays to this collection.
    ///
    /// Array capacities must equal or exceed `num_rows`.
    pub(crate) fn append_arrays<A>(&mut self, arrays: &[A], num_rows: usize) -> Result<()>
    where
        A: Borrow<Array>,
    {
        // Init first chunk if needed.
        if self.chunks.is_empty() {
            let chunk = RowChunk::try_new(&NopBufferManager, &self.layout, self.chunk_capacity)?;
            self.chunks.push(chunk);
        }

        let mut input_offset = 0; // Offset to begin copying from in the input.
        let mut rows_remaining = num_rows;

        while rows_remaining != 0 {
            let chunk = self.chunks.last_mut().expect("at least one chunk");
            let copy_count = usize::min(chunk.capacity - chunk.filled, rows_remaining);

            chunk.copy_rows(&self.layout, arrays, input_offset, copy_count)?;
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

    /// Initialize a scan state for scanning all chunks in the collection.
    pub fn init_full_scan(&self) -> RowCollectionScanState {
        RowCollectionScanState {
            chunks_to_scan: (0..self.chunks.len()).collect(),
            row_addresses: Vec::new(),
        }
    }

    /// Initialize a scan of only some of the chunks.
    pub fn init_partial_scan(
        &self,
        chunk_indices: impl IntoIterator<Item = usize>,
    ) -> RowCollectionScanState {
        RowCollectionScanState {
            chunks_to_scan: chunk_indices.into_iter().collect(),
            row_addresses: Vec::new(),
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
        let count = self.scan_columns(
            state,
            &(0..output.arrays.len()).collect::<Vec<_>>(), // TODO: Don't do this.
            &mut output.arrays,
            output.capacity,
        )?;
        // Ensure we set the row count in the output batch.
        output.set_num_rows(count)?;

        Ok(count)
    }

    /// Scan a subset of columns into the output arrays.
    ///
    /// `column` provide the columns to scan into the `output` arrays.
    ///
    /// `count` indicates the max number of rows to write to the array. This
    /// must be less than or equal to the array capacities.
    pub fn scan_columns<A>(
        &self,
        state: &mut RowCollectionScanState,
        columns: &[usize],
        output: &mut [A],
        count: usize,
    ) -> Result<usize>
    where
        A: BorrowMut<Array>,
    {
        assert_eq!(columns.len(), output.len());

        // Get chunk/row to start scanning at from the most recent scan if we
        // have it. Otherwise init from the chunks to scan.
        let (mut chunk_idx, mut row_idx) = match state.row_addresses.last() {
            Some(addr) => (addr.chunk_idx as usize, addr.row_idx as usize + 1),
            None => {
                match state.chunks_to_scan.pop_front() {
                    Some(chunk_idx) => (chunk_idx, 0),
                    None => return Ok(0), // State has no addresses or chunks.
                }
            }
        };

        state.row_addresses.clear();

        let mut remaining_cap = count;

        while remaining_cap > 0 {
            let chunk = &self.chunks[chunk_idx];
            if row_idx >= chunk.filled {
                // No more rows in this chunk, try to move to the next chunk.
                match state.chunks_to_scan.pop_front() {
                    Some(next_idx) => {
                        chunk_idx = next_idx;
                        row_idx = 0;
                        continue;
                    }
                    None => break, // No more chunks to scan.
                }
            }

            // We have a chunk with rows to scan.
            //
            // Compute how many rows we can scan from the chunk.
            let scan_row_count = usize::min(remaining_cap, chunk.filled - row_idx);
            chunk.scan(
                &self.layout,
                row_idx,
                scan_row_count,
                columns
                    .iter()
                    .copied()
                    .zip(output.iter_mut().map(|a| a.borrow_mut())),
            )?;

            // Update scan state with row addresses for rows we just scanned.
            state
                .row_addresses
                .extend(
                    (row_idx..(row_idx + scan_row_count)).map(|row_idx| RowAddress {
                        chunk_idx: chunk_idx as u32,
                        row_idx: row_idx as u16,
                    }),
                );

            row_idx += scan_row_count;
            remaining_cap -= scan_row_count;
        }

        Ok(state.row_addresses.len())
    }

    /// Returns a pointer to the start of a row in the collection.
    ///
    /// # Safety
    ///
    /// The row address provided must point to a valid row to ensure the pointer
    /// remains in bounds.
    pub(crate) unsafe fn row_ptr(&self, addr: RowAddress) -> *const u8 {
        let chunk = &self.chunks[addr.chunk_idx as usize];
        debug_assert!(addr.row_idx as usize <= chunk.filled);

        let offset = self.layout.row_width * (addr.row_idx as usize);
        chunk.data.raw.as_ptr().byte_add(offset)
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
    fn copy_rows<A>(
        &mut self,
        layout: &RowLayout,
        src: &[A],
        src_offset: usize,
        count: usize,
    ) -> Result<()>
    where
        A: Borrow<Array<B>>,
    {
        // Compute the exact size buffer to use for the encode.
        let offset = layout.buffer_size(self.filled);
        let size = layout.buffer_size(count);
        let buf = &mut self.data.as_slice_mut()[offset..(offset + size)];

        let selection = src_offset..(src_offset + count);
        layout.encode_arrays(src, selection, buf, &mut self.heap)?;

        self.filled += count;

        Ok(())
    }

    fn scan<'a>(
        &self,
        layout: &RowLayout,
        src_row_offset: usize,
        count: usize,
        output_arrays: impl IntoIterator<Item = (usize, &'a mut Array<B>)>,
    ) -> Result<()>
    where
        B: BufferManager + 'a,
    {
        let buf = self.data.as_slice();
        let selection = src_row_offset..(src_row_offset + count);
        layout.decode_arrays(buf, &self.heap, selection, output_arrays)?;

        Ok(())
    }
}

/// State for resumable scanning of the row collection.
#[derive(Debug)]
pub struct RowCollectionScanState {
    /// Remaining chunks to scan.
    chunks_to_scan: VecDeque<usize>,
    /// Collects row addresses as we scan them.
    row_addresses: Vec<RowAddress>,
}

impl RowCollectionScanState {
    /// Get the row addresses from the most recent scan.
    pub fn row_addresses(&self) -> &[RowAddress] {
        &self.row_addresses
    }
}

#[cfg(test)]
mod tests {
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::datatype::DataType;
    use crate::arrays::testutil::{assert_arrays_eq, assert_batches_eq, generate_batch};

    #[test]
    fn append_single_batch_i32() {
        let mut collection = RowCollection::new(RowLayout::new([DataType::Int32]), 16);
        let input = generate_batch!([1, 2, 3, 4, 5, 6]);
        collection.append_batch(&input).unwrap();

        let mut output = Batch::try_new([DataType::Int32], 16).unwrap();

        let mut state = collection.init_full_scan();
        collection.scan(&mut state, &mut output).unwrap();
        assert_eq!(6, state.row_addresses().len());

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

        let mut state = collection.init_full_scan();
        let scan_count = collection.scan(&mut state, &mut output).unwrap();
        assert_eq!(6, scan_count);
        assert_eq!(6, state.row_addresses().len());

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
        let mut state = collection.init_full_scan();

        let scan_count = collection.scan(&mut state, &mut output).unwrap();
        assert_eq!(16, scan_count);
        assert_eq!(16, state.row_addresses().len());

        let expected1 = generate_batch!(0..16);
        assert_batches_eq(&expected1, &output);

        let scan_count = collection.scan(&mut state, &mut output).unwrap();
        assert_eq!(16, scan_count);
        assert_eq!(16, state.row_addresses().len());

        let expected2 = generate_batch!(16..32);
        assert_batches_eq(&expected2, &output);
    }

    #[test]
    fn append_batch_scan_column_subset() {
        let mut collection =
            RowCollection::new(RowLayout::new([DataType::Int32, DataType::Utf8]), 16);
        let input = generate_batch!([1, 2, 3, 4], ["a", "b", "c", "d"]);
        collection.append_batch(&input).unwrap();

        // Scan just the string column.
        let mut output = Array::try_new(&NopBufferManager, DataType::Utf8, 4).unwrap();

        let mut state = collection.init_full_scan();
        collection
            .scan_columns(&mut state, &[1], &mut [&mut output], 4)
            .unwrap();

        let expected = Array::try_from_iter(["a", "b", "c", "d"]).unwrap();
        assert_arrays_eq(&expected, &output);
    }

    #[test]
    fn append_batch_scan_no_chunks() {
        let mut collection =
            RowCollection::new(RowLayout::new([DataType::Int32, DataType::Utf8]), 16);
        let input = generate_batch!([1, 2, 3, 4], ["a", "b", "c", "d"]);
        collection.append_batch(&input).unwrap();

        // Dummy output, nothing should be written.
        let mut output = Array::try_new(&NopBufferManager, DataType::Utf8, 4).unwrap();

        let mut state = collection.init_partial_scan([]);
        let count = collection
            .scan_columns(&mut state, &[1], &mut [&mut output], 4)
            .unwrap();
        assert_eq!(0, count);
    }

    #[test]
    fn append_multiple_batches_scan_single_chunk() {
        let mut collection =
            RowCollection::new(RowLayout::new([DataType::Int32, DataType::Utf8]), 4);

        let input1 = generate_batch!([1, 2, 3, 4], ["a", "b", "c", "d"]);
        collection.append_batch(&input1).unwrap();
        let input2 = generate_batch!([5, 6, 7, 8], ["e", "f", "g", "h"]);
        collection.append_batch(&input2).unwrap();
        let input3 = generate_batch!([9, 10, 11, 12], ["i", "j", "k", "l"]);
        collection.append_batch(&input3).unwrap();

        let mut output = Array::try_new(&NopBufferManager, DataType::Utf8, 4).unwrap();

        let mut state = collection.init_partial_scan([1]);
        let count = collection
            .scan_columns(&mut state, &[1], &mut [&mut output], 4)
            .unwrap();
        assert_eq!(4, count);

        let expected = Array::try_from_iter(["e", "f", "g", "h"]).unwrap();
        assert_arrays_eq(&expected, &output);

        let count = collection
            .scan_columns(&mut state, &[1], &mut [&mut output], 4)
            .unwrap();
        assert_eq!(0, count);
    }

    #[test]
    fn row_ptr_read_column_value() {
        // Read a column value by getting a pointer to the row and offsetting
        // into it using layout.
        let mut collection = RowCollection::new(
            RowLayout::new([DataType::Int32, DataType::Utf8, DataType::Int32]),
            16,
        );
        let input = generate_batch!([1, 2, 3, 4], ["cat", "dog", "goose", "moose"], [5, 6, 7, 8]);
        collection.append_batch(&input).unwrap();

        let row_ptr = unsafe {
            collection.row_ptr(RowAddress {
                chunk_idx: 0,
                row_idx: 2, // '3, "goose", 7'
            })
        };

        let ptr = unsafe { row_ptr.byte_add(collection.layout().offsets[2]) };
        let v = unsafe { ptr.cast::<i32>().read_unaligned() };

        assert_eq!(7, v);
    }
}
