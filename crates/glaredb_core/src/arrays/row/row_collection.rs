use std::borrow::{Borrow, BorrowMut};

use glaredb_error::{DbError, Result};

use super::block::ValidityInitializer;
use super::block_scan::BlockScanState;
use super::row_blocks::{BlockAppendState, RowBlocks};
use super::row_layout::RowLayout;
use super::row_scan::RowScanState;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::buffer::buffer_manager::DefaultBufferManager;

/// State for appending data to the collection.
#[derive(Debug)]
pub struct RowAppendState {
    /// State for appending to row/heap blocks.
    block_append: BlockAppendState,
    /// Reusable buffer for computing heaps sizes needed per row.
    heap_sizes: Vec<usize>,
}

impl RowAppendState {
    /// Returns a reference to the row pointers for the most recent insert into
    /// the collection.
    ///
    /// This can be used to get access to the raw data without needing to scan
    /// it into Arrays.
    pub fn row_pointers(&self) -> &[*const u8] {
        // SAFETY: Same in-memory representation. Mut/const pointers don't
        // actually mean anything in regards to value mutability.
        unsafe { std::mem::transmute::<&[*mut u8], &[*const u8]>(&self.block_append.row_pointers) }
    }
}

/// Collects array data by first row-encoding the data and storing it in raw
/// buffers.
#[derive(Debug)]
pub struct RowCollection {
    layout: RowLayout,
    blocks: RowBlocks<ValidityInitializer>,
}

impl RowCollection {
    pub fn new(layout: RowLayout, block_capacity: usize) -> Self {
        RowCollection {
            blocks: RowBlocks::new_using_row_layout(&DefaultBufferManager, &layout, block_capacity),
            layout,
        }
    }

    /// Gets a reference to the row layout for this collection.
    pub const fn layout(&self) -> &RowLayout {
        &self.layout
    }

    /// Gets a reference to the underlying blocks backing this row collection.
    pub fn blocks(&self) -> &RowBlocks<ValidityInitializer> {
        &self.blocks
    }

    pub fn blocks_mut(&mut self) -> &mut RowBlocks<ValidityInitializer> {
        &mut self.blocks
    }

    pub fn row_count(&self) -> usize {
        self.blocks.reserved_row_count()
    }

    /// Initializes state for appending rows to this collection.
    pub fn init_append(&self) -> RowAppendState {
        RowAppendState {
            block_append: BlockAppendState {
                row_pointers: Vec::new(),
                heap_pointers: Vec::new(),
            },
            heap_sizes: Vec::new(),
        }
    }

    /// Appends a batch to the collection.
    ///
    /// The array types for this batch should match the types specified in the
    /// row layout.
    pub fn append_batch(&mut self, state: &mut RowAppendState, batch: &Batch) -> Result<()> {
        self.append_arrays(state, &batch.arrays, batch.num_rows)
    }

    /// Internal method for appending arrays to this collection.
    ///
    /// The input arrays must match the row layout for the collection.
    ///
    /// Array capacities must equal or exceed `num_rows`.
    pub(crate) fn append_arrays<A>(
        &mut self,
        state: &mut RowAppendState,
        arrays: &[A],
        num_rows: usize,
    ) -> Result<()>
    where
        A: Borrow<Array>,
    {
        debug_assert_eq!(arrays.len(), self.layout().num_columns());

        state.block_append.clear();
        if self.layout().requires_heap {
            // Compute heap sizes per row.
            state.heap_sizes.resize(num_rows, 0);
            self.layout()
                .compute_heap_sizes(arrays, 0..num_rows, &mut state.heap_sizes)?;
        }

        if self.layout().requires_heap {
            self.blocks.prepare_append(
                &mut state.block_append,
                num_rows,
                Some(&state.heap_sizes),
            )?
        } else {
            self.blocks
                .prepare_append(&mut state.block_append, num_rows, None)?
        }

        // SAFETY: We assume that the pointers we computed are inbounds with
        // respect the blocks, and that we correctly computed the heap sizes.
        unsafe {
            self.layout()
                .write_arrays(&mut state.block_append, arrays, 0..num_rows)?
        };

        Ok(())
    }

    /// Initialize a scan state for scanning all row blocks in the collection.
    pub fn init_full_scan(&self) -> RowScanState {
        let mut state = RowScanState::empty();
        state.reset_for_full_scan(&self.blocks);
        state
    }

    /// Initialize a scan of only some of the row_blocks.
    pub fn init_partial_scan(
        &self,
        block_indices: impl IntoIterator<Item = usize>,
    ) -> RowScanState {
        let mut state = RowScanState::empty();
        state.reset_for_partial_scan(block_indices);
        state
    }

    /// Scan the collection, writing rows to `output`.
    ///
    /// The number of rows scanned is returned. Zero is returned if either the
    /// output batches capacity is zero, or if there are no more rows to scan
    /// according the scan state.
    ///
    /// This updates the scan state to allow for resuming scans.
    pub fn scan(&self, state: &mut RowScanState, output: &mut Batch) -> Result<usize> {
        let capacity = output.write_capacity()?;
        let count = state.scan(&self.layout, &self.blocks, &mut output.arrays, capacity)?;
        output.set_num_rows(count)?;

        Ok(count)
    }

    /// Scan a subset of columns into the output arrays.
    ///
    /// `columns` provides which columns to scan.
    ///
    /// `count` indicates the max number of rows to write to the array. This
    /// must be less than or equal to the array capacities.
    pub fn scan_columns<A>(
        &self,
        state: &mut RowScanState,
        columns: &[usize],
        outputs: &mut [A],
        count: usize,
    ) -> Result<usize>
    where
        A: BorrowMut<Array>,
    {
        state.scan_subset(
            &self.layout,
            &self.blocks,
            columns.iter().copied(),
            outputs,
            count,
        )
    }

    #[allow(unused)] // Useful for tests.
    pub(crate) unsafe fn scan_raw<'a, A>(
        &self,
        state: &BlockScanState,
        arrays: impl IntoIterator<Item = (usize, &'a mut A)>,
        write_offset: usize,
    ) -> Result<()>
    where
        A: BorrowMut<Array> + 'a,
    {
        unsafe {
            self.layout()
                .read_arrays(state.row_pointers_iter(), arrays, write_offset)
        }
    }

    /// Merges `other` into self.
    ///
    /// Both collections must have the same layout.
    pub fn merge_from(&mut self, other: &mut Self) -> Result<()> {
        if self.layout() != other.layout() {
            return Err(DbError::new(
                "Attemped to merge row collections with different layouts",
            ));
        }

        self.blocks.merge_blocks_from(&mut other.blocks);

        Ok(())
    }

    /// Produces a batch containing all data in the row collection.
    #[cfg(debug_assertions)]
    #[allow(unused)]
    pub fn debug_dump(&self) -> Batch {
        let mut batch = Batch::new(self.layout().types.clone(), self.row_count()).unwrap();
        let mut state = self.init_full_scan();
        self.scan(&mut state, &mut batch).unwrap();

        batch
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::datatype::DataType;
    use crate::testutil::arrays::{assert_arrays_eq, assert_batches_eq, generate_batch};
    use crate::util::iter::TryFromExactSizeIterator;

    #[test]
    fn append_single_batch_i32() {
        let mut collection =
            RowCollection::new(RowLayout::try_new([DataType::int32()]).unwrap(), 16);
        let input = generate_batch!([1, 2, 3, 4, 5, 6]);
        let mut state = collection.init_append();
        collection.append_batch(&mut state, &input).unwrap();

        let mut output = Batch::new([DataType::int32()], 16).unwrap();

        let mut state = collection.init_full_scan();
        collection.scan(&mut state, &mut output).unwrap();
        assert_eq!(6, state.scanned_row_pointers().len());

        let expected = generate_batch!([1, 2, 3, 4, 5, 6]);
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn append_single_batch_i32_with_invalid() {
        let mut collection =
            RowCollection::new(RowLayout::try_new([DataType::int32()]).unwrap(), 16);
        let input = generate_batch!([Some(1), Some(2), None, Some(4), None, Some(6)]);
        let mut state = collection.init_append();
        collection.append_batch(&mut state, &input).unwrap();
        assert_eq!(6, collection.row_count());

        let mut output = Batch::new([DataType::int32()], 16).unwrap();

        let mut state = collection.init_full_scan();
        let scan_count = collection.scan(&mut state, &mut output).unwrap();
        assert_eq!(6, scan_count);
        assert_eq!(6, state.scanned_row_pointers().len());

        let expected = generate_batch!([Some(1), Some(2), None, Some(4), None, Some(6)]);
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn append_multiple_batches_i32_with_invalid() {
        let mut collection =
            RowCollection::new(RowLayout::try_new([DataType::int32()]).unwrap(), 16);
        let mut state = collection.init_append();
        let input1 = generate_batch!(0..16);
        collection.append_batch(&mut state, &input1).unwrap();
        let input2 = generate_batch!(16..32);
        collection.append_batch(&mut state, &input2).unwrap();
        assert_eq!(32, collection.row_count());

        let mut output = Batch::new([DataType::int32()], 16).unwrap();
        let mut state = collection.init_full_scan();

        let scan_count = collection.scan(&mut state, &mut output).unwrap();
        assert_eq!(16, scan_count);
        assert_eq!(16, state.scanned_row_pointers().len());

        let expected1 = generate_batch!(0..16);
        assert_batches_eq(&expected1, &output);

        let scan_count = collection.scan(&mut state, &mut output).unwrap();
        assert_eq!(16, scan_count);
        assert_eq!(16, state.scanned_row_pointers().len());

        let expected2 = generate_batch!(16..32);
        assert_batches_eq(&expected2, &output);
    }

    #[test]
    fn append_batch_scan_column_subset() {
        let mut collection = RowCollection::new(
            RowLayout::try_new([DataType::int32(), DataType::utf8()]).unwrap(),
            16,
        );
        let mut state = collection.init_append();
        let input = generate_batch!([1, 2, 3, 4], ["a", "b", "c", "d"]);
        collection.append_batch(&mut state, &input).unwrap();

        // Scan just the string column.
        let mut output = Array::new(&DefaultBufferManager, DataType::utf8(), 4).unwrap();

        let mut state = collection.init_full_scan();
        collection
            .scan_columns(&mut state, &[1], &mut [&mut output], 4)
            .unwrap();

        let expected = Array::try_from_iter(["a", "b", "c", "d"]).unwrap();
        assert_arrays_eq(&expected, &output);
    }

    #[test]
    fn append_batch_scan_no_chunks() {
        let mut collection = RowCollection::new(
            RowLayout::try_new([DataType::int32(), DataType::utf8()]).unwrap(),
            16,
        );
        let mut state = collection.init_append();
        let input = generate_batch!([1, 2, 3, 4], ["a", "b", "c", "d"]);
        collection.append_batch(&mut state, &input).unwrap();

        // Dummy output, nothing should be written.
        let mut output = Array::new(&DefaultBufferManager, DataType::utf8(), 4).unwrap();

        let mut state = collection.init_partial_scan([]);
        let count = collection
            .scan_columns(&mut state, &[1], &mut [&mut output], 4)
            .unwrap();
        assert_eq!(0, count);
    }

    #[test]
    fn append_multiple_batches_scan_single_chunk() {
        let mut collection = RowCollection::new(
            RowLayout::try_new([DataType::int32(), DataType::utf8()]).unwrap(),
            4,
        );
        let mut state = collection.init_append();

        let input1 = generate_batch!([1, 2, 3, 4], ["a", "b", "c", "d"]);
        collection.append_batch(&mut state, &input1).unwrap();
        let input2 = generate_batch!([5, 6, 7, 8], ["e", "f", "g", "h"]);
        collection.append_batch(&mut state, &input2).unwrap();
        let input3 = generate_batch!([9, 10, 11, 12], ["i", "j", "k", "l"]);
        collection.append_batch(&mut state, &input3).unwrap();

        let mut output = Array::new(&DefaultBufferManager, DataType::utf8(), 4).unwrap();

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
    fn merge_with_utf8() {
        let mut collection1 =
            RowCollection::new(RowLayout::try_new([DataType::utf8()]).unwrap(), 16);
        let mut state1 = collection1.init_append();

        collection1
            .append_batch(
                &mut state1,
                &generate_batch!(["a", "b", "c", "dmakesurethisdoesntgetinline"]),
            )
            .unwrap();

        let mut collection2 =
            RowCollection::new(RowLayout::try_new([DataType::utf8()]).unwrap(), 16);
        let mut state2 = collection2.init_append();

        collection2
            .append_batch(
                &mut state2,
                &generate_batch!(["cat", "dogdogdogdogdogdogdog", "goose"]),
            )
            .unwrap();

        collection1.merge_from(&mut collection2).unwrap();

        let mut output = Batch::new([DataType::utf8()], 16).unwrap();
        let mut state = collection1.init_full_scan();
        let count = collection1.scan(&mut state, &mut output).unwrap();
        assert_eq!(7, count);

        let expected = generate_batch!([
            "a",
            "b",
            "c",
            "dmakesurethisdoesntgetinline",
            "cat",
            "dogdogdogdogdogdogdog",
            "goose",
        ]);

        assert_batches_eq(&expected, &output);
    }
}
