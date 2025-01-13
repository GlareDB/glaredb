use rayexec_error::Result;

use super::encode::prefix_encode;
use super::sort_layout::SortLayout;
use crate::arrays::batch::Batch;

#[derive(Debug)]
pub struct SortData {
    /// Capacity per block.
    max_per_block: usize,
    /// Blocks not yet sorted. Can continue to be written to.
    unsorted: Vec<SortBlock>,
    /// Sorted blocks with each block containing sorted rows.
    ///
    /// The order of blocks themselves is arbitrary.
    sorted: Vec<SortBlock>,
}

impl SortData {
    pub fn new(max_per_block: usize) -> Self {
        SortData {
            max_per_block,
            unsorted: Vec::new(),
            sorted: Vec::new(),
        }
    }

    pub fn push_batch(&mut self, layout: &SortLayout, batch: &Batch) -> Result<()> {
        let mut block = self.pop_or_allocate_unsorted_block(layout, batch.num_rows())?;

        let mut add_offset = 0;

        // Slice buffer to start at where we want to begin writing.
        let curr_offset = block.key_encode_offsets[block.batch.num_rows()];
        let buf = &mut block.key_encode_buffer[curr_offset..];

        for (idx, sort_col) in layout.key_columns.iter().enumerate() {
            let nulls_first = layout.key_nulls_first[idx];
            let desc = layout.key_desc[idx];
            let offsets = &block.key_encode_offsets;

            let key_array = &batch.arrays[*sort_col];
            let sel = batch.selection();

            // Encode this sort key.
            prefix_encode(key_array, sel, add_offset, offsets, buf, nulls_first, desc)?;

            // Update add offet to get to the correct offset for subsequent
            // keys.
            add_offset += layout.key_sizes[idx];
        }

        block.batch.append(batch)?;
        self.unsorted.push(block);

        Ok(())
    }

    pub fn take_sorted_for_merge(&mut self) -> Vec<SortBlock> {
        debug_assert_eq!(0, self.unsorted.len());
        std::mem::take(&mut self.sorted)
    }

    pub fn sort_unsorted_blocks(&mut self, layout: &SortLayout) -> Result<()> {
        let mut sort_indices_buf = Vec::new();

        for block in self.unsorted.drain(..) {
            sort_indices_buf.resize(block.batch.num_rows(), 0);
            let sorted = block.sort(layout, &mut sort_indices_buf)?;
            self.sorted.push(sorted);
        }

        Ok(())
    }

    /// Pops the last block if it has room for `count` number of additional
    /// rows. Otherwise we allocate a new block.
    ///
    /// Pops to satisfy lifetimes more easily.
    fn pop_or_allocate_unsorted_block(
        &mut self,
        layout: &SortLayout,
        count: usize,
    ) -> Result<SortBlock> {
        debug_assert!(count <= self.max_per_block);

        if let Some(last) = self.unsorted.last() {
            if last.has_capacity_for_rows(count) {
                return Ok(self.unsorted.pop().unwrap());
            }
        }

        let block = SortBlock::new(layout, self.max_per_block)?;

        Ok(block)
    }
}

/// Blocks containing unsorted input and encoded keys.
#[derive(Debug)]
pub struct SortBlock {
    /// Offsets of the first sort key into the key encode buffer.
    pub key_encode_offsets: Vec<usize>,
    /// Buffer containing all encoded keys.
    pub key_encode_buffer: Vec<u8>,
    /// Collection hold all arrays for sort input.
    pub batch: Batch,
}

impl SortBlock {
    pub fn new(layout: &SortLayout, capacity: usize) -> Result<Self> {
        let total_key_size: usize = layout.key_sizes.iter().sum();

        let offsets = (0..capacity).map(|idx| idx * total_key_size).collect();
        let buffer = vec![0; total_key_size * capacity]; // TODO: Get from manager.

        let batch = Batch::try_new(layout.input_types.clone(), capacity)?;

        Ok(SortBlock {
            key_encode_offsets: offsets,
            key_encode_buffer: buffer,
            batch,
        })
    }

    pub fn has_capacity_for_rows(&self, count: usize) -> bool {
        (self.batch.num_rows() + count) <= self.batch.capacity
    }

    /// Get a buffer slice representing the encoded sort keys for a row.
    pub fn get_sort_key_buf(&self, row_idx: usize) -> &[u8] {
        let start = self.key_encode_offsets[row_idx];
        let end = if row_idx + 1 == self.key_encode_offsets.len() {
            self.key_encode_buffer.len()
        } else {
            self.key_encode_offsets[row_idx + 1]
        };
        &self.key_encode_buffer[start..end]
    }

    pub fn get_sort_key_buf_mut(&mut self, row_idx: usize) -> &mut [u8] {
        let start = self.key_encode_offsets[row_idx];
        let end = if row_idx + 1 == self.key_encode_offsets.len() {
            self.key_encode_buffer.len()
        } else {
            self.key_encode_offsets[row_idx + 1]
        };
        &mut self.key_encode_buffer[start..end]
    }

    pub fn row_count(&self) -> usize {
        self.batch.num_rows()
    }

    /// Copy a row from another sort block into this sort block.
    pub fn copy_row_from_other(
        &mut self,
        dest_row: usize,
        source: &SortBlock,
        source_row: usize,
    ) -> Result<()> {
        // Copy encoded keys.
        let source_buf = source.get_sort_key_buf(source_row);
        let dest_buf = self.get_sort_key_buf_mut(dest_row);
        dest_buf.copy_from_slice(source_buf);

        // Copy actual row data.
        source
            .batch
            .copy_rows(&[(source_row, dest_row)], &mut self.batch)?;

        Ok(())
    }

    fn sort(mut self, layout: &SortLayout, sort_indices: &mut [usize]) -> Result<SortBlock> {
        debug_assert_eq!(sort_indices.len(), self.batch.num_rows());

        // Reset sort indices to 0..rowlen
        sort_indices
            .iter_mut()
            .enumerate()
            .for_each(|(idx, v)| *v = idx);

        // TODO: Need to account for varlen ties.
        sort_indices.sort_by_key(|&idx| {
            let start = self.key_encode_offsets[idx];
            let end = self.key_encode_offsets[idx + 1];
            &self.key_encode_buffer[start..end]
        });

        // TODO: Track
        let mut output_buf = vec![0; self.key_encode_buffer.len()];

        // Reorder buffer.
        for (dest_idx, &source_idx) in sort_indices.iter().enumerate() {
            let dest_start = self.key_encode_offsets[dest_idx];
            let dest_end = self.key_encode_offsets[dest_idx + 1];
            let dest = &mut output_buf[dest_start..dest_end];

            let source_start = self.key_encode_offsets[source_idx];
            let source_end = self.key_encode_offsets[source_idx + 1];
            let source = &self.key_encode_buffer[source_start..source_end];

            dest.copy_from_slice(source);
        }

        // Update batch block by selecting by sort indices.
        self.batch.select(sort_indices)?;

        Ok(SortBlock {
            key_encode_offsets: self.key_encode_offsets,
            key_encode_buffer: output_buf,
            batch: self.batch,
        })
    }
}

#[cfg(test)]
mod tests {
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::Array;
    use crate::arrays::datatype::DataType;
    use crate::arrays::testutil::assert_batches_eq;
    use crate::expr::physical::column_expr::PhysicalColumnExpr;
    use crate::expr::physical::PhysicalSortExpression;

    #[test]
    fn sort_i32_batches_asc() {
        let layout = SortLayout::new(
            vec![DataType::Int32],
            &[PhysicalSortExpression {
                column: PhysicalColumnExpr {
                    idx: 0,
                    datatype: DataType::Int32,
                },
                desc: false,
                nulls_first: false,
            }],
        );

        let mut sort_data = SortData::new(4096);

        let batch1 = Batch::try_from_arrays([Array::try_from_iter([4, 7, 6]).unwrap()]).unwrap();
        let batch2 = Batch::try_from_arrays([Array::try_from_iter([2, 8]).unwrap()]).unwrap();

        sort_data.push_batch(&layout, &batch1).unwrap();
        sort_data.push_batch(&layout, &batch2).unwrap();

        sort_data.sort_unsorted_blocks(&layout).unwrap();

        assert_eq!(0, sort_data.unsorted.len());
        assert_eq!(1, sort_data.sorted.len());

        let expected =
            Batch::try_from_arrays([Array::try_from_iter([2, 4, 6, 7, 8]).unwrap()]).unwrap();

        assert_batches_eq(&expected, &sort_data.sorted[0].batch);
    }

    #[test]
    fn sort_i32_batches_desc() {
        let layout = SortLayout::new(
            vec![DataType::Int32],
            &[PhysicalSortExpression {
                column: PhysicalColumnExpr {
                    idx: 0,
                    datatype: DataType::Int32,
                },
                desc: true,
                nulls_first: false,
            }],
        );

        let mut sort_data = SortData::new(4096);

        let batch1 = Batch::try_from_arrays([Array::try_from_iter([4, 7, 6]).unwrap()]).unwrap();
        let batch2 = Batch::try_from_arrays([Array::try_from_iter([2, 8]).unwrap()]).unwrap();

        sort_data.push_batch(&layout, &batch1).unwrap();
        sort_data.push_batch(&layout, &batch2).unwrap();

        sort_data.sort_unsorted_blocks(&layout).unwrap();

        assert_eq!(0, sort_data.unsorted.len());
        assert_eq!(1, sort_data.sorted.len());

        let expected =
            Batch::try_from_arrays([Array::try_from_iter([8, 7, 6, 4, 2]).unwrap()]).unwrap();

        assert_batches_eq(&expected, &sort_data.sorted[0].batch);
    }
}
