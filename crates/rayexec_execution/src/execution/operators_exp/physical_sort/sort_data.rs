use rayexec_error::Result;

use super::sort_layout::SortLayout;
use crate::arrays::batch::Batch;
use crate::arrays::buffer::physical_type::{PhysicalI8, PhysicalType};
use crate::arrays::buffer_manager::BufferManager;
use crate::arrays::datatype::DataType;
use crate::execution::operators_exp::batch_collection::BatchCollectionBlock;
use crate::execution::operators_exp::physical_sort::encode::prefix_encode;
use crate::expr::physical::PhysicalSortExpression;

// TODO:
// - varlen tiebreaks

#[derive(Debug)]
pub struct SortData<B: BufferManager> {
    /// Buffer manager.
    manager: B,
    /// Capacity per block.
    max_per_block: usize,
    /// Layout indicating how we're performing the sort.
    layout: SortLayout,
    /// Blocks not yet sorted. Can continue to be written to.
    unsorted: Vec<SortBlock<B>>,
    /// Sorted blocks with each block containing sorted rows.
    ///
    /// The order of blocks themselves is arbitrary.
    sorted: Vec<SortBlock<B>>,
}

impl<B> SortData<B>
where
    B: BufferManager,
{
    pub fn new(
        manager: B,
        max_per_block: usize,
        input_types: Vec<DataType>,
        sort_exprs: &[PhysicalSortExpression],
    ) -> Result<Self> {
        let layout = SortLayout::new(input_types, sort_exprs);

        Ok(SortData {
            manager,
            max_per_block,
            layout,
            unsorted: Vec::new(),
            sorted: Vec::new(),
        })
    }

    pub fn push_batch(&mut self, batch: &Batch<B>) -> Result<()> {
        let mut block = self.pop_or_allocate_unsorted_block(batch.num_rows())?;

        let mut add_offset = 0;

        // Slice buffer to start at where we want to begin writing.
        let curr_offset = block.key_encode_offsets[block.block.row_count()];
        let buf = &mut block.key_encode_buffer[curr_offset..];

        for (idx, sort_col) in self.layout.key_columns.iter().enumerate() {
            let nulls_first = self.layout.key_nulls_first[idx];
            let desc = self.layout.key_desc[idx];
            let offsets = &block.key_encode_offsets;

            let key_array = batch.get_array(*sort_col)?;
            let sel = batch.generate_selection();

            // Encode this sort key.
            prefix_encode(key_array, sel, add_offset, offsets, buf, nulls_first, desc)?;

            // Update add offet to get to the correct offset for subsequent
            // keys.
            add_offset += self.layout.key_sizes[idx];
        }

        block.block.append_batch_data(batch)?;

        self.unsorted.push(block);

        Ok(())
    }

    pub fn sort_unsorted_blocks(&mut self) -> Result<()> {
        let mut sort_indices_buf = Vec::new();

        for block in self.unsorted.drain(..) {
            sort_indices_buf.resize(block.block.row_count(), 0);
            let sorted = block.sort(&self.manager, &self.layout, &mut sort_indices_buf)?;
            self.sorted.push(sorted);
        }

        Ok(())
    }

    /// Pops the last block if it has room for `count` number of additional
    /// rows. Otherwise we allocate a new block.
    ///
    /// Pops to satisfy lifetimes more easily.
    fn pop_or_allocate_unsorted_block(&mut self, count: usize) -> Result<SortBlock<B>> {
        debug_assert!(count <= self.max_per_block);

        if let Some(last) = self.unsorted.last() {
            if last.block.has_capacity_for_rows(count) {
                return Ok(self.unsorted.pop().unwrap());
            }
        }

        let block = SortBlock::new(&self.manager, &self.layout, self.max_per_block)?;

        Ok(block)
    }
}

/// Blocks containing unsorted input and encoded keys.
#[derive(Debug)]
pub struct SortBlock<B: BufferManager> {
    /// Offsets of the first sort key into the key encode buffer.
    pub key_encode_offsets: Vec<usize>,
    /// Buffer containing all encoded keys.
    pub key_encode_buffer: Vec<u8>,
    /// Collection hold all arrays for sort input.
    pub block: BatchCollectionBlock<B>,
}

impl<B> SortBlock<B>
where
    B: BufferManager,
{
    pub fn new(manager: &B, layout: &SortLayout, capacity: usize) -> Result<Self> {
        let total_key_size: usize = layout.key_sizes.iter().sum();

        let offsets = (0..capacity).map(|idx| idx * total_key_size).collect();
        let buffer = vec![0; total_key_size * capacity]; // TODO: Get from manager.

        let block = BatchCollectionBlock::new(manager, &layout.input_types, capacity)?;

        Ok(SortBlock {
            key_encode_offsets: offsets,
            key_encode_buffer: buffer,
            block,
        })
    }

    /// Get a buffer slice representing the encoded sort keys for a row.
    pub fn get_sort_key_buf(&self, row_idx: usize) -> &[u8] {
        let start = self.key_encode_offsets[row_idx];
        let end = self.key_encode_offsets[row_idx + 1];
        &self.key_encode_buffer[start..end]
    }

    pub fn get_sort_key_buf_mut(&mut self, row_idx: usize) -> &mut [u8] {
        let start = self.key_encode_offsets[row_idx];
        let end = self.key_encode_offsets[row_idx + 1];
        &mut self.key_encode_buffer[start..end]
    }

    pub fn row_count(&self) -> usize {
        self.block.row_count()
    }

    /// Copy a row from another sort block into this sort block.
    pub fn copy_row_from_other(&mut self, dest_row: usize, source: &SortBlock<B>, source_row: usize) -> Result<()> {
        // Copy encoded keys.
        let source_buf = source.get_sort_key_buf(source_row);
        let dest_buf = self.get_sort_key_buf_mut(dest_row);
        dest_buf.copy_from_slice(source_buf);

        // Copy actual row data.
        self.block.copy_row_from_other(dest_row, &source.block, source_row)?;

        Ok(())
    }

    fn sort(mut self, manager: &B, layout: &SortLayout, sort_indices: &mut [usize]) -> Result<SortBlock<B>> {
        debug_assert_eq!(sort_indices.len(), self.block.row_count());

        // Reset sort indices to 0..rowlen
        sort_indices.iter_mut().enumerate().for_each(|(idx, v)| *v = idx);

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
        self.block.select(manager, sort_indices)?;

        Ok(SortBlock {
            key_encode_offsets: self.key_encode_offsets,
            key_encode_buffer: output_buf,
            block: self.block,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::array::Array;
    use crate::arrays::buffer::physical_type::PhysicalI32;
    use crate::arrays::buffer::Int32BufferBuilder;
    use crate::arrays::buffer_manager::NopBufferManager;
    use crate::arrays::executor::scalar::unary::UnaryExecutor;
    use crate::expr::physical::column_expr::PhysicalColumnExpr;

    #[test]
    fn sort_i32_batches() {
        let mut sort_data = SortData::new(
            NopBufferManager,
            4096,
            vec![DataType::Int32],
            &[PhysicalSortExpression {
                column: PhysicalColumnExpr { idx: 0 },
                desc: false,
                nulls_first: false,
            }],
        )
        .unwrap();

        let batch1 = Batch::from_arrays(
            [Array::new_with_buffer(
                DataType::Int32,
                Int32BufferBuilder::from_iter([4, 7, 6]).unwrap(),
            )],
            true,
        )
        .unwrap();
        let batch2 = Batch::from_arrays(
            [Array::new_with_buffer(
                DataType::Int32,
                Int32BufferBuilder::from_iter([2, 8]).unwrap(),
            )],
            true,
        )
        .unwrap();

        sort_data.push_batch(&batch1).unwrap();
        sort_data.push_batch(&batch2).unwrap();

        sort_data.sort_unsorted_blocks().unwrap();

        assert_eq!(0, sort_data.unsorted.len());
        assert_eq!(1, sort_data.sorted.len());
        assert_eq!(5, sort_data.sorted[0].block.row_count());

        let mut out = vec![0; 5];
        let flat = sort_data.sorted[0].block.arrays()[0].flat_view().unwrap();
        UnaryExecutor::for_each_flat::<PhysicalI32, _>(flat, 0..5, |idx, v| {
            out[idx] = v.copied().unwrap();
        })
        .unwrap();

        assert_eq!(vec![2, 4, 6, 7, 8], out);
    }
}
