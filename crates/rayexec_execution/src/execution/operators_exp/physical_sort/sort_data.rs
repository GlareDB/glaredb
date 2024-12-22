use rayexec_error::Result;

use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::buffer::physical_type::{PhysicalI8, PhysicalType};
use crate::arrays::buffer_manager::BufferManager;
use crate::arrays::datatype::DataType;
use crate::execution::operators_exp::physical_sort::encode::prefix_encode;
use crate::expr::physical::PhysicalSortExpression;

#[derive(Debug)]
pub struct SortData<B: BufferManager> {
    manager: B,
    max_per_block: usize,
    layout: SortLayout,
    /// Blocks not yet sorted. Can continue to be written to.
    unsorted: Vec<Block<B>>,
    /// Sorted blocks, should not be written to.
    sorted: Vec<Block<B>>,
}

impl<B> SortData<B>
where
    B: BufferManager,
{
    pub fn push_batch(&mut self, batch: &Batch) -> Result<()> {
        let mut block = self.pop_or_allocate_block(batch.num_rows())?;

        let mut add_offset = 0;

        for (idx, sort_col) in self.layout.key_columns.iter().enumerate() {
            let nulls_first = self.layout.key_nulls_first[idx];
            let desc = self.layout.key_desc[idx];
            let buf = &mut block.key_encode_buffer;
            let offsets = &block.key_encode_offsets;

            let sel = batch.generate_selection();
            let key_array = batch.get_array(*sort_col)?;

            // Encode this sort key.
            prefix_encode(key_array, sel, add_offset, offsets, buf, nulls_first, desc)?;

            // Update add offet to get to the correct offset for subsequent
            // keys.
            add_offset += self.layout.key_sizes[idx];
        }

        self.unsorted.push(block);

        unimplemented!()
    }

    /// Pops the last block if it has room for `count` number of additional
    /// rows. Otherwise we allocate a new block.
    ///
    /// Pops to satisfy lifetimes more easily.
    fn pop_or_allocate_block(&mut self, count: usize) -> Result<Block<B>> {
        debug_assert!(count <= self.max_per_block);

        if let Some(last) = self.unsorted.last() {
            if last.row_count + count <= self.max_per_block {
                return Ok(self.unsorted.pop().unwrap());
            }
        }

        let block = self.layout.new_block(&self.manager, self.max_per_block)?;

        Ok(block)
    }
}

#[derive(Debug)]
struct SortLayout {
    key_types: Vec<DataType>,
    key_columns: Vec<usize>,
    key_sizes: Vec<usize>,
    key_nulls_first: Vec<bool>,
    key_desc: Vec<bool>,
}

impl SortLayout {
    fn new(key_types: Vec<DataType>, exprs: &[PhysicalSortExpression]) -> Self {
        debug_assert_eq!(key_types.len(), exprs.len());

        let key_columns = exprs.iter().map(|expr| expr.column.idx).collect();
        let key_nulls_first = exprs.iter().map(|expr| expr.nulls_first).collect();
        let key_desc = exprs.iter().map(|expr| expr.desc).collect();

        let key_sizes = key_types
            .iter()
            .map(|key_type| {
                let size = match key_type.physical_type() {
                    PhysicalType::Int8 => std::mem::size_of::<i8>(),
                    PhysicalType::Int32 => std::mem::size_of::<i32>(),
                    _ => unimplemented!(),
                };
                size + 1 // Account for validity byte. Currently we set it for everything.
            })
            .collect();

        SortLayout {
            key_desc,
            key_types,
            key_sizes,
            key_columns,
            key_nulls_first,
        }
    }

    /// Create a new block adhering to this sort layout.
    fn new_block<B: BufferManager>(&self, manager: &B, capacity: usize) -> Result<Block<B>> {
        let total_key_size: usize = self.key_sizes.iter().sum();

        let offsets = (0..capacity).map(|idx| idx * total_key_size).collect();
        let buffer = vec![0; total_key_size * capacity]; // TODO: Get from manager.

        let arrays = self
            .key_types
            .iter()
            .map(|typ| Array::new(manager, typ.clone(), capacity))
            .collect::<Result<Vec<_>>>()?;

        Ok(Block {
            row_count: 0,
            key_encode_offsets: offsets,
            key_encode_buffer: buffer,
            arrays,
        })
    }
}

#[derive(Debug)]
struct Block<B: BufferManager> {
    /// Number of rows we're currently storing in this block.
    ///
    /// Should not exceed the capacity of the arrays.
    row_count: usize,
    /// Offsets of the first sort key into the key encode buffer.
    key_encode_offsets: Vec<usize>,
    /// Buffer containing all encoded keys.
    key_encode_buffer: Vec<u8>,
    /// Arrays contains batch input data.
    arrays: Vec<Array<B>>,
}
