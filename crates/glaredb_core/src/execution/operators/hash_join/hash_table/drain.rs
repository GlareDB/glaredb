use glaredb_error::{DbError, Result};

use super::{HashTableOperatorState, JoinHashTable};
use crate::arrays::batch::Batch;
use crate::logical::logical_join::JoinType;

pub const fn needs_drain(join_type: JoinType) -> bool {
    match join_type {
        JoinType::Left | JoinType::LeftSemi | JoinType::Full | JoinType::LeftMark { .. } => true,
        _ => false, // TODO
    }
}

/// Drain state for LEFT/OUTER/MARK joins.
#[derive(Debug)]
pub struct HashTablePartitionDrainState {
    pub partition_idx: usize,
    /// Buffer to row pointers to read from.
    pub row_pointers: Vec<*const u8>,
    /// Current block we're reading from.
    pub curr_block_idx: usize,
    /// Current row within the block we're draining from.
    pub curr_row: usize,
}

unsafe impl Send for HashTablePartitionDrainState {}
unsafe impl Sync for HashTablePartitionDrainState {}

impl HashTablePartitionDrainState {
    pub fn drain_next(
        &mut self,
        table: &JoinHashTable,
        op_state: &HashTableOperatorState,
        output: &mut Batch,
    ) -> Result<()> {
        match table.join_type {
            JoinType::LeftSemi => self.drain_left_semi(table, op_state, output),
            JoinType::LeftMark { .. } => self.drain_left_mark(table, op_state, output),
            other => Err(DbError::new(format!(
                "Unexpected join type for drain: {other}"
            ))),
        }
    }

    fn drain_left_mark(
        &mut self,
        table: &JoinHashTable,
        op_state: &HashTableOperatorState,
        output: &mut Batch,
    ) -> Result<()> {
        output.reset_for_write()?;

        // Match all pointers. We want to return if a row did or didn't match.
        self.load_row_ptrs(table, op_state, output, |_| true)?;

        debug_assert_eq!(output.arrays.len(), table.data_column_count + 1);
        let match_idx = table.matches_column_idx().expect("match column to exist");

        let [left_arrs, match_arrs] = output
            .arrays
            .get_disjoint_mut([
                0..table.data_column_count,
                table.data_column_count..table.data_column_count + 1,
            ])
            .expect("indices to exist and be disjoint");

        // [left_arrs, match]
        let arr_iter = left_arrs
            .iter_mut()
            .enumerate()
            .chain(std::iter::once((match_idx, &mut match_arrs[0])));

        unsafe {
            table
                .layout
                .read_arrays(self.row_pointers.iter().copied(), arr_iter, 0)?
        };

        Ok(())
    }

    fn drain_left_semi(
        &mut self,
        table: &JoinHashTable,
        op_state: &HashTableOperatorState,
        output: &mut Batch,
    ) -> Result<()> {
        output.reset_for_write()?;

        // We want to drain unmatched rows.
        self.load_row_ptrs(table, op_state, output, |did_match| !did_match)?;

        debug_assert_eq!(output.arrays.len(), table.data_column_count);

        let left_arrs = &mut output.arrays[0..table.data_column_count];
        unsafe {
            table.layout.read_arrays(
                self.row_pointers.iter().copied(),
                left_arrs.iter_mut().enumerate(),
                0,
            )?
        };

        Ok(())
    }

    /// Loads the next set of row pointers that can fit in the output.
    ///
    /// `match_fn` gets passed the "matched" boolean, and returns if the row
    /// pointer should be used.
    fn load_row_ptrs(
        &mut self,
        table: &JoinHashTable,
        op_state: &HashTableOperatorState,
        output: &mut Batch,
        match_fn: impl Fn(bool) -> bool,
    ) -> Result<()> {
        let out_cap = output.write_capacity()?;

        let matches_byte_offset =
            table.layout.offsets[table.matches_column_idx().expect("matches column to exist")];

        self.row_pointers.clear();

        loop {
            let collection = unsafe { op_state.merged_row_collection.get() };
            if collection.blocks().row_blocks.len() >= self.curr_block_idx {
                // No more blocks for us.
                break;
            }

            let row_count = collection.blocks().rows_in_row_block(self.curr_block_idx);
            let block_ptr = collection.blocks().row_blocks[self.curr_block_idx].as_ptr();

            for row_idx in (self.curr_row)..row_count {
                let row_ptr = unsafe { block_ptr.byte_add(table.layout.row_width) };
                let did_match = unsafe {
                    row_ptr
                        .byte_add(matches_byte_offset)
                        .cast::<bool>()
                        .read_unaligned()
                };
                if !match_fn(did_match) {
                    continue;
                }

                self.row_pointers.push(row_ptr);
                if self.row_pointers.len() == out_cap {
                    // We just processed the current row, +1 may go beyond the
                    // end of the block, but that'll short circuite the loop on
                    // the next drain.
                    self.curr_row = row_idx + 1;
                    break;
                }
            }

            if self.row_pointers.len() == out_cap {
                break;
            }

            // Move to next block...
            //
            // Each partition is scanning a disjoint set of blocks.
            self.curr_block_idx += op_state.partition_count();
            self.curr_row = 0;
        }

        Ok(())
    }
}
