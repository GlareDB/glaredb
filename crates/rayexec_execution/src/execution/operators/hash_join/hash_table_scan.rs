use rayexec_error::Result;

use super::join_hash_table::JoinHashTable;
use crate::arrays::array::buffer_manager::NopBufferManager;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::collection::row_blocks::BlockReadState;
use crate::arrays::collection::row_matcher::MatchState;
use crate::logical::logical_join::JoinType;

/// Scan state for resuming probes of the hash table.
///
/// This gets updated after a single probe to the hash table. `scan_next` should
/// be called with the same join keys until we produce a result with zero rows.
#[derive(Debug)]
pub struct HashTableScanState {
    /// Selection for the rows we're still scanning. Applies to both the row
    /// pointers and keys.
    ///
    /// Once this is empty, we know we're done scanning this set of keys.
    pub selection: Vec<usize>,
    /// Current set of pointers we're working with.
    ///
    /// The 'i'th row pointer corresponds the 'i'th row in the keys we're
    /// probing for.
    ///
    /// We'll continually update these until we reach the end of the chain.
    pub row_pointers: Vec<*const u8>,
    /// Reusable hashes buffer. Filled when we probe the hash table with
    /// rhs_keys.
    pub hashes: Vec<u64>,
    /// State for the predicate matchers.
    pub match_state: MatchState,
    /// State used to read rows for the build side.
    pub block_read: BlockReadState,
}

impl HashTableScanState {
    pub fn scan_next(
        &mut self,
        table: &JoinHashTable,
        rhs: &mut Batch,
        output: &mut Batch,
    ) -> Result<()> {
        match table.join_type {
            JoinType::Inner => self.scan_next_inner_join(table, rhs, output),
            _ => unimplemented!(),
        }
    }

    fn scan_next_inner_join(
        &mut self,
        table: &JoinHashTable,
        rhs: &mut Batch,
        output: &mut Batch,
    ) -> Result<()> {
        if self.selection.is_empty() {
            println!("SEL EMPTY");
            // Done, need new of keys.
            output.set_num_rows(0)?;
            return Ok(());
        }

        let comparison_cols: Vec<_> = table
            .probe_comparison_columns
            .iter()
            .map(|&idx| &rhs.arrays[idx])
            .collect();

        let match_count = self.match_inner_join(table, &comparison_cols)?;
        if match_count == 0 {
            // All chains at the end, found no matches.
            output.set_num_rows(0)?;
            return Ok(());
        }

        // Store pointers to the matched rows.
        self.block_read.clear();
        self.block_read.row_pointers.extend(
            self.match_state
                .get_row_matches()
                .iter()
                .map(|&pred_idx| self.row_pointers[pred_idx]),
        );

        // Update 'matches' column if needed.
        if table.join_type.produce_all_build_side_rows() {
            // SAFTEY: Assumes that the row pointers we have actually point the
            // rows.
            unsafe {
                table.write_rows_matched(self.block_read.row_pointers.iter().copied());
            }
        }

        // Get LHS data from the table. Skips trying to read hashes or the
        // matches column.
        let lhs_col_count = table.data.layout().num_columns() - table.extra_column_count();
        debug_assert_eq!(lhs_col_count + rhs.arrays.len(), output.arrays.len());

        let lhs_arrays = (0..lhs_col_count).zip(&mut output.arrays);
        // SAFETY: ...
        unsafe { table.data.scan_raw(&self.block_read, lhs_arrays, 0) }?;

        // Select rhs data.
        let rhs_out = &mut output.arrays[lhs_col_count..];
        for (rhs_out, rhs) in rhs_out.iter_mut().zip(&mut rhs.arrays) {
            rhs_out.select_from_other(
                &NopBufferManager,
                rhs,
                self.match_state.get_row_matches().iter().copied(),
            )?;
        }

        output.set_num_rows(match_count)?;

        // Go to next entries in the chain, next call to scan will then match
        // against those entries.
        self.follow_next_in_chain(table);

        Ok(())
    }

    /// Find the rows from `rhs_keys` that match the predicates.
    ///
    /// Outputs will be placed in `predicated_matched` vector, and the number of
    /// matches returned.
    ///
    /// This will follow the pointer chain if the current set of pointers
    /// produces no matches. If zero is returned, we're at the end of all of the
    /// chains.
    fn match_inner_join(
        &mut self,
        table: &JoinHashTable,
        comparison_cols: &[&Array],
    ) -> Result<usize> {
        loop {
            // Rows to read from the left.
            let lhs_rows = &self.row_pointers;
            // Row indices to compare between left and right.
            let selection = self.selection.iter().copied();

            let match_count = table.row_matcher.find_matches(
                &mut self.match_state,
                table.data.layout(),
                &lhs_rows,
                &table.build_comparison_columns,
                comparison_cols,
                selection,
            )?;

            if match_count > 0 {
                // Predicates matched, need to produce output.
                return Ok(match_count);
            }

            // Otherwise none of the predicates matched, move to next entries.
            self.follow_next_in_chain(table);
            if self.selection.is_empty() {
                // We're at the end of all chains, nothing more to read.
                return Ok(0);
            }
        }
    }

    /// For each entry in the current scan state, follow the chain to load the
    /// next entry to read from.
    fn follow_next_in_chain(&mut self, table: &JoinHashTable) {
        for &ent_idx in &self.selection {
            let ent = &mut self.row_pointers[ent_idx];
            if !ent.is_null() {
                // Move to next in the chain.
                // SAFETY: ...
                //
                // Assumes that we're generating row addresses correctly and that
                // they are in bounds.
                *ent = unsafe { table.read_next_entry_ptr(*ent) }
            }
        }

        // Update the selection to only include entries that are not null.
        self.selection.clear();
        self.selection.extend(
            self.row_pointers
                .iter()
                .enumerate()
                .filter_map(|(idx, ent)| if ent.is_null() { None } else { Some(idx) }),
        );
    }
}
