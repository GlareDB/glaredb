use glaredb_error::{Result, not_implemented};

use super::{HashTableOperatorState, JoinHashTable};
use crate::arrays::batch::Batch;
use crate::arrays::cache::NopCache;
use crate::arrays::row::block_scan::BlockScanState;
use crate::arrays::scalar::ScalarValue;
use crate::buffer::buffer_manager::DefaultBufferManager;
use crate::execution::operators::hash_join::hash_table::needs_match_column;
use crate::expr::physical::evaluator::ExpressionEvaluator;
use crate::logical::logical_join::JoinType;

// TODO:
// - [ ] LEFT/OUTER drain
// - [ ] RIGHT
// - [ ] SEMI/ANTI
// - [ ] MARK

/// Scan state for resuming probes of the hash table.
///
/// This gets updated after a single probe to the hash table. `scan_next` should
/// be called with the same join keys until we produce a result with zero rows.
#[derive(Debug)]
pub struct HashTablePartitionScanState {
    pub partition_idx: usize,
    /// Selection for the rows we're still scanning. Applies to both the row
    /// pointers and keys.
    ///
    /// Once this is empty, we know we're done scanning this set of keys.
    ///
    /// Updated by the predicate row matcher.
    pub selection: Vec<usize>,
    /// Indicator for rows in the right batch that matched.
    ///
    /// Used for RIGHT/OUTER joins.
    pub right_matches: Vec<bool>,
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
    /// State used to read rows for the build side.
    pub block_read: BlockScanState,
    /// Evaluating for producing join keys for the probe side.
    pub join_keys_evaluator: ExpressionEvaluator,
    /// Probe-side join keys.
    ///
    /// Updated when we probe with a new RHS.
    ///
    /// This will contain everything we need for the comparisons. We should not
    /// need to consult the original RHS for that.
    pub join_keys: Batch,
    /// Unused vec for the row matcher. Just need something to drain into.
    pub not_matched: Vec<usize>,
}

// SAFETY: The `Vec<*mut u8>` is just a buffer for storing row pointers.
unsafe impl Send for HashTablePartitionScanState {}
unsafe impl Sync for HashTablePartitionScanState {}

impl HashTablePartitionScanState {
    /// Scan the next batch.
    ///
    /// This should continually be called with the same RHS batch until output
    /// has zero rows.
    pub fn scan_next(
        &mut self,
        table: &JoinHashTable,
        op_state: &HashTableOperatorState,
        rhs: &mut Batch,
        output: &mut Batch,
    ) -> Result<()> {
        match table.join_type {
            JoinType::Inner => self.scan_next_inner_join(table, op_state, rhs, output, false),
            JoinType::Left => {
                // Same as inner join for this step. This will internally mark
                // visited rows, and we'll drain the unvisited rows after we're
                // finished scanning.
                self.scan_next_inner_join(table, op_state, rhs, output, true)
            }
            JoinType::Right => self.scan_next_right_join(table, op_state, rhs, output),
            JoinType::Full => {
                // Same as right join, internally marks which left rows we
                // visited. And we'll drain at the end.
                self.scan_next_right_join(table, op_state, rhs, output)
            }
            JoinType::LeftMark { .. } => {
                self.scan_next_left_mark_join(table, op_state, rhs, output)
            }
            other => not_implemented!("scan join type: {other}"),
        }
    }

    fn scan_next_right_join(
        &mut self,
        table: &JoinHashTable,
        op_state: &HashTableOperatorState,
        rhs: &mut Batch,
        output: &mut Batch,
    ) -> Result<()> {
        // Call the normal inner join.
        self.scan_next_inner_join(table, op_state, rhs, output, true)?;

        if output.num_rows() == 0 {
            // We've finished the 'normal' scan. We need to flush out rows from
            // the right that we didn't visit.
            let right_arr_len = rhs.arrays.len();
            let left_arr_len = output.arrays.len() - right_arr_len;

            let not_match_sel: Vec<_> = self
                .right_matches
                .iter()
                .enumerate()
                .filter_map(|(idx, &did_match)| if !did_match { Some(idx) } else { None })
                .collect();

            if not_match_sel.is_empty() {
                // Everything on the right matched, nothing we should do.
                return Ok(());
            }

            // Only run the selection once, this will short circuit the next
            // scan right.
            // TODO: Definitely jank.
            self.right_matches.clear();

            // Mark all left arrays as NULL.
            let left_arrs = &mut output.arrays[0..left_arr_len];
            for left_arr in left_arrs {
                left_arr.set_value(0, &ScalarValue::Null)?;
                left_arr.select(
                    &DefaultBufferManager,
                    std::iter::repeat_n(0, not_match_sel.len()),
                )?;
            }

            let right_out_arrs = &mut output.arrays[left_arr_len..];

            // Select the right rows.
            for (right_out, right_in) in right_out_arrs.iter_mut().zip(&mut rhs.arrays) {
                right_out.select_from_other(
                    &DefaultBufferManager,
                    right_in,
                    not_match_sel.iter().copied(),
                    &mut NopCache,
                )?;
            }

            output.set_num_rows(not_match_sel.len())?;
        }

        Ok(())
    }

    fn scan_next_inner_join(
        &mut self,
        table: &JoinHashTable,
        op_state: &HashTableOperatorState,
        rhs: &mut Batch,
        output: &mut Batch,
        track_right: bool,
    ) -> Result<()> {
        if self.selection.is_empty() {
            // Done, need new rhs.
            output.set_num_rows(0)?;
            return Ok(());
        }

        let match_count = self.match_inner_join(table, track_right)?;
        if match_count == 0 {
            // All chains at the end, found no matches.
            output.set_num_rows(0)?;
            return Ok(());
        }

        // Store pointers to the matched rows.
        self.block_read.clear();
        self.block_read.row_pointers.extend(
            self.selection
                .iter()
                .map(|&pred_idx| self.row_pointers[pred_idx]),
        );

        // Update 'matches' column if needed.
        if needs_match_column(table.join_type) {
            // SAFTEY: Assumes that the row pointers we have actually point the
            // rows.
            unsafe {
                table.write_rows_matched(self.block_read.row_pointers.iter().copied());
            }
        }

        // Get LHS data from the table. Skips trying to read hashes or the
        // matches column.
        let data = unsafe { op_state.merged_row_collection.get() };
        debug_assert_eq!(
            table.data_column_count + rhs.arrays.len(),
            output.arrays.len(),
            "Output should only contain columns for the original inputs to left and right",
        );

        // Only decode the original inputs from the left side.
        let lhs_arrays = (0..table.data_column_count).zip(&mut output.arrays);
        // SAFETY: ...
        unsafe { data.scan_raw(&self.block_read, lhs_arrays, 0) }?;

        // Select rhs data.
        let rhs_out = &mut output.arrays[table.data_column_count..];
        for (rhs_out, rhs) in rhs_out.iter_mut().zip(&mut rhs.arrays) {
            rhs_out.select_from_other(
                &DefaultBufferManager,
                rhs,
                self.selection.iter().copied(),
                &mut NopCache,
            )?;
        }

        output.set_num_rows(match_count)?;

        // Go to next entries in the chain, next call to scan will then match
        // against those entries.
        self.follow_next_in_chain(table);

        Ok(())
    }

    /// "Scans" the next mark join results.
    ///
    /// This will always return an output batch with zero rows. When we "scan"
    /// here, we're just marking visited rows on the left.
    fn scan_next_left_mark_join(
        &mut self,
        table: &JoinHashTable,
        _op_state: &HashTableOperatorState,
        _rhs: &mut Batch,
        output: &mut Batch,
    ) -> Result<()> {
        if self.selection.is_empty() {
            output.set_num_rows(0)?;
            return Ok(());
        }

        loop {
            let match_count = self.match_inner_join(table, false)?;
            if match_count == 0 {
                // All chains at the end, found no matches.
                output.set_num_rows(0)?;
                return Ok(());
            }

            debug_assert!(
                needs_match_column(table.join_type),
                "Left mark always needs match column"
            );

            // SAFTEY: Assumes that the row pointers we have actually point the
            // rows.
            unsafe {
                table.write_rows_matched(self.block_read.row_pointers.iter().copied());
            }

            // Move to next in chain.
            self.follow_next_in_chain(table);
            // Continue... we'll keep looping until we've followed all chains to
            // the end.
        }
    }

    /// Find the rows from `rhs_keys` that match the predicates.
    ///
    /// Outputs will be placed in `predicated_matched` vector, and the number of
    /// matches returned.
    ///
    /// This will follow the pointer chain if the current set of pointers
    /// produces no matches. If zero is returned, we're at the end of all of the
    /// chains.
    fn match_inner_join(&mut self, table: &JoinHashTable, track_right: bool) -> Result<usize> {
        loop {
            let lhs_rows = &self.row_pointers;

            debug_assert_eq!(table.encoded_key_columns.len(), self.join_keys.arrays.len());

            self.not_matched.clear(); // Not used.

            // Compare the encoded keys with the keys we generated for the RHS.
            let match_count = table.row_matcher.find_matches(
                &table.layout,
                lhs_rows,
                &table.encoded_key_columns,
                &self.join_keys.arrays,
                &mut self.selection,
                &mut self.not_matched,
            )?;

            if track_right {
                // Mark right rows matched.
                for &sel_idx in &self.selection {
                    self.right_matches[sel_idx] = true;
                }
            }

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
    ///
    /// The selection will be updated to only point to non-null pointers.
    fn follow_next_in_chain(&mut self, table: &JoinHashTable) {
        let mut new_count = 0;

        // SAFETY: Assumes row_pointers contains valid addresses and that
        // read_next_entry_ptr safely reads the next pointer in the chain.
        for i in 0..self.selection.len() {
            let idx = self.selection[i];
            let ent = &mut self.row_pointers[idx];

            // Advance to the next pointer in the chain
            *ent = unsafe { table.read_next_entry_ptr(*ent) };

            // Keep only non-null entries
            if !ent.is_null() {
                self.selection[new_count] = idx;
                new_count += 1;
            }
        }

        // Truncate selection to remove unused entries
        self.selection.truncate(new_count);
    }
}
