use glaredb_error::{Result, not_implemented};

use super::{HashTableOperatorState, JoinHashTable};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::cache::NopCache;
use crate::arrays::row::block_scan::BlockScanState;
use crate::buffer::buffer_manager::DefaultBufferManager;
use crate::execution::operators::hash_join::hash_table::needs_match_column;
use crate::expr::physical::evaluator::ExpressionEvaluator;
use crate::logical::logical_join::JoinType;

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
            JoinType::LeftSemi => {
                // We're not going to be producing any batches until we drain.
                // Just mark the left rows as visited.
                //
                // LEFT SEMI join will one (and only one) row for visited left
                // side rows. We can only guarantee we return one row during
                // draining.
                self.scan_next_left_mark(table, op_state, rhs, output)
            }
            JoinType::LeftMark { .. } => self.scan_next_left_mark(table, op_state, rhs, output),
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
                let mut const_null = Array::new_null(
                    &DefaultBufferManager,
                    left_arr.datatype().clone(),
                    not_match_sel.len(),
                )?;
                left_arr.swap(&mut const_null)?;
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

        let matched_sel = self.chase_until_match_or_exhaust(table, track_right)?;
        if matched_sel.is_empty() {
            // All chains at the end, found no matches.
            output.set_num_rows(0)?;
            return Ok(());
        }

        // Store pointers to the matched rows.
        self.block_read.clear();
        self.block_read
            .row_pointers
            .extend(matched_sel.iter().map(|&idx| self.row_pointers[idx]));

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

        // TODO: Allow doing this more selectively.
        output.reset_for_write()?;

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
                matched_sel.iter().copied(),
                &mut NopCache,
            )?;
        }

        output.set_num_rows(matched_sel.len())?;

        // Go to next entries in the chain, next call to scan will then match
        // against those entries.
        Self::follow_next_in_chain(table, &mut self.row_pointers, &mut self.selection);

        Ok(())
    }

    /// "Scans" the next join results.
    ///
    /// This will always return an output batch with zero rows. When we "scan"
    /// here, we're just marking visited rows on the left.
    fn scan_next_left_mark(
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
            let matched_sel = self.chase_until_match_or_exhaust(table, false)?;
            if matched_sel.is_empty() {
                // All chains at the end, found no matches.
                output.set_num_rows(0)?;
                return Ok(());
            }

            debug_assert!(
                needs_match_column(table.join_type),
                "Left mark(-like) join always needs match column"
            );

            self.block_read.clear();
            self.block_read
                .row_pointers
                .extend(matched_sel.iter().map(|&idx| self.row_pointers[idx]));

            // SAFTEY: Assumes that the row pointers we have actually point the
            // rows.
            unsafe {
                table.write_rows_matched(self.block_read.row_pointers.iter().copied());
            }

            // Move to next in chain.
            Self::follow_next_in_chain(table, &mut self.row_pointers, &mut self.selection);
            // Continue... we'll keep looping until we've followed all chains to
            // the end.
        }
    }

    /// Find the selected rows from the left that match the join keys.
    fn chase_until_match_or_exhaust(
        &mut self,
        table: &JoinHashTable,
        track_right: bool,
    ) -> Result<Vec<usize>> {
        while !self.selection.is_empty() {
            // Try to match all of active at their current pointers
            let mut sel = self.selection.clone();

            debug_assert_eq!(table.encoded_key_columns.len(), self.join_keys.arrays.len());

            self.not_matched.clear(); // Not used.

            // Compare the encoded keys with the keys we generated for the RHS.
            let _ = table.row_matcher.find_matches(
                &table.layout,
                &self.row_pointers,
                &table.encoded_key_columns,
                &self.join_keys.arrays,
                &mut sel,
                &mut self.not_matched,
            )?;

            if track_right {
                // Mark right rows matched.
                for &sel_idx in &sel {
                    self.right_matches[sel_idx] = true;
                }
            }

            // If we got any, we’re done.
            if !sel.is_empty() {
                return Ok(sel);
            }

            // Otherwise none of the predicates matched, move pointers to next
            // in chain, pruning indices as needed.
            Self::follow_next_in_chain(table, &mut self.row_pointers, &mut self.selection);
        }

        // All chains exhausted, no matches.
        Ok(Vec::new())
    }

    /// For each selected row pointer, update it to move to the next in the
    /// chain.
    ///
    /// The `selection` will be updated to only point to non-null row pointers.
    fn follow_next_in_chain(
        table: &JoinHashTable,
        row_pointers: &mut [*const u8],
        selection: &mut Vec<usize>,
    ) {
        let mut new_count = 0;

        // SAFETY: Assumes row_pointers contains valid addresses and that
        // read_next_entry_ptr safely reads the next pointer in the chain.
        for idx in 0..selection.len() {
            let ptr_idx = selection[idx];
            let ent = &mut row_pointers[ptr_idx];

            // Advance to the next pointer in the chain
            *ent = unsafe { table.read_next_entry_ptr(*ent) };

            // Keep only non-null entries
            if !ent.is_null() {
                selection[new_count] = ptr_idx;
                new_count += 1;
            }
        }

        // Truncate selection to remove unused entries
        selection.truncate(new_count);
    }
}
