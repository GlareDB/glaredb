use hashbrown::raw::RawTable;
use rayexec_bullet::{batch::Batch, compute, datatype::DataType};
use rayexec_error::Result;
use std::{collections::HashMap, fmt};

use crate::execution::operators::util::outer_join_tracker::{
    LeftOuterJoinTracker, RightOuterJoinTracker,
};

use super::condition::{HashJoinCondition, LeftPrecomputedJoinConditions};

/// Points to a row in the hash table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RowKey {
    /// Index of the batch in the batches vector.
    batch_idx: usize,
    /// Index of the row in the batch.
    row_idx: usize,
}

pub struct JoinHashTable {
    /// All collected batches.
    batches: Vec<Batch>,
    /// Conditions we're joining on.
    conditions: LeftPrecomputedJoinConditions,
    /// Hash table pointing to a row.
    hash_table: RawTable<(u64, RowKey)>,
    /// Column types for left side of join.
    ///
    /// Used when generating the left columns for a RIGHT OUTER join.
    left_types: Vec<DataType>,
    /// If we're a right join.
    right_join: bool,
}

impl JoinHashTable {
    pub fn new(
        left_types: Vec<DataType>,
        conditions: &[HashJoinCondition],
        right_join: bool,
    ) -> Self {
        let conditions = conditions.iter().map(|c| c.clone().into()).collect();

        JoinHashTable {
            batches: Vec::new(),
            conditions: LeftPrecomputedJoinConditions { conditions },
            hash_table: RawTable::new(),
            left_types,
            right_join,
        }
    }

    /// Insert a batch into the hash table for the left side of the join.
    ///
    /// `hash_indices` indicates which columns in the batch was used to compute
    /// the hashes.
    pub fn insert_batch(&mut self, batch: Batch, hashes: &[u64]) -> Result<()> {
        assert_eq!(batch.num_rows(), hashes.len());

        self.conditions.precompute_for_left_batch(&batch)?;

        let batch_idx = self.batches.len();
        self.batches.push(batch);

        for (row_idx, hash) in hashes.iter().enumerate() {
            let row_key = RowKey { batch_idx, row_idx };
            self.hash_table
                .insert(*hash, (*hash, row_key), |(hash, _)| *hash);
        }

        Ok(())
    }

    pub fn collected_batches(&self) -> &[Batch] {
        &self.batches
    }

    /// Merge some other hash table into this one.
    pub fn merge(&mut self, mut other: Self) -> Result<()> {
        let batch_offset = self.batches.len();

        // Append all batches from other. When we drain the hash table, we'll
        // update the row keys to account for the new offset.
        self.batches.append(&mut other.batches);

        // Append all precompute left results.
        //
        // Similar to above, we just append precomputed results for each
        // condition which keeps the offset in sync.
        for (c1, c2) in self
            .conditions
            .conditions
            .iter_mut()
            .zip(other.conditions.conditions.iter_mut())
        {
            c1.left_precomputed.append(&mut c2.left_precomputed);
        }

        for (hash, mut row_key) in other.hash_table.drain() {
            row_key.batch_idx += batch_offset;
            self.hash_table
                .insert(hash, (hash, row_key), |(hash, _)| *hash);
        }

        Ok(())
    }

    pub fn probe(
        &self,
        right: &Batch,
        hashes: &[u64],
        mut left_outer_tracker: Option<&mut LeftOuterJoinTracker>,
    ) -> Result<Vec<Batch>> {
        // Track per-batch row indices that match the input columns.
        //
        // The value is a vec of (left_idx, right_idx) pairs pointing to rows in
        // the left (build) and right (probe) batches respectively
        let mut row_indices: HashMap<usize, Vec<(usize, usize)>> = HashMap::new();

        for (right_idx, hash) in hashes.iter().enumerate() {
            // Get all matching row keys from hash table.
            //
            // SAFETY: Iterator only lives for this method call.
            // See: https://docs.rs/hashbrown/latest/hashbrown/raw/struct.RawTable.html#method.iter_hash
            unsafe {
                self.hash_table.iter_hash(*hash).for_each(|bucket| {
                    let val = bucket.as_ref(); // Unsafe
                    let row_key = val.1;

                    // Hashbrown only stores first seven bits of hash. We check
                    // here to further prune items we pull out of the table.
                    //
                    // Note this still doesn't guarantee row equality. That is
                    // checked when we actually execute the conditions, this
                    // just gets us the candidates.
                    if &val.0 != hash {
                        return;
                    }

                    // This is all safe, just adding to the row_indices vec.
                    use std::collections::hash_map::Entry;
                    match row_indices.entry(row_key.batch_idx) {
                        Entry::Occupied(mut ent) => {
                            ent.get_mut().push((row_key.row_idx, right_idx))
                        }
                        Entry::Vacant(ent) => {
                            ent.insert(vec![(row_key.row_idx, right_idx)]);
                        }
                    }
                })
            }
        }

        let mut right_tracker = if self.right_join {
            Some(RightOuterJoinTracker::new_for_batch(right))
        } else {
            None
        };

        let mut batches = Vec::with_capacity(row_indices.len());
        for (batch_idx, row_indices) in row_indices {
            let (left_rows, right_rows): (Vec<_>, Vec<_>) = row_indices.into_iter().unzip();

            // Update left visit bitmaps with rows we're visiting from batches
            // in the hash table.
            //
            // May be None if we're not doing a LEFT JOIN.
            if let Some(left_outer_tracker) = left_outer_tracker.as_mut() {
                left_outer_tracker.mark_rows_visited_for_batch(batch_idx, &left_rows);
            }

            // Update right unvisited bitmap. May be None if we're not doing a
            // RIGHT JOIN.
            if let Some(right_outer_tracker) = right_tracker.as_mut() {
                right_outer_tracker.mark_rows_visited(&right_rows);
            }

            // Initial right side of the batch.
            let initial_right_side = Batch::try_new(
                right
                    .columns()
                    .iter()
                    .map(|arr| compute::take::take(arr.as_ref(), &right_rows))
                    .collect::<Result<Vec<_>>>()?,
            )?;

            // Run through conditions. This will also check the column equality
            // for the join key (since it's just another condition).
            let selection = self.conditions.compute_selection_for_probe(
                batch_idx,
                &left_rows,
                &initial_right_side,
            )?;

            // Prune left row indices using selection.
            let left_rows: Vec<_> = left_rows
                .into_iter()
                .zip(selection.values().iter())
                .filter_map(|(left_row, selected)| if selected { Some(left_row) } else { None })
                .collect();

            // Get the left columns for this batch.
            let left_batch = self.batches.get(batch_idx).expect("batch to exist");
            let left_cols = left_batch
                .columns()
                .iter()
                .map(|arr| compute::take::take(arr.as_ref(), &left_rows))
                .collect::<Result<Vec<_>>>()?;

            // Trim down right cols using only selected rows.
            let right_cols = initial_right_side
                .columns()
                .iter()
                .map(|arr| compute::filter::filter(arr, &selection))
                .collect::<Result<Vec<_>>>()?;

            // Create final batch.
            let batch = Batch::try_new(left_cols.into_iter().chain(right_cols))?;
            batches.push(batch);
        }

        // Append batch from RIGHT OUTER if needed.
        if let Some(right_tracker) = right_tracker {
            let extra = right_tracker.into_unvisited(&self.left_types, right)?;
            batches.push(extra);
        }

        Ok(batches)
    }
}

impl fmt::Debug for JoinHashTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartitionHashTable").finish_non_exhaustive()
    }
}
