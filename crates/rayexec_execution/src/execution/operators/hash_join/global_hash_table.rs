use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use hashbrown::raw::RawTable;
use rayexec_bullet::batch::BatchOld;
use rayexec_bullet::datatype::DataTypeOld;
use rayexec_bullet::selection::SelectionVector;
use rayexec_error::Result;

use super::condition::{
    HashJoinCondition,
    LeftPrecomputedJoinCondition,
    LeftPrecomputedJoinConditions,
};
use super::partition_hash_table::{PartitionHashTable, RowKey};
use crate::execution::operators::util::outer_join_tracker::{
    LeftOuterJoinTracker,
    RightOuterJoinTracker,
};

/// Global hash table shared across all partitions for a single instance of a
/// hash join operator.
///
/// Contains read-only left-side data, and probed using batches from the right
/// side.
pub struct GlobalHashTable {
    /// All collected batches.
    batches: Vec<BatchOld>,
    /// Conditions we're joining on.
    conditions: LeftPrecomputedJoinConditions,
    /// Hash table pointing to a row.
    hash_table: RawTable<(u64, RowKey)>,
    /// Column types for left side of join.
    ///
    /// Used when generating the left columns for a RIGHT OUTER join.
    left_types: Vec<DataTypeOld>,
    /// If we're a right join.
    right_join: bool,
    /// If we're a mark join.
    ///
    /// If true, we won't actually be doing any joining, and instead just update
    /// the visit bitmaps.
    is_mark: bool,
}

impl GlobalHashTable {
    /// Merge many partition hash tables into a new global hash table.
    pub fn new(
        left_types: Vec<DataTypeOld>,
        right_join: bool,
        is_mark: bool,
        partition_tables: Vec<PartitionHashTable>,
        conditions: &[HashJoinCondition],
    ) -> Self {
        // Merge all partition tables left to right.

        let batches_cap: usize = partition_tables.iter().map(|t| t.batches.len()).sum();
        let hash_table_cap: usize = partition_tables.iter().map(|t| t.hash_table.len()).sum();
        let precomputed_cap: usize = partition_tables
            .iter()
            .map(|t| {
                t.conditions
                    .conditions
                    .iter()
                    .map(|c| c.left_precomputed.len())
                    .sum::<usize>()
            })
            .sum();

        let mut batches = Vec::with_capacity(batches_cap);
        let mut hash_table = RawTable::with_capacity(hash_table_cap);

        let mut conditions = LeftPrecomputedJoinConditions {
            conditions: conditions
                .iter()
                .map(|c| {
                    LeftPrecomputedJoinCondition::from_condition_with_capacity(
                        c.clone(),
                        precomputed_cap,
                    )
                })
                .collect(),
        };

        for mut table in partition_tables {
            let batch_offset = batches.len();

            // Merge batches.
            batches.append(&mut table.batches);

            // Merge hash tables, updating row key to point to the correct batch
            // in the merged batch vec.
            for (hash, mut row_key) in table.hash_table.drain() {
                row_key.batch_idx += batch_offset as u32;
                hash_table.insert(hash, (hash, row_key), |(hash, _)| *hash);
            }

            // Append all precompute left results.
            //
            // We just append precomputed results for each condition which keeps
            // the offset in sync.
            for (c1, c2) in conditions
                .conditions
                .iter_mut()
                .zip(table.conditions.conditions.iter_mut())
            {
                c1.left_precomputed.append(&mut c2.left_precomputed);
            }
        }

        GlobalHashTable {
            batches,
            conditions,
            hash_table,
            left_types,
            right_join,
            is_mark,
        }
    }

    pub fn collected_batches(&self) -> &[BatchOld] {
        &self.batches
    }

    /// Probe the table.
    pub fn probe(
        &self,
        right: &BatchOld,
        hashes: &[u64],
        mut left_outer_tracker: Option<&mut LeftOuterJoinTracker>,
    ) -> Result<Vec<BatchOld>> {
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
                    match row_indices.entry(row_key.batch_idx as usize) {
                        Entry::Occupied(mut ent) => {
                            ent.get_mut().push((row_key.row_idx as usize, right_idx))
                        }
                        Entry::Vacant(ent) => {
                            ent.insert(vec![(row_key.row_idx as usize, right_idx)]);
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
            // Initial selection vectors, may include rows that aren't part of
            // the join.
            //
            // Contains rows on left and right that had the same hash key. Still
            // needs run through the filter expressions.
            let (left_row_sel, right_row_sel): (SelectionVector, SelectionVector) =
                row_indices.into_iter().unzip();

            // Actual selection vectors, rows not part of the join are pruned
            // out.
            let (left_row_sel, right_row_sel) = self.conditions.compute_selection_for_probe(
                batch_idx,
                left_row_sel,
                right_row_sel,
                right,
            )?;

            // Update right unvisited bitmap. May be None if we're not doing a
            // RIGHT JOIN.
            if let Some(right_outer_tracker) = right_tracker.as_mut() {
                right_outer_tracker.mark_rows_visited(right_row_sel.iter_locations());
            }

            // Update left visit bitmaps with rows we're visiting from batches
            // in the hash table.
            //
            // This is done _after_ evaluating the join conditions which may
            // result in fewer rows on the left that we're actually joining
            // with.
            //
            // May be None if we're not doing a LEFT JOIN.
            if let Some(left_outer_tracker) = left_outer_tracker.as_mut() {
                left_outer_tracker
                    .mark_rows_visited_for_batch(batch_idx, left_row_sel.iter_locations());
            }

            // Don't actually do the join.
            if self.is_mark {
                // But continue working on the next batch.
                continue;
            }

            // Get the left columns for this batch.
            let left_batch = self.batches.get(batch_idx).expect("batch to exist");
            let left_cols = left_batch.select(Arc::new(left_row_sel)).into_arrays();

            // Get the right.
            let right_cols = right.select(Arc::new(right_row_sel)).into_arrays();

            // Create final batch.
            let batch = BatchOld::try_new(left_cols.into_iter().chain(right_cols))?;
            batches.push(batch);
        }

        // Append batch from RIGHT OUTER if needed.
        if let Some(right_tracker) = right_tracker {
            if let Some(extra) = right_tracker.into_unvisited(&self.left_types, right)? {
                batches.push(extra);
            }
        }

        Ok(batches)
    }
}

impl fmt::Debug for GlobalHashTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GlobalHashTable").finish_non_exhaustive()
    }
}
