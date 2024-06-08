use crate::functions::aggregate::GroupedStates;
use hashbrown::raw::RawTable;
use rayexec_bullet::{
    array::Array,
    batch::Batch,
    bitmap::Bitmap,
    field::DataType,
    row::{OwnedScalarRow, ScalarRow},
};
use rayexec_error::{RayexecError, Result};
use std::fmt;
use std::sync::Arc;

/// States for a single aggregation.
#[derive(Debug)]
pub struct AggregateStates {
    pub states: Box<dyn GroupedStates>,

    /// Bitmap for selecting columns from the input to the hash map.
    ///
    /// This is used to allow the hash map to handle states for different
    /// aggregates working on different columns. For example:
    ///
    /// SELECT SUM(a), MIN(b) FROM ...
    ///
    /// This query computes aggregates on columns 'a' and 'b', but to minimize
    /// work, we pass both 'a' and 'b' to the hash table in one pass. Then this
    /// bitmap is used to further refine the inputs specific to the aggregate.
    pub col_selection: Bitmap,
}

/// An aggregate hash table for storing group values alongside the computed
/// aggregates.
///
/// This can be used to store partial aggregate data for a single partition and
/// later combined other other hash tables also containing partial aggregate
/// data for the same partition.
pub struct PartitionAggregateHashTable {
    /// Statest for aggregates.
    ///
    /// There should exist one `AggregateState` per aggregate function call.
    ///
    /// - `SELECT SUM(a), ...` => length of 1
    /// - `SELECT SUM(a), MAX(b), ...` => length  of 2
    agg_states: Vec<AggregateStates>,

    // TODO: This is likely a peformance bottleneck with storing group values in
    // rows.
    group_values: Vec<OwnedScalarRow>,

    /// Hash table pointing to the group index.
    hash_table: RawTable<(u64, usize)>,

    /// Buffer used when looking for group indices for group values.
    indexes_buffer: Vec<usize>,
}

impl PartitionAggregateHashTable {
    /// Create a new hash table using the provided aggregate states.
    ///
    /// All states must have zero initialized states.
    pub fn try_new(agg_states: Vec<AggregateStates>) -> Result<Self> {
        for agg in &agg_states {
            if agg.states.num_groups() != 0 {
                return Err(RayexecError::new(format!(
                    "Attempted to initialize aggregate table with non-empty states: {agg:?}"
                )));
            }
        }

        Ok(PartitionAggregateHashTable {
            agg_states,
            group_values: Vec::new(),
            hash_table: RawTable::new(),
            indexes_buffer: Vec::new(),
        })
    }

    pub fn insert_groups(
        &mut self,
        groups: &[&Array],
        hashes: &[u64],
        inputs: &[&Array],
        selection: &Bitmap,
    ) -> Result<()> {
        let row_count = selection.popcnt();

        self.indexes_buffer.clear();
        self.indexes_buffer.reserve(row_count);

        // Get group indices, creating new states as needed for groups we've
        // never seen before.
        self.find_or_create_group_indices(groups, hashes, selection)?;

        // Now we just rip through the values.
        for agg_states in self.agg_states.iter_mut() {
            let input_cols: Vec<_> = agg_states
                .col_selection
                .iter()
                .zip(inputs.iter())
                .filter_map(|(selected, arr)| if selected { Some(*arr) } else { None })
                .collect();

            agg_states
                .states
                .update_states(selection, &input_cols, &self.indexes_buffer)?;
        }

        Ok(())
    }

    pub fn num_groups(&self) -> usize {
        self.group_values.len()
    }

    fn find_or_create_group_indices(
        &mut self,
        groups: &[&Array],
        hashes: &[u64],
        selection: &Bitmap,
    ) -> Result<()> {
        for (row_idx, (&hash, selected)) in hashes.iter().zip(selection.iter()).enumerate() {
            if !selected {
                continue;
            }

            // TODO: This is probably a bit slower than we'd want.
            //
            // It's like that replacing this with something that compares
            // scalars directly to a arrays at an index would be faster.
            let row = ScalarRow::try_new_from_arrays(groups, row_idx)?;

            // Look up the entry into the hash table.
            let ent = self.hash_table.get_mut(hash, |(_hash, group_idx)| {
                row == self.group_values[*group_idx]
            });

            match ent {
                Some((_, group_idx)) => {
                    // Group already exists.
                    self.indexes_buffer.push(*group_idx);
                }
                None => {
                    // Need to create new states and insert them into the hash table.
                    let mut states_iter = self.agg_states.iter_mut();

                    // Use first state to generate the group index. Each new
                    // state we create for this group should generate the same
                    // index.
                    let group_idx = match states_iter.next() {
                        Some(agg_state) => agg_state.states.new_group(),
                        None => {
                            return Err(RayexecError::new("Aggregate hash table has no aggregates"))
                        }
                    };

                    for agg_state in states_iter {
                        let idx = agg_state.states.new_group();
                        // Very critical, if we're not generating the same
                        // index, all bets are off.
                        assert_eq!(group_idx, idx);
                    }

                    self.hash_table
                        .insert(hash, (hash, group_idx), |(hash, _group_idx)| *hash);

                    self.group_values.push(row.into_owned());
                    self.indexes_buffer.push(group_idx);
                }
            }
        }

        Ok(())
    }

    /// Merge other hash table into self.
    pub fn merge(&mut self, mut other: Self) -> Result<()> {
        let row_count = other.group_values.len();
        if row_count == 0 {
            return Ok(());
        }

        self.indexes_buffer.clear();
        self.indexes_buffer.reserve(row_count);

        // Ensure the has table we're merging into has all the groups from
        // the other hash table.
        for (hash, group_idx) in other.hash_table.drain() {
            // TODO: Deduplicate with othe find and create method.

            let row = std::mem::replace(&mut other.group_values[group_idx], ScalarRow::empty());

            let ent = self.hash_table.get_mut(hash, |(_hash, self_group_idx)| {
                row == self.group_values[*self_group_idx]
            });

            match ent {
                Some((_, self_group_idx)) => {
                    // 'self' already has the group from the other table.
                    self.indexes_buffer.push(*self_group_idx)
                }
                None => {
                    // 'self' has never seend this group before. Add it to the map with
                    // an empty state.

                    let mut states_iter = self.agg_states.iter_mut();

                    // Use first state to generate the group index. Each new
                    // state we create for this group should generate the same
                    // index.
                    let group_idx = match states_iter.next() {
                        Some(agg_state) => agg_state.states.new_group(),
                        None => {
                            return Err(RayexecError::new("Aggregate hash table has no aggregates"))
                        }
                    };

                    for agg_state in states_iter {
                        let idx = agg_state.states.new_group();
                        // Very critical, if we're not generating the same
                        // index, all bets are off.
                        assert_eq!(group_idx, idx);
                    }

                    self.hash_table
                        .insert(hash, (hash, group_idx), |(hash, _group_idx)| *hash);

                    self.group_values.push(row.into_owned());
                    self.indexes_buffer.push(group_idx);
                }
            }
        }

        // And now we combine the states using the computed mappings.
        let other_states = std::mem::take(&mut other.agg_states);
        for (own_state, other_state) in self.agg_states.iter_mut().zip(other_states.into_iter()) {
            own_state
                .states
                .try_combine(other_state.states, &self.indexes_buffer)?;
        }

        Ok(())
    }

    pub fn into_drain(
        self,
        batch_size: usize,
        group_types: Vec<DataType>,
    ) -> AggregateHashTableDrain {
        AggregateHashTableDrain {
            group_types,
            batch_size,
            table: self,
            group_values_drain_buf: Vec::new(),
        }
    }
}

impl fmt::Debug for PartitionAggregateHashTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AggregateHashTable")
            .field("aggregate_states", &self.agg_states)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct AggregateHashTableDrain {
    /// Datatypes of the grouping columns. Used to construct the arrays
    /// representing the group by values.
    group_types: Vec<DataType>,

    /// Max size of batch to return.
    batch_size: usize,

    /// Inner table.
    table: PartitionAggregateHashTable,

    /// Reused buffer for draining rows representing the group values from the
    /// table.
    group_values_drain_buf: Vec<OwnedScalarRow>,
}

impl AggregateHashTableDrain {
    fn next_inner(&mut self) -> Result<Option<Batch>> {
        let result_cols = self
            .table
            .agg_states
            .iter_mut()
            .map(|agg_state| agg_state.states.drain_finalize_n(self.batch_size))
            .collect::<Result<Option<Vec<_>>>>()?;
        let result_cols: Vec<_> = match result_cols {
            Some(cols) => cols.into_iter().map(Arc::new).collect(),
            None => return Ok(None),
        };

        // Convert group values into arrays.
        let num_rows = result_cols.first().map(|col| col.len()).unwrap_or(0);
        let mut group_cols = Vec::with_capacity(self.group_types.len());

        // Drain out collected group rows into our local buffer equal to the
        // number of rows we're returning.
        self.group_values_drain_buf.clear();
        self.group_values_drain_buf
            .extend(self.table.group_values.drain(0..num_rows));

        for group_dt in self.group_types.iter() {
            // Since group values are in row format, we just pop the value for
            // each row in each iteration to get the column values.
            let iter = self
                .group_values_drain_buf
                .iter_mut()
                .map(|row| row.columns.remove(0)); // TODO: Could probably use something other than `remove(0)` here.

            let arr = Array::try_from_scalars(group_dt.clone(), iter)?;
            group_cols.push(Arc::new(arr));
        }

        let batch = Batch::try_new(result_cols.into_iter().chain(group_cols.into_iter()))?;

        Ok(Some(batch))
    }
}

impl Iterator for AggregateHashTableDrain {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_inner().transpose()
    }
}
