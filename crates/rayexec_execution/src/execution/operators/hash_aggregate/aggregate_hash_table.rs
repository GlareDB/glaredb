use crate::functions::aggregate::GroupedStates;
use hashbrown::raw::RawTable;
use rayexec_bullet::{
    array::Array,
    batch::Batch,
    bitmap::Bitmap,
    datatype::DataType,
    executor::aggregate::RowToStateMapping,
    row::{OwnedScalarRow, ScalarRow},
    selection::SelectionVector,
};
use rayexec_error::{RayexecError, Result};
use std::fmt;

/// States for a single aggregation.
#[derive(Debug)]
pub struct AggregateStates {
    /// The states we're tracking for a single aggregate.
    ///
    /// Internally the state are stored in a vector, with the index of the
    /// vector corresponding to the index of the group in the table's
    /// `group_values` vector.
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct GroupValue<'a> {
    // TODO: This is likely a peformance bottleneck with storing group values in
    // rows.
    row: ScalarRow<'a>,
    /// Id for the group. Computed from the null mask.
    group_id: u64,
}

impl<'a> GroupValue<'a> {
    const fn empty() -> Self {
        GroupValue {
            row: ScalarRow::empty(),
            group_id: 0,
        }
    }
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

    group_values: Vec<GroupValue<'static>>,

    /// Hash table pointing to the group index.
    hash_table: RawTable<(u64, usize)>,

    // Reusable buffer for building up the row to state mappings.
    mappings_buffer: Vec<RowToStateMapping>,
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
            mappings_buffer: Vec::new(),
        })
    }

    pub fn insert_groups(
        &mut self,
        groups: &[&Array],
        hashes: &[u64],
        inputs: &[&Array],
        selection: &SelectionVector,
        group_id: u64,
    ) -> Result<()> {
        self.mappings_buffer.clear();
        self.mappings_buffer.reserve(selection.num_rows());

        // Get group indices, creating new states as needed for groups we've
        // never seen before.
        self.find_or_create_group_indices(groups, hashes, selection, group_id)?;

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
                .update_states(&input_cols, &self.mappings_buffer)?;
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
        selection: &SelectionVector,
        group_id: u64,
    ) -> Result<()> {
        for row_idx in selection.iter_locations() {
            let hash = hashes[row_idx];

            // TODO: This is probably a bit slower than we'd want.
            //
            // It's like that replacing this with something that compares
            // scalars directly to a arrays at an index would be faster.
            let value = GroupValue {
                row: ScalarRow::try_new_from_arrays(groups, row_idx)?,
                group_id,
            };

            // Look up the entry into the hash table.
            let ent = self.hash_table.get_mut(hash, |(_hash, group_idx)| {
                value == self.group_values[*group_idx]
            });

            match ent {
                Some((_, group_idx)) => {
                    // Group already exists.
                    self.mappings_buffer.push(RowToStateMapping {
                        from_row: row_idx,
                        to_state: *group_idx,
                    });
                }
                None => {
                    let group_idx = self.group_values.len();

                    // Need to create new states and insert them into the hash table.
                    for agg_state in self.agg_states.iter_mut() {
                        let idx = agg_state.states.new_group();
                        // Very critical, if we're not generating the same
                        // index, all bets are off.
                        assert_eq!(group_idx, idx);
                    }

                    self.hash_table
                        .insert(hash, (hash, group_idx), |(hash, _group_idx)| *hash);

                    self.group_values.push(GroupValue {
                        row: value.row.into_owned(),
                        group_id: value.group_id,
                    });

                    self.mappings_buffer.push(RowToStateMapping {
                        from_row: row_idx,
                        to_state: group_idx,
                    });
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

        // This buffer is used to build up a mapping of (other_group -> own_group) for merging.
        let mut state_mappings = vec![0; row_count];

        // Ensure the has table we're merging into has all the groups from
        // the other hash table.
        for (hash, other_group_idx) in other.hash_table.drain() {
            // TODO: Deduplicate with othe find and create method.

            let row = std::mem::replace(
                &mut other.group_values[other_group_idx],
                GroupValue::empty(),
            );

            let ent = self.hash_table.get_mut(hash, |(_hash, self_group_idx)| {
                row == self.group_values[*self_group_idx]
            });

            match ent {
                Some((_, self_group_idx)) => {
                    // 'self' already has the group from the other table.
                    //
                    // Map other group to this group for merge.
                    state_mappings[other_group_idx] = *self_group_idx;
                }
                None => {
                    // 'self' has never seend this group before. Add it to the map with
                    // an empty state.
                    let new_group_idx = self.group_values.len();

                    // Need to create new states and insert them into the hash table.
                    for agg_state in self.agg_states.iter_mut() {
                        let idx = agg_state.states.new_group();
                        // Very critical, if we're not generating the same
                        // index, all bets are off.
                        assert_eq!(new_group_idx, idx);
                    }

                    self.hash_table
                        .insert(hash, (hash, new_group_idx), |(hash, _group_idx)| *hash);

                    self.group_values.push(row);

                    // Map other group to the newly created group in this table.
                    state_mappings[other_group_idx] = new_group_idx
                }
            }
        }

        // And now we combine the states using the computed mappings.
        //
        // This will do the merge between the other states and own states using
        // the the mapping we just built up.
        let other_states = std::mem::take(&mut other.agg_states);
        for (own_state, other_state) in self.agg_states.iter_mut().zip(other_states.into_iter()) {
            own_state
                .states
                .try_combine(other_state.states, &state_mappings)?;
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
            .field("group_values", &self.group_values)
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
            .map(|agg_state| agg_state.states.drain_next(self.batch_size))
            .collect::<Result<Option<Vec<_>>>>()?;

        let result_cols = match result_cols {
            Some(cols) => cols,
            None => return Ok(None),
        };

        // Convert group values into arrays.
        //
        // If we have nothing for results, we still want to try to pull from
        // groups, so set to non-zero value.
        let num_rows = result_cols
            .first()
            .map(|col| col.logical_len())
            .unwrap_or(usize::min(self.table.group_values.len(), self.batch_size));

        // No results, and nothing left in groups.
        if num_rows == 0 {
            return Ok(None);
        }

        // Drain out collected group rows into our local buffer equal to the
        // number of rows we're returning.
        self.group_values_drain_buf.clear();
        self.group_values_drain_buf
            .extend(self.table.group_values.drain(0..num_rows).map(|v| v.row));

        let group_cols =
            Batch::try_from_rows(&self.group_values_drain_buf, &self.group_types)?.into_arrays();

        // Create batch with result cols first, then group cols after.
        let batch = Batch::try_new(result_cols.into_iter().chain(group_cols))?;

        Ok(Some(batch))
    }
}

impl Iterator for AggregateHashTableDrain {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_inner().transpose()
    }
}
