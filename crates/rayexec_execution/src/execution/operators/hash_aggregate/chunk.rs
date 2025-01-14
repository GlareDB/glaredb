use rayexec_error::Result;

use super::hash_table::GroupAddress;
use super::AggregateStates;
use crate::arrays::array::physical_type::PhysicalType;
use crate::arrays::array::selection::Selection;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;

/// Holds a chunk of value for the aggregate hash table.
#[derive(Debug)]
pub struct GroupChunk {
    /// Index of this chunk.
    pub chunk_idx: u16,
    /// All row hashes.
    pub hashes: Vec<u64>,
    /// Batch containing the group values.
    pub batch: Batch,
    /// Aggregate states we're keeping track of.
    pub aggregate_states: Vec<AggregateStates>,
}

impl GroupChunk {
    pub fn num_groups(&self) -> usize {
        self.batch.num_rows()
    }

    pub fn can_append(
        &self,
        new_groups: usize,
        group_vals: impl ExactSizeIterator<Item = PhysicalType>,
    ) -> bool {
        if self.batch.num_rows() + new_groups > self.batch.capacity {
            return false;
        }

        debug_assert_eq!(self.batch.arrays.len(), group_vals.len());

        // Make sure we can actually concat. This is important when we have null
        // masks in the case of grouping sets.
        for (arr_idx, input_phys_type) in group_vals.enumerate() {
            if self.batch.arrays[arr_idx].physical_type() != input_phys_type {
                return false;
            }
        }

        true
    }

    /// Appends group values to this chunk, instantating all necessary aggregate
    /// states.
    ///
    /// Only indices that are part of the selection will be copied into this
    /// chunk.
    pub fn append_group_values(
        &mut self,
        group_vals: &[Array],
        hashes: &[u64],
        selection: &[usize],
    ) -> Result<()> {
        debug_assert_eq!(self.batch.arrays.len(), group_vals.len());

        let new_groups = selection.len();
        let num_groups = self.num_groups();

        for (current, incoming) in self.batch.arrays.iter_mut().zip(group_vals.iter()) {
            let mapping = selection
                .iter()
                .copied()
                .enumerate()
                .map(|(to, from)| (from, to + num_groups));

            incoming.copy_rows(mapping, current)?;
        }

        // Select the hashes.
        self.hashes.extend(selection.iter().map(|&idx| hashes[idx]));

        for states in &mut self.aggregate_states {
            states.states.new_groups(new_groups);
        }

        self.batch.set_num_rows(num_groups + new_groups)?;

        Ok(())
    }

    /// Update all states in this chunk using rows in `inputs`.
    ///
    /// `addrs` contains a list of group addresses we'll be using to map input
    /// rows to the state index. If and address is for a different chunk, that
    /// row will be skipped.
    pub fn update_states(&mut self, inputs: &[Array], addrs: &[GroupAddress]) -> Result<()> {
        // TODO: Cache these vecs.
        //
        // Produce selection/mapping vecs for updating the group states.
        let (selection, mapping): (Vec<_>, Vec<_>) = addrs
            .iter()
            .enumerate()
            .filter_map(|(row_idx, addr)| {
                if addr.chunk_idx != self.chunk_idx {
                    return None;
                }

                // Select this row, update the state pointed to by the addr.
                Some((row_idx, addr.row_idx as usize))
            })
            .unzip();

        for agg_states in &mut self.aggregate_states {
            let input_cols: Vec<_> = agg_states
                .col_selection
                .iter()
                .zip(inputs.iter())
                .filter_map(|(selected, arr)| if selected { Some(arr) } else { None })
                .collect();

            agg_states.states.update_group_states(
                &input_cols,
                Selection::slice(&selection),
                &mapping,
            )?;
        }

        Ok(())
    }

    /// Merges other into self according to `addrs`.
    ///
    /// Only addresses with this chunk's idx will be used, and the `row_idx` in
    /// the address corresponds to the aggregate row state in self to update
    /// from that addressess' position.
    pub fn combine_states(&mut self, other: &mut GroupChunk, addrs: &[GroupAddress]) -> Result<()> {
        // TODO: Cache these vecs.
        //
        // Produce selection/mapping vecs for updating the group states.
        let (selection, mapping): (Vec<_>, Vec<_>) = addrs
            .iter()
            .enumerate()
            .filter_map(|(row_idx, addr)| {
                if addr.chunk_idx != self.chunk_idx {
                    return None;
                }

                // Select this row, update the state pointed to by the addr.
                Some((row_idx, addr.row_idx as usize))
            })
            .unzip();

        for agg_idx in 0..self.aggregate_states.len() {
            let own_state = &mut self.aggregate_states[agg_idx];
            let other_state = &mut other.aggregate_states[agg_idx];

            own_state.states.combine(
                &mut other_state.states,
                Selection::slice(&selection),
                &mapping,
            )?;
        }

        Ok(())
    }
}
