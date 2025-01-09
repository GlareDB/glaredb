use std::sync::Arc;

use rayexec_error::Result;

use super::hash_table::HashTable;
use crate::arrays::array::Array;
use crate::arrays::executor::scalar::HashExecutor;
use crate::arrays::selection::SelectionVector;
use crate::execution::operators::hash_aggregate::hash_table::GroupAddress;
use crate::functions::aggregate::states::{AggregateGroupStates, OpaqueStatesMut};
use crate::functions::aggregate::ChunkGroupAddressIter;

/// And implementation of GroupedStates that buffers inputs to an aggregate in a
/// hash table to ensure the aggregate is computed with distinct values.
// TODO: Move this to aggregates function module.
#[derive(Debug)]
pub struct DistinctGroupedStates {
    /// Distinct inputs per group.
    distinct_inputs: Vec<Option<HashTable>>,
    /// The underlying states.
    ///
    /// These won't be initialized until we've received all distinct input.
    states: Box<dyn AggregateGroupStates>,
    /// Reusable hash buffer.
    hash_buf: Vec<u64>,
}

impl DistinctGroupedStates {
    pub fn new(states: Box<dyn AggregateGroupStates>) -> Self {
        DistinctGroupedStates {
            distinct_inputs: Vec::new(),
            states,
            hash_buf: Vec::new(),
        }
    }
}

impl AggregateGroupStates for DistinctGroupedStates {
    fn opaque_states_mut(&mut self) -> OpaqueStatesMut<'_> {
        OpaqueStatesMut(&mut self.distinct_inputs)
    }

    fn new_states(&mut self, count: usize) {
        // Hash tables created with empty aggregates.
        self.distinct_inputs
            .extend((0..count).map(|_| Some(HashTable::new(16, Vec::new()))));
    }

    fn num_states(&self) -> usize {
        self.distinct_inputs.len()
    }

    fn update_states(&mut self, inputs: &[&Array], mapping: ChunkGroupAddressIter) -> Result<()> {
        // TODO: Would be cool not needing to do this.
        let mappings: Vec<_> = mapping.collect();

        // For each group we're tracking, select the rows from the input and
        // insert into the group specific hash table.
        for state_idx in 0..self.distinct_inputs.len() {
            let row_sel = Arc::new(SelectionVector::from_iter(mappings.iter().filter_map(
                |row_mapping| {
                    if row_mapping.to_state == state_idx {
                        Some(row_mapping.from_row)
                    } else {
                        None
                    }
                },
            )));

            let inputs: Vec<_> = inputs
                .iter()
                .map(|&arr| {
                    let mut arr = arr.clone();
                    arr.select_mut2(row_sel.clone());
                    arr
                })
                .collect();

            let len = match inputs.first() {
                Some(arr) => arr.logical_len(),
                None => return Ok(()),
            };

            self.hash_buf.clear();
            self.hash_buf.resize(len, 0);

            HashExecutor::hash_many(&inputs, &mut self.hash_buf)?;

            // Insert into hash map with empty inputs.
            self.distinct_inputs[state_idx]
                .as_mut()
                .expect("hash table to exist")
                .insert(&inputs, &self.hash_buf, &[])?;
        }

        Ok(())
    }

    fn combine(
        &mut self,
        consume: &mut Box<dyn AggregateGroupStates>,
        mapping: ChunkGroupAddressIter,
    ) -> Result<()> {
        let other_distinct_inputs = consume
            .opaque_states_mut()
            .downcast::<Vec<Option<HashTable>>>()?;

        for mapping in mapping {
            let target = self.distinct_inputs[mapping.to_state].as_mut().unwrap();
            let consume = other_distinct_inputs[mapping.from_row].as_mut().unwrap();
            target.merge(consume)?;
        }

        Ok(())
    }

    fn finalize(&mut self) -> Result<Array> {
        // And now we actually create the states we need.
        self.states.new_states(self.distinct_inputs.len());

        let mut addresses_buf = Vec::new();

        for (group_idx, hash_table) in self.distinct_inputs.iter_mut().enumerate() {
            // Drain the hash table and inserting them into the newly created
            // states.
            let drain = hash_table.take().unwrap().into_drain();

            for result in drain {
                let batch = result?;
                let len = batch.num_rows();
                // TODO: Prune group id column?
                let arrays = batch.into_arrays();

                // TODO: Bit jank, but works. We just assume we're working with
                // chunk 0 always.
                //
                // I would like to have `GroupStates` be able to accept any
                // iterator that produce row mappings, but can't really do that
                // with dynamic dispatch.
                addresses_buf.clear();
                addresses_buf.extend((0..len).map(|_| GroupAddress {
                    chunk_idx: 0,
                    row_idx: group_idx as u16,
                }));

                let chunk_iter = ChunkGroupAddressIter::new(0, &addresses_buf);

                let inputs: Vec<_> = arrays.iter().collect(); // TODO
                self.states.update_states(&inputs, chunk_iter)?;
            }
        }

        // Now we can actually drain the states.
        self.states.finalize()
    }
}
