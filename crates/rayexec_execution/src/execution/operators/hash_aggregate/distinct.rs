use rayexec_error::Result;

use super::hash_table::HashTable;
use crate::arrays::array::selection::Selection;
use crate::arrays::array::Array;
use crate::functions::aggregate::states::{AggregateGroupStates, OpaqueStatesMut};

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
        unimplemented!()
    }

    fn new_groups(&mut self, count: usize) {
        unimplemented!()
    }

    fn num_states(&self) -> usize {
        unimplemented!()
    }

    fn update_group_states(
        &mut self,
        inputs: &[&Array],
        selection: Selection,
        mapping: &[usize],
    ) -> Result<()> {
        unimplemented!()
    }

    fn combine(
        &mut self,
        consume: &mut Box<dyn AggregateGroupStates>,
        selection: Selection,
        mapping: &[usize],
    ) -> Result<()> {
        unimplemented!()
    }

    fn drain(&mut self, output: &mut Array) -> Result<usize> {
        unimplemented!()
    }
}
