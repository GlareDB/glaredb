pub mod sum;

use arrow_array::{Array, ArrayRef};
use rayexec_error::{RayexecError, Result};
use std::{any::Any, fmt::Debug};

pub trait AggregateFunction: Send + Sync + Debug {
    /// Name used to generate columns names in the output if an alias is not
    /// explicitly provided.
    fn name(&self) -> &str;
}

pub trait AccumulatorState: Send + Sync + Debug {
    /// Number of groups represented by this state.
    fn num_groups(&self) -> usize;

    /// Convert to a mutable any reference.
    ///
    /// This allows us to downcast to the concrete state when merging multiple
    /// states for the same accumulator together.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Downcast this to the concrete state to allow for merging states across
/// multiple instances of an accumulator.
pub fn downcast_state_mut<T: AccumulatorState + 'static>(
    state: &mut Box<dyn AccumulatorState>,
) -> Result<&mut T> {
    state
        .as_any_mut()
        .downcast_mut::<T>()
        .ok_or_else(|| RayexecError::new("failed to downcast to requested state"))
}

pub trait Accumulator: Sync + Send + Debug {
    /// Update the internal state for the group at the given index using `vals`.
    ///
    /// This may be called out of order, and th accumulator should initialized
    /// skipped groups to some uninitialized state.
    fn accumulate(&mut self, group_idx: usize, vals: &[&ArrayRef]) -> Result<()>;

    /// Take the internal state so that it can be merged with another instance
    /// of this accumulator.
    fn take_state(&mut self) -> Result<Box<dyn AccumulatorState>>;

    /// Update the internal state using the state from a different instances.
    ///
    /// `groups` provides the mapping from the external state to internal state.
    /// The group index at `groups[0]` means merge the the 0th group from
    /// `state` into the group at `groups[0]` in `self`.
    ///
    /// `groups` may be bigger than the number of groups that this state has
    /// seen, and so the internal state needs to be updated to accomdate that.
    fn update_from_state(
        &mut self,
        groups: &[usize],
        state: Box<dyn AccumulatorState>,
    ) -> Result<()>;

    /// Produce the final result of accumulation.
    fn finish(&mut self) -> Result<ArrayRef>;
}
