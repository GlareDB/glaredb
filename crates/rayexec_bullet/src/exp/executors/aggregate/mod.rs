pub mod unary;

use std::fmt::Debug;

use rayexec_error::Result;

use super::OutputBuffer;
use crate::exp::buffer::addressable::MutableAddressableStorage;

/// State for a single group's aggregate.
///
/// An example state for SUM would be a struct that takes a running sum from
/// values provided in `update`.
pub trait AggregateState<Input: ?Sized, Output: ?Sized>: Debug {
    /// Merge other state into this state.
    fn merge(&mut self, other: &mut Self) -> Result<()>;

    /// Update this state with some input.
    fn update(&mut self, input: &Input) -> Result<()>;

    /// Produce a single value from the state, along with a bool indicating if
    /// the value is valid.
    fn finalize<M>(&mut self, output: OutputBuffer<M>) -> Result<()>
    where
        M: MutableAddressableStorage<T = Output>;
}
