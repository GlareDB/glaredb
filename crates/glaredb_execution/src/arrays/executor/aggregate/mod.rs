//! Vectorized aggregate executors.

mod binary;
pub use binary::*;

mod unary;
use std::fmt::Debug;

use glaredb_error::Result;
pub use unary::*;

use super::PutBuffer;
use crate::arrays::array::physical_type::AddressableMut;

/// State for a single group's aggregate.
///
/// An example state for SUM would be a struct that takes a running sum from
/// values provided in `update`.
pub trait AggregateState<Input, Output: ?Sized>: Debug + Sync + Send {
    type BindState: Sync + Send;

    /// Merge other state into this state.
    fn merge(&mut self, state: &Self::BindState, other: &mut Self) -> Result<()>;

    /// Update this state with some input.
    fn update(&mut self, state: &Self::BindState, input: Input) -> Result<()>;

    /// Produce a single value from the state and write it to the buffer.
    fn finalize<M>(&mut self, state: &Self::BindState, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = Output>;
}
