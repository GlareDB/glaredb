//! Vectorized aggregate executors.

mod binary;
pub use binary::*;

mod unary;
use std::fmt::Debug;

use rayexec_error::Result;
pub use unary::*;

use super::PutBuffer;
use crate::arrays::array::buffer_manager::BufferManager;
use crate::arrays::array::physical_type::AddressableMut;

/// State for a single group's aggregate.
///
/// An example state for SUM would be a struct that takes a running sum from
/// values provided in `update`.
pub trait AggregateState<Input, Output: ?Sized>: Debug {
    /// Merge other state into this state.
    fn merge(&mut self, other: &mut Self) -> Result<()>;

    /// Update this state with some input.
    fn update(&mut self, input: Input) -> Result<()>;

    /// Produce a single value from the state and write it to the buffer.
    fn finalize<M, B>(&mut self, output: PutBuffer<M, B>) -> Result<()>
    where
        M: AddressableMut<B, T = Output>,
        B: BufferManager;
}

#[derive(Debug, Clone, Copy)]
pub struct StateCombiner;

impl StateCombiner {
    /// Combine states, merging states from `consume` into `targets`.
    ///
    /// `mapping` provides (from, to) mappings between `consume` and `targets`.
    pub fn combine<State, Input, Output>(
        consume: &mut [State],
        mapping: impl IntoIterator<Item = (usize, usize)>,
        targets: &mut [State],
    ) -> Result<()>
    where
        State: AggregateState<Input, Output>,
        Output: ?Sized,
    {
        for (from, to) in mapping {
            let consume = &mut consume[from];
            let target = &mut targets[to];
            target.merge(consume)?;
        }

        Ok(())
    }
}
