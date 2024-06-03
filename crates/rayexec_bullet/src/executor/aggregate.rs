//! Vectorized aggregate executors.
use rayexec_error::Result;
use std::fmt::Debug;

use crate::{
    array::{ArrayAccessor, ArrayBuilder},
    bitmap::Bitmap,
};

/// State for a single group's aggregate.
///
/// An example state for SUM would be a struct that takes a running sum from
/// values provided in `update`.
pub trait AggregateState<T, O>: Default + Debug {
    /// Merge other state into this state.
    fn merge(&mut self, other: Self) -> Result<()>;

    /// Update this state with some input.
    fn update(&mut self, input: T) -> Result<()>;

    /// Produce a single value from the state.
    fn finalize(self) -> Result<O>;
}

#[derive(Debug, Clone, Copy)]
pub struct UnaryNonNullUpdater;

impl UnaryNonNullUpdater {
    /// Updates a list of target states from some inputs.
    ///
    /// The row selection bitmap indicates which rows from the input to use for
    /// the update, and the mapping slice maps rows to target states.
    ///
    /// Values that are considered null (not valid) will not be passed to the
    /// state for udpates.
    pub fn update<Array, Type, Iter, State, Output>(
        row_selection: &Bitmap,
        inputs: Array,
        mapping: &[usize],
        target_states: &mut [State],
    ) -> Result<()>
    where
        Array: ArrayAccessor<Type, ValueIter = Iter>,
        Iter: Iterator<Item = Type>,
        State: AggregateState<Type, Output>,
    {
        debug_assert_eq!(
            row_selection.popcnt(),
            mapping.len(),
            "number of rows selected in input must equal length of mappings"
        );

        // TODO: Figure out null handling. Some aggs want it, some don't.

        match inputs.validity() {
            Some(validity) => {
                let mut mapping_idx = 0;
                for (selected, (input, valid)) in row_selection
                    .iter()
                    .zip(inputs.values_iter().zip(validity.iter()))
                {
                    if !selected || !valid {
                        continue;
                    }
                    let target = &mut target_states[mapping[mapping_idx]];
                    target.update(input)?;
                    mapping_idx += 1;
                }
            }
            None => {
                let mut mapping_idx = 0;
                for (selected, input) in row_selection.iter().zip(inputs.values_iter()) {
                    if !selected {
                        continue;
                    }
                    let target = &mut target_states[mapping[mapping_idx]];
                    target.update(input)?;
                    mapping_idx += 1;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BinaryUpdater;

impl BinaryUpdater {
    pub fn update<Array1, Type1, Iter1, Array2, Type2, Iter2, State, Output>(
        row_selection: &Bitmap,
        first: Array1,
        second: Array2,
        mapping: &[usize],
        target_states: &mut [State],
    ) -> Result<()>
    where
        Array1: ArrayAccessor<Type1, ValueIter = Iter1>,
        Iter1: Iterator<Item = Type1>,
        Array2: ArrayAccessor<Type2, ValueIter = Iter2>,
        Iter2: Iterator<Item = Type2>,
        State: AggregateState<(Type1, Type2), Output>,
    {
        debug_assert_eq!(
            row_selection.popcnt(),
            mapping.len(),
            "number of rows selected in input must equal length of mappings"
        );

        // TODO: Figure out null handling. Some aggs want it, some don't.
        // TOOD: Validity checks, see unary

        let first = first.values_iter();
        let second = second.values_iter();

        let mut mapping_idx = 0;
        for (selected, (first, second)) in row_selection.iter().zip(first.zip(second)) {
            if !selected {
                continue;
            }
            let target = &mut target_states[mapping[mapping_idx]];
            target.update((first, second))?;
            mapping_idx += 1;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StateCombiner;

impl StateCombiner {
    /// Combine states, merging states from `consume` into `targets`.
    ///
    /// `mapping` provides a mapping of consume states to the target index. The
    /// 'n'th state in `consume` corresponds to the 'n'th value `mapping`. With the value
    /// in mapping being the index of the target state.
    pub fn combine<State, Input, Output>(
        consume: Vec<State>,
        mapping: &[usize],
        targets: &mut [State],
    ) -> Result<()>
    where
        State: AggregateState<Input, Output>,
    {
        for (target_idx, consume_state) in mapping.iter().zip(consume.into_iter()) {
            let target = &mut targets[*target_idx];
            target.merge(consume_state)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StateFinalizer;

impl StateFinalizer {
    pub fn finalize<S, T, O>(
        states: impl IntoIterator<Item = S>,
        builder: &mut impl ArrayBuilder<O>,
    ) -> Result<()>
    where
        S: AggregateState<T, O>,
    {
        for state in states {
            let out = state.finalize()?;
            builder.push_value(out);
        }

        Ok(())
    }
}
