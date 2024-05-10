//! Vectorized aggregate executors.
use rayexec_error::Result;
use std::fmt::Debug;

use crate::array::{ArrayAccessor, ArrayBuilder};

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
pub struct UnaryUpdater;

impl UnaryUpdater {
    pub fn update<A, T, I, S, O>(inputs: A, mapping: &[usize], states: &mut [S]) -> Result<()>
    where
        A: ArrayAccessor<T, ValueIter = I>,
        I: Iterator<Item = T>,
        S: AggregateState<T, O>,
    {
        // TODO: Figure out null handling. Some aggs want it, some don't.

        for (input, &mapping) in inputs.values_iter().zip(mapping.iter()) {
            let state = &mut states[mapping];
            state.update(input)?;
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct CovarSampFloat64 {
    count: usize,
    meanx: f64,
    meany: f64,
    co_moment: f64,
}

impl AggregateState<(f64, f64), f64> for CovarSampFloat64 {
    fn merge(&mut self, other: Self) -> Result<()> {
        let count = self.count + other.count;
        let meanx =
            (other.count as f64 * other.meanx + self.count as f64 * self.meanx) / count as f64;
        let meany =
            (other.count as f64 * other.meany + self.count as f64 * self.meany) / count as f64;

        let deltax = self.meanx - other.meanx;
        let deltay = self.meany - other.meany;

        self.co_moment = other.co_moment
            + self.co_moment
            + deltax * deltay * other.count as f64 * self.count as f64 / count as f64;
        self.meanx = meanx;
        self.meany = meany;
        self.count = count;

        Ok(())
    }

    fn update(&mut self, input: (f64, f64)) -> Result<()> {
        let x = input.1;
        let y = input.0;

        let n = self.count as f64;
        self.count += 1;

        let dx = x - self.meanx;
        let meanx = self.meanx + dx / n;

        let dy = y - self.meany;
        let meany = self.meany + dy / n;

        let co_moment = self.co_moment + dx * (y - meany);

        self.meanx = meanx;
        self.meany = meany;
        self.co_moment = co_moment;

        Ok(())
    }

    fn finalize(self) -> Result<f64> {
        Ok(self.co_moment / (self.count - 1) as f64)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BinaryUpdater;

impl BinaryUpdater {
    pub fn update<A1, T1, I1, A2, T2, I2, S, O>(
        first: A1,
        second: A2,
        mapping: &[usize],
        states: &mut [S],
    ) -> Result<()>
    where
        A1: ArrayAccessor<T1, ValueIter = I1>,
        I1: Iterator<Item = T1>,
        A2: ArrayAccessor<T2, ValueIter = I2>,
        I2: Iterator<Item = T2>,
        S: AggregateState<(T1, T2), O>,
    {
        // TODO: Figure out null handling. Some aggs want it, some don't.

        let first = first.values_iter();
        let second = second.values_iter();

        for (&mapping, (first, second)) in mapping.iter().zip(first.zip(second)) {
            let state = &mut states[mapping];
            state.update((first, second))?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StateCombiner;

impl StateCombiner {
    pub fn combine<S, T, O>(states: &mut [S], consume: Vec<S>) -> Result<()>
    where
        S: AggregateState<T, O>,
    {
        for (state, consume) in states.iter_mut().zip(consume.into_iter()) {
            state.merge(consume)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StateFinalizer;

impl StateFinalizer {
    pub fn finalize<S, T, O>(states: Vec<S>, builder: &mut impl ArrayBuilder<O>) -> Result<()>
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
