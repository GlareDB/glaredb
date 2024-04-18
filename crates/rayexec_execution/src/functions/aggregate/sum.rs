use arrow::datatypes::ArrowNativeTypeOp;
use arrow_array::{
    cast::AsArray,
    types::{ArrowPrimitiveType, Float64Type, Int64Type},
    ArrayRef, PrimitiveArray,
};
use rayexec_error::{RayexecError, Result};
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use crate::functions::aggregate::downcast_state_mut;

use super::{Accumulator, AccumulatorState, AggregateFunction};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sum;

impl AggregateFunction for Sum {
    fn name(&self) -> &str {
        "SUM"
    }
}

pub type SumAccI64 = SumAcc<Int64Type>;
pub type SumAccF64 = SumAcc<Float64Type>;

#[derive(Debug)]
pub struct SumAcc<O: ArrowPrimitiveType> {
    /// Sums per group.
    sums: Vec<O::Native>,
}

impl<O: ArrowPrimitiveType> SumAcc<O> {
    pub fn new() -> Self {
        SumAcc { sums: Vec::new() }
    }
}

impl<O> AccumulatorState for SumAcc<O>
where
    O: Debug + Sync + Send + ArrowPrimitiveType,
{
    fn num_groups(&self) -> usize {
        self.sums.len()
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<O> Accumulator for SumAcc<O>
where
    O: Debug + Sync + Send + ArrowPrimitiveType,
{
    fn accumulate(&mut self, group_idx: usize, vals: &[&ArrayRef]) -> Result<()> {
        if vals.len() != 1 {
            return Err(RayexecError::new("Sum expects only a single input"));
        }

        let vals: &PrimitiveArray<O> = vals[0]
            .as_primitive_opt()
            .ok_or_else(|| RayexecError::new("failed to downcast values"))?;

        let sum = arrow::compute::sum(vals).unwrap_or(O::Native::ZERO);

        if self.sums.len() <= group_idx {
            self.sums.resize(group_idx + 1, O::Native::ZERO);
        }

        self.sums[group_idx] = self.sums[group_idx].add_wrapping(sum);

        Ok(())
    }

    fn take_state(&mut self) -> Result<Box<dyn AccumulatorState>> {
        let replace = Self::new();
        let current = std::mem::replace(self, replace);
        let state = Box::new(current);
        Ok(state)
    }

    fn update_from_state(
        &mut self,
        groups: &[usize],
        mut state: Box<dyn AccumulatorState>,
    ) -> Result<()> {
        let state = downcast_state_mut::<Self>(&mut state)?;

        let largest = match groups.iter().max() {
            Some(max) => *max,
            None => return Ok(()), // Nothing to do, no groups to update.
        };

        self.sums.resize(largest + 1, O::Native::ZERO);

        for (&group_idx, &sum) in groups.iter().zip(state.sums.iter()) {
            self.sums[group_idx] = self.sums[group_idx].add_wrapping(sum);
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<ArrayRef> {
        let sums = std::mem::take(&mut self.sums);
        let arr = PrimitiveArray::<O>::new(sums.into(), None);
        Ok(Arc::new(arr))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Array, Int64Array};

    use super::*;

    #[test]
    fn single_group() {
        let mut acc = SumAccI64::new();

        let inputs1 = [&(Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef)];
        acc.accumulate(0, inputs1.as_slice()).unwrap();

        let inputs2 = [&(Arc::new(Int64Array::from(vec![4, 5, 6])) as ArrayRef)];
        acc.accumulate(0, inputs2.as_slice()).unwrap();

        let out = acc.finish().unwrap();
        let expected = Int64Array::from(vec![21]);
        assert_eq!(&expected, out.as_primitive());
    }

    #[test]
    fn merge_multiple() {
        // Accumulator 1 only sees inputs for group at index 0.
        let mut acc1 = SumAccI64::new();
        let inputs1 = [&(Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef)];
        acc1.accumulate(0, inputs1.as_slice()).unwrap();

        // Accumulator 2 sees inputs for groups 0 and 1.
        let mut acc2 = SumAccI64::new();
        let inputs2 = [&(Arc::new(Int64Array::from(vec![4, 5, 6])) as ArrayRef)];
        acc2.accumulate(0, inputs2.as_slice()).unwrap();
        let inputs3 = [&(Arc::new(Int64Array::from(vec![7, 8, 9])) as ArrayRef)];
        acc2.accumulate(1, inputs3.as_slice()).unwrap();

        // Merge them. The mapping is the same between them.
        acc1.update_from_state(&[0, 1], acc2.take_state().unwrap())
            .unwrap();

        // Acc1 should now produce the final results for groups 0 and 1.
        let out = acc1.finish().unwrap();
        let expected = Int64Array::from(vec![21, 24]);
        assert_eq!(&expected, out.as_primitive());
    }
}
