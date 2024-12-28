use iterutil::IntoExactSizeIterator;
use rayexec_error::Result;

use super::AggregateState;
use crate::arrays::array::exp::Array;
use crate::arrays::buffer::physical_type::{Addressable, PhysicalStorage};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BinaryNonNullUpdater;

impl BinaryNonNullUpdater {
    pub fn update<S1, S2, State, Output>(
        array1: &Array,
        array2: &Array,
        selection: impl IntoExactSizeIterator<Item = usize>,
        mapping: impl IntoExactSizeIterator<Item = usize>,
        states: &mut [State],
    ) -> Result<()>
    where
        S1: PhysicalStorage,
        S2: PhysicalStorage,
        Output: ?Sized,
        for<'a> State: AggregateState<(&'a S1::StorageType, &'a S2::StorageType), Output>,
    {
        // TODO: Dictionary

        // TODO: Length check.

        let input1 = S1::get_addressable(array1.data())?;
        let input2 = S2::get_addressable(array2.data())?;

        let validity1 = array1.validity();
        let validity2 = array2.validity();

        if validity1.all_valid() && validity2.all_valid() {
            for (input_idx, state_idx) in selection.into_iter().zip(mapping.into_iter()) {
                let val1 = input1.get(input_idx).unwrap();
                let val2 = input2.get(input_idx).unwrap();

                let state = &mut states[state_idx];

                state.update((val1, val2))?;
            }
        } else {
            for (input_idx, state_idx) in selection.into_iter().zip(mapping.into_iter()) {
                if !validity1.is_valid(input_idx) || !validity2.is_valid(input_idx) {
                    continue;
                }

                let val1 = input1.get(input_idx).unwrap();
                let val2 = input2.get(input_idx).unwrap();

                let state = &mut states[state_idx];

                state.update((val1, val2))?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::buffer::physical_type::AddressableMut;
    use crate::arrays::executor_exp::PutBuffer;

    // SUM(col) + PRODUCT(col)
    #[derive(Debug)]
    struct TestAddSumAndProductState {
        sum: i32,
        product: i32,
    }

    impl Default for TestAddSumAndProductState {
        fn default() -> Self {
            TestAddSumAndProductState { sum: 0, product: 1 }
        }
    }

    impl AggregateState<(&i32, &i32), i32> for TestAddSumAndProductState {
        fn merge(&mut self, other: &mut Self) -> Result<()> {
            self.sum += other.sum;
            self.product *= other.product;
            Ok(())
        }

        fn update(&mut self, (&i1, &i2): (&i32, &i32)) -> Result<()> {
            self.sum += i1;
            self.product *= i2;
            Ok(())
        }

        fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
        where
            M: AddressableMut<T = i32>,
        {
            output.put(&(self.sum + self.product));
            Ok(())
        }
    }

    #[test]
    fn test_name() {}
}
