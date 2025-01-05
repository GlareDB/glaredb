use rayexec_error::Result;
use stdutil::iter::IntoExactSizeIterator;

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
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::buffer::physical_type::{AddressableMut, PhysicalI32};
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
    fn binary_primitive_single_state() {
        let mut states = [TestAddSumAndProductState::default()];
        let array1 = Array::try_from_iter([1, 2, 3, 4, 5]).unwrap();
        let array2 = Array::try_from_iter([6, 7, 8, 9, 10]).unwrap();

        BinaryNonNullUpdater::update::<PhysicalI32, PhysicalI32, _, _>(
            &array1,
            &array2,
            [1, 3, 4],
            [0, 0, 0],
            &mut states,
        )
        .unwrap();

        assert_eq!(11, states[0].sum);
        assert_eq!(630, states[0].product);
    }
}
