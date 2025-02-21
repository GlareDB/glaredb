use rayexec_error::{RayexecError, Result};
use stdutil::iter::IntoExactSizeIterator;

use super::AggregateState;
use crate::arrays::array::flat::FlattenedArray;
use crate::arrays::array::physical_type::{Addressable, ScalarStorage};
use crate::arrays::array::Array;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BinaryNonNullUpdater;

impl BinaryNonNullUpdater {
    pub fn update<S1, S2, State, Output>(
        array1: &Array,
        array2: &Array,
        selection: impl IntoExactSizeIterator<Item = usize>,
        states: &mut [*mut State],
    ) -> Result<()>
    where
        S1: ScalarStorage,
        S2: ScalarStorage,
        Output: ?Sized,
        for<'a> State: AggregateState<(&'a S1::StorageType, &'a S2::StorageType), Output>,
    {
        let selection = selection.into_exact_size_iter();
        if selection.len() != states.len() {
            return Err(RayexecError::new(
                "Invalid number of states for selection in binary agggregate executor",
            )
            .with_field("sel_len", selection.len())
            .with_field("states_len", states.len()));
        }

        if array1.should_flatten_for_execution() || array2.should_flatten_for_execution() {
            let flat1 = array1.flatten()?;
            let flat2 = array2.flatten()?;
            return Self::update_flat::<S1, S2, State, Output>(flat1, flat2, selection, states);
        }

        let input1 = S1::get_addressable(&array1.data)?;
        let input2 = S2::get_addressable(&array2.data)?;

        let validity1 = &array1.validity;
        let validity2 = &array2.validity;

        if validity1.all_valid() && validity2.all_valid() {
            for (state_idx, input_idx) in selection.enumerate() {
                let val1 = input1.get(input_idx).unwrap();
                let val2 = input2.get(input_idx).unwrap();

                let state = unsafe { &mut *states[state_idx] };

                state.update((val1, val2))?;
            }
        } else {
            for (state_idx, input_idx) in selection.enumerate() {
                if !validity1.is_valid(input_idx) || !validity2.is_valid(input_idx) {
                    continue;
                }

                let val1 = input1.get(input_idx).unwrap();
                let val2 = input2.get(input_idx).unwrap();

                let state = unsafe { &mut *states[state_idx] };

                state.update((val1, val2))?;
            }
        }

        Ok(())
    }

    fn update_flat<S1, S2, State, Output>(
        array1: FlattenedArray<'_>,
        array2: FlattenedArray<'_>,
        selection: impl IntoExactSizeIterator<Item = usize>,
        states: &mut [*mut State],
    ) -> Result<()>
    where
        S1: ScalarStorage,
        S2: ScalarStorage,
        Output: ?Sized,
        for<'a> State: AggregateState<(&'a S1::StorageType, &'a S2::StorageType), Output>,
    {
        let input1 = S1::get_addressable(array1.array_buffer)?;
        let input2 = S2::get_addressable(array2.array_buffer)?;

        let validity1 = &array1.validity;
        let validity2 = &array2.validity;

        if validity1.all_valid() && validity2.all_valid() {
            for (state_idx, input_idx) in selection.into_exact_size_iter().enumerate() {
                let sel1 = array1.selection.get(input_idx).unwrap();
                let val1 = input1.get(sel1).unwrap();
                let sel2 = array2.selection.get(input_idx).unwrap();
                let val2 = input2.get(sel2).unwrap();

                let state = unsafe { &mut *states[state_idx] };

                state.update((val1, val2))?;
            }
        } else {
            for (state_idx, input_idx) in selection.into_exact_size_iter().enumerate() {
                if !validity1.is_valid(input_idx) || !validity2.is_valid(input_idx) {
                    continue;
                }

                let sel1 = array1.selection.get(input_idx).unwrap();
                let val1 = input1.get(sel1).unwrap();
                let sel2 = array2.selection.get(input_idx).unwrap();
                let val2 = input2.get(sel2).unwrap();

                let state = unsafe { &mut *states[state_idx] };

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
    use crate::arrays::array::physical_type::{AddressableMut, PhysicalI32};
    use crate::arrays::executor::PutBuffer;
    use crate::buffer::buffer_manager::NopBufferManager;

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
        let mut state = TestAddSumAndProductState::default();
        let state_ptr: *mut TestAddSumAndProductState = &mut state;
        let mut states = vec![state_ptr; 3];

        let array1 = Array::try_from_iter([1, 2, 3, 4, 5]).unwrap();
        let array2 = Array::try_from_iter([6, 7, 8, 9, 10]).unwrap();

        BinaryNonNullUpdater::update::<PhysicalI32, PhysicalI32, _, _>(
            &array1,
            &array2,
            [1, 3, 4],
            &mut states,
        )
        .unwrap();

        assert_eq!(11, state.sum);
        assert_eq!(630, state.product);
    }

    #[test]
    fn binary_primitive_single_state_dictionary() {
        let mut state = TestAddSumAndProductState::default();
        let state_ptr: *mut TestAddSumAndProductState = &mut state;
        let mut states = vec![state_ptr; 3];

        let mut array1 = Array::try_from_iter([1, 2, 3, 4, 5]).unwrap();
        // => [4, 5, 2, 3, 1]
        array1.select(&NopBufferManager, [3, 4, 1, 2, 0]).unwrap();
        let array2 = Array::try_from_iter([6, 7, 8, 9, 10]).unwrap();

        BinaryNonNullUpdater::update::<PhysicalI32, PhysicalI32, _, _>(
            &array1,
            &array2,
            [1, 3, 4],
            &mut states,
        )
        .unwrap();

        assert_eq!(9, state.sum);
        assert_eq!(630, state.product);
    }
}
