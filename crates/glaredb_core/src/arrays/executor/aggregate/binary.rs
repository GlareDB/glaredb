use glaredb_error::{DbError, Result};

use super::AggregateState;
use crate::arrays::array::Array;
use crate::arrays::array::execution_format::{ExecutionFormat, SelectionFormat};
use crate::arrays::array::physical_type::{Addressable, ScalarStorage};
use crate::arrays::array::validity::Validity;
use crate::util::iter::IntoExactSizeIterator;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BinaryNonNullUpdater;

impl BinaryNonNullUpdater {
    pub fn update<S1, S2, BindState, State, Output>(
        array1: &Array,
        array2: &Array,
        selection: impl IntoExactSizeIterator<Item = usize>,
        bind_state: &BindState,
        states: &mut [*mut State],
    ) -> Result<()>
    where
        S1: ScalarStorage,
        S2: ScalarStorage,
        Output: ?Sized,
        for<'a> State: AggregateState<
                (&'a S1::StorageType, &'a S2::StorageType),
                Output,
                BindState = BindState,
            >,
    {
        let selection = selection.into_exact_size_iter();
        if selection.len() != states.len() {
            return Err(DbError::new(
                "Invalid number of states for selection in binary agggregate executor",
            )
            .with_field("sel_len", selection.len())
            .with_field("states_len", states.len()));
        }

        let format1 = S1::downcast_execution_format(&array1.data)?;
        let format2 = S2::downcast_execution_format(&array2.data)?;

        match (format1, format2) {
            (ExecutionFormat::Flat(a1), ExecutionFormat::Flat(a2)) => {
                let input1 = S1::addressable(a1);
                let input2 = S2::addressable(a2);

                let validity1 = &array1.validity;
                let validity2 = &array2.validity;

                if validity1.all_valid() && validity2.all_valid() {
                    for (state_idx, input_idx) in selection.enumerate() {
                        let val1 = input1.get(input_idx).unwrap();
                        let val2 = input2.get(input_idx).unwrap();

                        let state = unsafe { &mut *states[state_idx] };

                        state.update(bind_state, (val1, val2))?;
                    }
                } else {
                    for (state_idx, input_idx) in selection.enumerate() {
                        if !validity1.is_valid(input_idx) || !validity2.is_valid(input_idx) {
                            continue;
                        }

                        let val1 = input1.get(input_idx).unwrap();
                        let val2 = input2.get(input_idx).unwrap();

                        let state = unsafe { &mut *states[state_idx] };

                        state.update(bind_state, (val1, val2))?;
                    }
                }

                Ok(())
            }
            (a1, a2) => {
                let a1 = match a1 {
                    ExecutionFormat::Flat(a1) => SelectionFormat::flat(a1),
                    ExecutionFormat::Selection(a1) => a1,
                };
                let a2 = match a2 {
                    ExecutionFormat::Flat(a2) => SelectionFormat::flat(a2),
                    ExecutionFormat::Selection(a2) => a2,
                };

                Self::update_selection_format::<S1, S2, _, _, _>(
                    &array1.validity,
                    a1,
                    &array2.validity,
                    a2,
                    selection,
                    bind_state,
                    states,
                )
            }
        }
    }

    fn update_selection_format<S1, S2, BindState, State, Output>(
        validity1: &Validity,
        array1: SelectionFormat<'_, S1::ArrayBuffer>,
        validity2: &Validity,
        array2: SelectionFormat<'_, S2::ArrayBuffer>,
        selection: impl IntoExactSizeIterator<Item = usize>,
        bind_state: &BindState,
        states: &mut [*mut State],
    ) -> Result<()>
    where
        S1: ScalarStorage,
        S2: ScalarStorage,
        Output: ?Sized,
        for<'a> State: AggregateState<
                (&'a S1::StorageType, &'a S2::StorageType),
                Output,
                BindState = BindState,
            >,
    {
        let input1 = S1::addressable(array1.buffer);
        let input2 = S2::addressable(array2.buffer);

        if validity1.all_valid() && validity2.all_valid() {
            for (state_idx, input_idx) in selection.into_exact_size_iter().enumerate() {
                let sel1 = array1.selection.get(input_idx).unwrap();
                let val1 = input1.get(sel1).unwrap();
                let sel2 = array2.selection.get(input_idx).unwrap();
                let val2 = input2.get(sel2).unwrap();

                let state = unsafe { &mut *states[state_idx] };

                state.update(bind_state, (val1, val2))?;
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

                state.update(bind_state, (val1, val2))?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::array::physical_type::{AddressableMut, PhysicalI32};
    use crate::arrays::executor::PutBuffer;
    use crate::buffer::buffer_manager::DefaultBufferManager;
    use crate::util::iter::TryFromExactSizeIterator;

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
        type BindState = ();

        fn merge(&mut self, _state: &(), other: &mut Self) -> Result<()> {
            self.sum += other.sum;
            self.product *= other.product;
            Ok(())
        }

        fn update(&mut self, _state: &(), (&i1, &i2): (&i32, &i32)) -> Result<()> {
            self.sum += i1;
            self.product *= i2;
            Ok(())
        }

        fn finalize<M>(&mut self, _state: &(), output: PutBuffer<M>) -> Result<()>
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

        BinaryNonNullUpdater::update::<PhysicalI32, PhysicalI32, _, _, _>(
            &array1,
            &array2,
            [1, 3, 4],
            &(),
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
        array1
            .select(&DefaultBufferManager, [3, 4, 1, 2, 0])
            .unwrap();
        let array2 = Array::try_from_iter([6, 7, 8, 9, 10]).unwrap();

        BinaryNonNullUpdater::update::<PhysicalI32, PhysicalI32, _, _, _>(
            &array1,
            &array2,
            [1, 3, 4],
            &(),
            &mut states,
        )
        .unwrap();

        assert_eq!(9, state.sum);
        assert_eq!(630, state.product);
    }
}
