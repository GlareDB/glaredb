use glaredb_error::{DbError, Result};

use super::AggregateState;
use crate::arrays::array::Array;
use crate::arrays::array::flat::FlattenedArray;
use crate::arrays::array::physical_type::{Addressable, ScalarStorage};
use crate::util::iter::IntoExactSizeIterator;

#[derive(Debug, Clone, Copy)]
pub struct UnaryNonNullUpdater;

impl UnaryNonNullUpdater {
    pub fn update<S, State, BindState, Output>(
        array: &Array,
        selection: impl IntoExactSizeIterator<Item = usize>,
        bind_state: &BindState,
        states: &mut [*mut State],
    ) -> Result<()>
    where
        S: ScalarStorage,
        Output: ?Sized,
        for<'a> State: AggregateState<&'a S::StorageType, Output, BindState = BindState>,
    {
        let selection = selection.into_exact_size_iter();
        if selection.len() != states.len() {
            return Err(DbError::new(
                "Invalid number of states for selection in unary agggregate executor",
            )
            .with_field("sel_len", selection.len())
            .with_field("states_len", states.len()));
        }

        if array.should_flatten_for_execution() {
            let flat = array.flatten()?;
            return Self::update_flat::<S, State, BindState, Output>(
                flat, selection, bind_state, states,
            );
        }

        let input = S::get_addressable(&array.data)?;
        let validity = &array.validity;

        if validity.all_valid() {
            for (state_idx, input_idx) in selection.enumerate() {
                let val = input.get(input_idx).unwrap();
                let state = unsafe { &mut *states[state_idx] };
                state.update(bind_state, val)?;
            }
        } else {
            for (state_idx, input_idx) in selection.enumerate() {
                if !validity.is_valid(input_idx) {
                    continue;
                }

                let val = input.get(input_idx).unwrap();
                let state = unsafe { &mut *states[state_idx] };
                state.update(bind_state, val)?;
            }
        }

        Ok(())
    }

    fn update_flat<S, State, BindState, Output>(
        array: FlattenedArray<'_>,
        selection: impl IntoExactSizeIterator<Item = usize>,
        bind_state: &BindState,
        states: &mut [*mut State],
    ) -> Result<()>
    where
        S: ScalarStorage,
        Output: ?Sized,
        for<'b> State: AggregateState<&'b S::StorageType, Output, BindState = BindState>,
    {
        let input = S::get_addressable(array.array_buffer)?;
        let validity = &array.validity;

        if validity.all_valid() {
            for (state_idx, input_idx) in selection.into_exact_size_iter().enumerate() {
                let selected_idx = array.selection.get(input_idx).unwrap();

                let val = input.get(selected_idx).unwrap();
                let state = unsafe { &mut *states[state_idx] };
                state.update(bind_state, val)?;
            }
        } else {
            for (state_idx, input_idx) in selection.into_exact_size_iter().enumerate() {
                if !validity.is_valid(input_idx) {
                    continue;
                }

                let selected_idx = array.selection.get(input_idx).unwrap();
                let val = input.get(selected_idx).unwrap();
                let state = unsafe { &mut *states[state_idx] };
                state.update(bind_state, val)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::array::physical_type::{AddressableMut, PhysicalI32, PhysicalUtf8};
    use crate::arrays::executor::PutBuffer;
    use crate::buffer::buffer_manager::NopBufferManager;
    use crate::util::iter::TryFromExactSizeIterator;

    #[derive(Debug, Default)]
    struct TestSumState {
        val: i32,
    }

    impl AggregateState<&i32, i32> for TestSumState {
        type BindState = ();

        fn merge(&mut self, _state: &(), other: &mut Self) -> Result<()> {
            self.val += other.val;
            Ok(())
        }

        fn update(&mut self, _state: &(), &input: &i32) -> Result<()> {
            self.val += input;
            Ok(())
        }

        fn finalize<M>(&mut self, _state: &(), output: PutBuffer<M>) -> Result<()>
        where
            M: AddressableMut<T = i32>,
        {
            output.put(&self.val);
            Ok(())
        }
    }

    #[test]
    fn unary_primitive_single_state() {
        let mut state = TestSumState::default();
        let state_ptr: *mut TestSumState = &mut state;
        let mut states = vec![state_ptr; 4];

        let array = Array::try_from_iter([1, 2, 3, 4, 5]).unwrap();

        UnaryNonNullUpdater::update::<PhysicalI32, _, _, _>(&array, [0, 1, 2, 4], &(), &mut states)
            .unwrap();

        assert_eq!(11, state.val);
    }

    #[test]
    fn unary_primitive_single_state_dictionary() {
        let mut state = TestSumState::default();
        let state_ptr: *mut TestSumState = &mut state;
        let mut states = vec![state_ptr; 4];

        let mut array = Array::try_from_iter([1, 2, 3, 4, 5]).unwrap();
        // '[1, 5, 5, 5, 5, 2, 2]'
        array
            .select(&NopBufferManager, [0, 4, 4, 4, 4, 1, 1])
            .unwrap();

        UnaryNonNullUpdater::update::<PhysicalI32, _, _, _>(
            &array,
            [0, 1, 2, 4], // Select from the resulting dictionary.
            &(),
            &mut states,
        )
        .unwrap();

        assert_eq!(16, state.val);
    }

    #[test]
    fn unary_primitive_single_state_dictionary_invalid() {
        let mut state = TestSumState::default();
        let state_ptr: *mut TestSumState = &mut state;
        let mut states = vec![state_ptr; 4];

        let mut array = Array::try_from_iter([Some(1), Some(2), Some(3), Some(4), None]).unwrap();
        // => '[1, NULL, NULL, NULL, NULL, 2, 2]'
        array
            .select(&NopBufferManager, [0, 4, 4, 4, 4, 1, 1])
            .unwrap();

        UnaryNonNullUpdater::update::<PhysicalI32, _, _, _>(
            &array,
            [0, 1, 2, 4], // Select from the resulting dictionary.
            &(),
            &mut states,
        )
        .unwrap();

        assert_eq!(1, state.val);
    }

    #[test]
    fn unary_primitive_single_state_constant() {
        let mut state = TestSumState::default();
        let state_ptr: *mut TestSumState = &mut state;
        let mut states = vec![state_ptr; 4];

        let array = Array::new_constant(&NopBufferManager, &3.into(), 5).unwrap();

        UnaryNonNullUpdater::update::<PhysicalI32, _, _, _>(
            &array,
            [0, 1, 2, 4], // Select from the resulting dictionary.
            &(),
            &mut states,
        )
        .unwrap();

        assert_eq!(12, state.val);
    }

    #[test]
    fn unary_primitive_single_state_skip_null() {
        let mut state = TestSumState::default();
        let state_ptr: *mut TestSumState = &mut state;
        let mut states = vec![state_ptr; 4];

        let array = Array::try_from_iter([None, Some(2), Some(3), Some(4), Some(5)]).unwrap();

        UnaryNonNullUpdater::update::<PhysicalI32, _, _, _>(&array, [0, 1, 2, 4], &(), &mut states)
            .unwrap();

        assert_eq!(10, state.val);
    }

    #[test]
    fn unary_primitive_multiple_states() {
        let mut state1 = TestSumState::default();
        let mut state2 = TestSumState::default();
        let ptr1: *mut TestSumState = &mut state1;
        let ptr2: *mut TestSumState = &mut state2;

        let mut states = [ptr1, ptr1, ptr1, ptr1, ptr2, ptr2, ptr1];

        let array = Array::try_from_iter([1, 2, 3, 4, 5]).unwrap();

        UnaryNonNullUpdater::update::<PhysicalI32, _, _, _>(
            &array,
            [0, 1, 2, 4, 0, 3, 3],
            &(),
            &mut states,
        )
        .unwrap();

        assert_eq!(15, state1.val);
        assert_eq!(5, state2.val);
    }

    #[derive(Debug)]
    struct TestStringAggBindState {
        sep: String,
    }

    #[derive(Debug, Default)]
    struct TestStringAgg {
        val: String,
    }

    impl AggregateState<&str, str> for TestStringAgg {
        type BindState = TestStringAggBindState;

        fn merge(&mut self, state: &TestStringAggBindState, other: &mut Self) -> Result<()> {
            // Lazy, will prefix with the separator.
            self.val.push_str(&state.sep);
            self.val.push_str(&other.val);
            Ok(())
        }

        fn update(&mut self, state: &TestStringAggBindState, input: &str) -> Result<()> {
            // Lazy, will prefix with the separator.
            self.val.push_str(&state.sep);
            self.val.push_str(input);
            Ok(())
        }

        fn finalize<M>(
            &mut self,
            _state: &TestStringAggBindState,
            output: PutBuffer<M>,
        ) -> Result<()>
        where
            M: AddressableMut<T = str>,
        {
            output.put(&self.val);
            Ok(())
        }
    }

    #[test]
    fn unary_string_single_state() {
        // Test just checks to ensure working with varlen is sane.
        let mut state = TestStringAgg::default();
        let state_ptr: *mut TestStringAgg = &mut state;
        let mut states = vec![state_ptr; 3];

        let array = Array::try_from_iter(["aa", "bbb", "cccc"]).unwrap();

        UnaryNonNullUpdater::update::<PhysicalUtf8, _, _, _>(
            &array,
            [0, 1, 2],
            &TestStringAggBindState {
                sep: ",".to_string(),
            },
            &mut states,
        )
        .unwrap();

        assert_eq!(",aa,bbb,cccc", &state.val);
    }
}
