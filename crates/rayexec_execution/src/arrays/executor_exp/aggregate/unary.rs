use iterutil::IntoExactSizeIterator;
use rayexec_error::Result;

use super::AggregateState;
use crate::arrays::array::exp::Array;
use crate::arrays::array::flat::FlatArrayView;
use crate::arrays::buffer::physical_type::{Addressable, PhysicalStorage};

#[derive(Debug, Clone, Copy)]
pub struct UnaryNonNullUpdater;

impl UnaryNonNullUpdater {
    pub fn update<S, State, Output>(
        array: &Array,
        selection: impl IntoExactSizeIterator<Item = usize>,
        mapping: impl IntoExactSizeIterator<Item = usize>,
        states: &mut [State],
    ) -> Result<()>
    where
        S: PhysicalStorage,
        Output: ?Sized,
        for<'a> State: AggregateState<&'a S::StorageType, Output>,
    {
        if array.is_dictionary() {
            let flat = array.flat_view()?;
            return Self::update_flat::<S, State, Output>(flat, selection, mapping, states);
        }

        // TODO: Length check.

        let input = S::get_addressable(array.data())?;
        let validity = array.validity();

        if validity.all_valid() {
            for (input_idx, state_idx) in selection.into_iter().zip(mapping.into_iter()) {
                let val = input.get(input_idx).unwrap();
                let state = &mut states[state_idx];
                state.update(val)?;
            }
        } else {
            for (input_idx, state_idx) in selection.into_iter().zip(mapping.into_iter()) {
                if !validity.is_valid(input_idx) {
                    continue;
                }

                let val = input.get(input_idx).unwrap();
                let state = &mut states[state_idx];
                state.update(val)?;
            }
        }

        Ok(())
    }

    pub fn update_flat<'a, S, State, Output>(
        array: FlatArrayView<'a>,
        selection: impl IntoExactSizeIterator<Item = usize>,
        mapping: impl IntoExactSizeIterator<Item = usize>,
        states: &mut [State],
    ) -> Result<()>
    where
        S: PhysicalStorage,
        Output: ?Sized,
        for<'b> State: AggregateState<&'b S::StorageType, Output>,
    {
        let input = S::get_addressable(array.array_buffer)?;
        let validity = &array.validity;

        if validity.all_valid() {
            for (input_idx, state_idx) in selection.into_iter().zip(mapping.into_iter()) {
                let selected_idx = array.selection.get(input_idx).unwrap();

                let val = input.get(selected_idx).unwrap();
                let state = &mut states[state_idx];
                state.update(val)?;
            }
        } else {
            for (input_idx, state_idx) in selection.into_iter().zip(mapping.into_iter()) {
                let selected_idx = array.selection.get(input_idx).unwrap();

                if !validity.is_valid(selected_idx) {
                    continue;
                }

                let val = input.get(selected_idx).unwrap();
                let state = &mut states[state_idx];
                state.update(val)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use iterutil::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::buffer::buffer_manager::NopBufferManager;
    use crate::arrays::buffer::physical_type::{AddressableMut, PhysicalI32, PhysicalUtf8};
    use crate::arrays::executor_exp::PutBuffer;

    #[derive(Debug, Default)]
    struct TestSumState {
        val: i32,
    }

    impl AggregateState<&i32, i32> for TestSumState {
        fn merge(&mut self, other: &mut Self) -> Result<()> {
            self.val += other.val;
            Ok(())
        }

        fn update(&mut self, &input: &i32) -> Result<()> {
            self.val += input;
            Ok(())
        }

        fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
        where
            M: AddressableMut<T = i32>,
        {
            output.put(&self.val);
            Ok(())
        }
    }

    #[test]
    fn unary_primitive_single_state() {
        let mut states = [TestSumState::default()];
        let array = Array::try_from_iter([1, 2, 3, 4, 5]).unwrap();

        UnaryNonNullUpdater::update::<PhysicalI32, _, _>(
            &array,
            [0, 1, 2, 4],
            [0, 0, 0, 0],
            &mut states,
        )
        .unwrap();

        assert_eq!(11, states[0].val);
    }

    #[test]
    fn unary_primitive_single_state_dictionary() {
        let mut states = [TestSumState::default()];
        let mut array = Array::try_from_iter([1, 2, 3, 4, 5]).unwrap();
        // '[1, 5, 5, 5, 5, 2, 2]'
        array
            .select(&NopBufferManager, [0, 4, 4, 4, 4, 1, 1])
            .unwrap();

        UnaryNonNullUpdater::update::<PhysicalI32, _, _>(
            &array,
            [0, 1, 2, 4], // Select from the resulting dictionary.
            [0, 0, 0, 0],
            &mut states,
        )
        .unwrap();

        assert_eq!(16, states[0].val);
    }

    #[test]
    fn unary_primitive_single_state_skip_null() {
        let mut states = [TestSumState::default()];
        let array = Array::try_from_iter([None, Some(2), Some(3), Some(4), Some(5)]).unwrap();

        UnaryNonNullUpdater::update::<PhysicalI32, _, _>(
            &array,
            [0, 1, 2, 4],
            [0, 0, 0, 0],
            &mut states,
        )
        .unwrap();

        assert_eq!(10, states[0].val);
    }

    #[test]
    fn unary_primitive_multiple_states() {
        let mut states = [TestSumState::default(), TestSumState::default()];
        let array = Array::try_from_iter([1, 2, 3, 4, 5]).unwrap();

        UnaryNonNullUpdater::update::<PhysicalI32, _, _>(
            &array,
            [0, 1, 2, 4, 0, 3, 3],
            [0, 0, 0, 0, 1, 1, 0],
            &mut states,
        )
        .unwrap();

        assert_eq!(15, states[0].val);
        assert_eq!(5, states[1].val);
    }

    #[derive(Debug, Default)]
    struct TestStringAgg {
        val: String,
    }

    impl AggregateState<&str, str> for TestStringAgg {
        fn merge(&mut self, other: &mut Self) -> Result<()> {
            self.val.push_str(&other.val);
            Ok(())
        }

        fn update(&mut self, input: &str) -> Result<()> {
            self.val.push_str(input);
            Ok(())
        }

        fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
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
        let mut states = [TestStringAgg::default()];
        let array = Array::try_from_iter(["aa", "bbb", "cccc"]).unwrap();

        UnaryNonNullUpdater::update::<PhysicalUtf8, _, _>(
            &array,
            [0, 1, 2],
            [0, 0, 0],
            &mut states,
        )
        .unwrap();

        assert_eq!("aabbbcccc", &states[0].val);
    }
}