use rayexec_error::Result;

use super::AggregateState;
use crate::compute::util::IntoExactSizedIterator;
use crate::exp::array::Array;
use crate::exp::buffer::addressable::AddressableStorage;
use crate::exp::buffer::physical_type::PhysicalStorage;

#[derive(Debug, Clone, Copy)]
pub struct UnaryNonNullUpdater;

impl UnaryNonNullUpdater {
    pub fn update<S, State, Output>(
        array: &Array,
        selection: impl IntoExactSizedIterator<Item = usize>,
        mapping: impl IntoExactSizedIterator<Item = usize>,
        states: &mut [State],
    ) -> Result<()>
    where
        S: PhysicalStorage,
        Output: ?Sized,
        for<'a> State: AggregateState<&'a S::StorageType, Output>,
    {
        // TODO: Length check.

        let input = S::get_storage(array.buffer())?;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatype::DataType;
    use crate::exp::buffer::addressable::MutableAddressableStorage;
    use crate::exp::buffer::physical_type::{PhysicalI32, PhysicalUtf8};
    use crate::exp::buffer::{Int32Builder, StringViewBufferBuilder};
    use crate::exp::executors::OutputBuffer;
    use crate::exp::validity::Validity;

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

        fn finalize<M>(&mut self, output: OutputBuffer<M>) -> Result<()>
        where
            M: MutableAddressableStorage<T = i32>,
        {
            output.put(&self.val);
            Ok(())
        }
    }

    #[test]
    fn unary_primitive_single_state() {
        let mut states = [TestSumState::default()];
        let array = Array::new(
            DataType::Int32,
            Int32Builder::from_iter([1, 2, 3, 4, 5]).unwrap(),
        );

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
    fn unary_primitive_single_state_skip_null() {
        let mut states = [TestSumState::default()];
        let mut validity = Validity::new_all_valid(5);
        validity.set_invalid(0);
        let array = Array::new_with_validity(
            DataType::Int32,
            Int32Builder::from_iter([1, 2, 3, 4, 5]).unwrap(),
            validity,
        )
        .unwrap();

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
        let array = Array::new(
            DataType::Int32,
            Int32Builder::from_iter([1, 2, 3, 4, 5]).unwrap(),
        );

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

        fn finalize<M>(&mut self, output: OutputBuffer<M>) -> Result<()>
        where
            M: MutableAddressableStorage<T = str>,
        {
            output.put(&self.val);
            Ok(())
        }
    }

    #[test]
    fn unary_string_single_state() {
        // Test just checks to ensure working with varlen is sane.
        let mut states = [TestStringAgg::default()];
        let array = Array::new(
            DataType::Utf8,
            StringViewBufferBuilder::from_iter(["aa", "bbb", "cccc"]).unwrap(),
        );

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
