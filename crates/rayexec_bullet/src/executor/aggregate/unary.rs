use rayexec_error::Result;

use crate::{
    array::Array, executor::physical_type::PhysicalStorage, selection, storage::AddressableStorage,
};

use super::{AggregateState, RowToStateMapping};

/// Updates aggregate states for an aggregate that accepts one input.
#[derive(Debug, Clone, Copy)]
pub struct UnaryNonNullUpdater;

impl UnaryNonNullUpdater {
    pub fn update<'a, S, I, State, Output>(
        array: &'a Array,
        mapping: I,
        states: &mut [State],
    ) -> Result<()>
    where
        S: PhysicalStorage<'a>,
        I: IntoIterator<Item = RowToStateMapping>,
        State: AggregateState<<S::Storage as AddressableStorage>::T, Output>,
    {
        let selection = array.selection_vector();

        match &array.validity {
            Some(validity) => {
                let values = S::get_storage(&array.data)?;

                for mapping in mapping {
                    let sel = selection::get_unchecked(selection, mapping.from_row);
                    if !validity.value_unchecked(sel) {
                        // Null, continue.
                        continue;
                    }

                    let val = unsafe { values.get_unchecked(sel) };
                    let state = &mut states[mapping.to_state];

                    state.update(val)?;
                }
            }
            None => {
                let values = S::get_storage(&array.data)?;

                for mapping in mapping {
                    let sel = selection::get_unchecked(selection, mapping.from_row);
                    let val = unsafe { values.get_unchecked(sel) };
                    let state = &mut states[mapping.to_state];

                    state.update(val)?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::executor::physical_type::{PhysicalI32, PhysicalUtf8};

    use super::*;

    #[derive(Debug, Default)]
    struct TestSumState {
        val: i32,
    }

    impl AggregateState<i32, i32> for TestSumState {
        fn merge(&mut self, other: Self) -> Result<()> {
            self.val += other.val;
            Ok(())
        }

        fn update(&mut self, input: i32) -> Result<()> {
            self.val += input;
            Ok(())
        }

        fn finalize(self) -> Result<(i32, bool)> {
            Ok((self.val, true))
        }
    }

    #[test]
    fn unary_primitive_single_state() {
        let mut states = [TestSumState::default()];
        let array = Array::from_iter([1, 2, 3, 4, 5]);
        let mapping = [
            RowToStateMapping {
                from_row: 1,
                to_state: 0,
            },
            RowToStateMapping {
                from_row: 3,
                to_state: 0,
            },
            RowToStateMapping {
                from_row: 4,
                to_state: 0,
            },
        ];

        UnaryNonNullUpdater::update::<PhysicalI32, _, _, _>(&array, mapping, &mut states).unwrap();

        assert_eq!(11, states[0].val);
    }

    #[test]
    fn unary_primitive_single_state_skip_null() {
        let mut states = [TestSumState::default()];
        let array = Array::from_iter([Some(1), Some(2), Some(3), None, Some(5)]);
        let mapping = [
            RowToStateMapping {
                from_row: 1,
                to_state: 0,
            },
            RowToStateMapping {
                from_row: 3,
                to_state: 0,
            },
            RowToStateMapping {
                from_row: 4,
                to_state: 0,
            },
        ];

        UnaryNonNullUpdater::update::<PhysicalI32, _, _, _>(&array, mapping, &mut states).unwrap();

        assert_eq!(7, states[0].val);
    }

    #[test]
    fn unary_primitive_multiple_state() {
        let mut states = [TestSumState::default(), TestSumState::default()];
        let array = Array::from_iter([1, 2, 3, 4, 5]);
        let mapping = [
            RowToStateMapping {
                from_row: 1,
                to_state: 1,
            },
            RowToStateMapping {
                from_row: 0,
                to_state: 0,
            },
            RowToStateMapping {
                from_row: 3,
                to_state: 0,
            },
            RowToStateMapping {
                from_row: 4,
                to_state: 1,
            },
        ];

        UnaryNonNullUpdater::update::<PhysicalI32, _, _, _>(&array, mapping, &mut states).unwrap();

        assert_eq!(5, states[0].val);
        assert_eq!(7, states[1].val);
    }

    #[derive(Debug, Default)]
    struct TestStringAgg {
        buf: String,
    }

    impl AggregateState<&str, String> for TestStringAgg {
        fn merge(&mut self, other: Self) -> Result<()> {
            self.buf.push_str(&other.buf);
            Ok(())
        }

        fn update(&mut self, input: &str) -> Result<()> {
            self.buf.push_str(input);
            Ok(())
        }

        fn finalize(self) -> Result<(String, bool)> {
            Ok((self.buf, true))
        }
    }

    #[test]
    fn unary_str_single_state() {
        // Test just checks to ensure working with varlen is sane.
        let mut states = [TestStringAgg::default()];
        let array = Array::from_iter(["aa", "bbb", "cccc"]);
        let mapping = [
            RowToStateMapping {
                from_row: 0,
                to_state: 0,
            },
            RowToStateMapping {
                from_row: 1,
                to_state: 0,
            },
            RowToStateMapping {
                from_row: 2,
                to_state: 0,
            },
        ];

        UnaryNonNullUpdater::update::<PhysicalUtf8, _, _, _>(&array, mapping, &mut states).unwrap();

        assert_eq!("aabbbcccc", &states[0].buf);
    }
}
