use rayexec_error::{RayexecError, Result};

use super::{AggregateState, RowToStateMapping};
use crate::arrays::array::physical_type::PhysicalStorage;
use crate::arrays::array::Array;
use crate::arrays::executor::scalar::check_validity;
use crate::arrays::selection;
use crate::arrays::storage::AddressableStorage;

/// Updates aggregate states for an aggregate that accepts two inputs.
#[derive(Debug, Clone, Copy)]
pub struct BinaryNonNullUpdater;

impl BinaryNonNullUpdater {
    pub fn update<'a, S1, S2, I, State, Output>(
        array1: &'a Array,
        array2: &'a Array,
        mapping: I,
        states: &mut [State],
    ) -> Result<()>
    where
        S1: PhysicalStorage,
        S2: PhysicalStorage,
        I: IntoIterator<Item = RowToStateMapping>,
        State: AggregateState<(S1::Type<'a>, S2::Type<'a>), Output>,
    {
        if array1.logical_len() != array2.logical_len() {
            return Err(RayexecError::new(format!(
                "Cannot compute binary aggregate on arrays with different lengths, got {} and {}",
                array1.logical_len(),
                array2.logical_len(),
            )));
        }

        let selection1 = array1.selection_vector();
        let selection2 = array2.selection_vector();

        let validity1 = array1.validity();
        let validity2 = array2.validity();

        if validity1.is_some() || validity2.is_some() {
            let values1 = S1::get_storage(&array1.data2)?;
            let values2 = S2::get_storage(&array2.data2)?;

            for mapping in mapping {
                let sel1 = unsafe { selection::get_unchecked(selection1, mapping.from_row) };
                let sel2 = unsafe { selection::get_unchecked(selection2, mapping.from_row) };

                if check_validity(sel1, validity1) && check_validity(sel2, validity2) {
                    let val1 = unsafe { values1.get_unchecked(sel1) };
                    let val2 = unsafe { values2.get_unchecked(sel2) };

                    let state = &mut states[mapping.to_state];
                    state.update((val1, val2))?
                }
            }
        } else {
            let values1 = S1::get_storage(&array1.data2)?;
            let values2 = S2::get_storage(&array2.data2)?;

            for mapping in mapping {
                let sel1 = unsafe { selection::get_unchecked(selection1, mapping.from_row) };
                let sel2 = unsafe { selection::get_unchecked(selection2, mapping.from_row) };

                let val1 = unsafe { values1.get_unchecked(sel1) };
                let val2 = unsafe { values2.get_unchecked(sel2) };

                let state = &mut states[mapping.to_state];
                state.update((val1, val2))?
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::array::physical_type::PhysicalI32;

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

    impl AggregateState<(i32, i32), i32> for TestAddSumAndProductState {
        fn merge(&mut self, other: &mut Self) -> Result<()> {
            self.sum += other.sum;
            self.product *= other.product;
            Ok(())
        }

        fn update(&mut self, input: (i32, i32)) -> Result<()> {
            self.sum += input.0;
            self.product *= input.1;
            Ok(())
        }

        fn finalize(&mut self) -> Result<(i32, bool)> {
            Ok((self.sum + self.product, true))
        }
    }

    #[test]
    fn binary_primitive_single_state() {
        let mut states = [TestAddSumAndProductState::default()];
        let array1 = Array::from_iter([1, 2, 3, 4, 5]);
        let array2 = Array::from_iter([6, 7, 8, 9, 10]);

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

        BinaryNonNullUpdater::update::<PhysicalI32, PhysicalI32, _, _, _>(
            &array1,
            &array2,
            mapping,
            &mut states,
        )
        .unwrap();

        assert_eq!(11, states[0].sum);
        assert_eq!(630, states[0].product);
    }
}
