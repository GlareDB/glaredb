use rayexec_error::Result;

use crate::arrays::array::Array2;
use crate::arrays::executor::physical_type::{PhysicalBool_2, PhysicalStorage2};
use crate::arrays::selection::{self, SelectionVector};
use crate::arrays::storage::AddressableStorage;

#[derive(Debug, Clone)]
pub struct SelectExecutor;

impl SelectExecutor {
    /// Writes row selections to `output_sel`.
    ///
    /// Errors if the provided array isn't a boolean array.
    pub fn select(bool_array: &Array2, output_sel: &mut SelectionVector) -> Result<()> {
        output_sel.clear();
        let selection = bool_array.selection_vector();
        let len = bool_array.logical_len();

        match bool_array.validity() {
            Some(validity) => {
                let values = PhysicalBool_2::get_storage(&bool_array.data)?;

                for idx in 0..len {
                    let sel = selection::get(selection, idx);
                    if !validity.value(sel) {
                        continue;
                    }

                    let val = unsafe { values.get_unchecked(sel) };

                    if val {
                        output_sel.push_location(idx);
                    }
                }
            }
            None => {
                let values = PhysicalBool_2::get_storage(&bool_array.data)?;

                for idx in 0..len {
                    let sel = selection::get(selection, idx);
                    let val = unsafe { values.get_unchecked(sel) };

                    if val {
                        output_sel.push_location(idx);
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_simple() {
        let arr = Array2::from_iter([false, true, true, false, true]);
        let mut selection = SelectionVector::with_capacity(5);

        SelectExecutor::select(&arr, &mut selection).unwrap();

        let expected = SelectionVector::from_iter([1, 2, 4]);
        assert_eq!(selection, expected)
    }

    #[test]
    fn select_with_nulls() {
        let arr = Array2::from_iter([Some(false), Some(true), None, Some(false), Some(true)]);
        let mut selection = SelectionVector::with_capacity(5);

        SelectExecutor::select(&arr, &mut selection).unwrap();

        let expected = SelectionVector::from_iter([1, 4]);
        assert_eq!(selection, expected)
    }

    #[test]
    fn select_with_selection() {
        let mut arr = Array2::from_iter([Some(false), Some(true), None, Some(false), Some(true)]);
        // => [NULL, false, true]
        arr.select_mut(SelectionVector::from_iter([2, 3, 4]));

        let mut selection = SelectionVector::with_capacity(3);
        SelectExecutor::select(&arr, &mut selection).unwrap();

        let expected = SelectionVector::from_iter([2]);
        assert_eq!(selection, expected)
    }

    #[test]
    fn select_wrong_type() {
        let arr = Array2::from_iter([1, 2, 3, 4, 5]);
        let mut selection = SelectionVector::with_capacity(5);

        SelectExecutor::select(&arr, &mut selection).unwrap_err();
    }
}