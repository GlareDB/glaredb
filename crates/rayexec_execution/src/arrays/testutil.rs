//! Test utilities.
//!
//! Note this this isn't behind a `#[cfg(test)]` flag since this should be
//! usable outside of this crate.
//!
//! Should not be used outside of tests.

use crate::arrays::array::Array2;
use crate::arrays::batch::Batch2;

/// Asserts that two arrays are logically equal.
pub fn assert_arrays_eq(a: &Array2, b: &Array2) {
    assert_eq!(a.datatype(), b.datatype(), "data types differ");
    assert_eq!(a.logical_len(), b.logical_len(), "logical lengths differ");

    for row_idx in 0..a.logical_len() {
        let a_val = a.logical_value(row_idx).unwrap();
        let b_val = b.logical_value(row_idx).unwrap();

        assert_eq!(a_val, b_val);
    }
}

/// Asserts that two batches are logically equal.
pub fn assert_batches_eq(a: &Batch2, b: &Batch2) {
    assert_eq!(a.num_rows(), b.num_rows(), "num rows differ");
    assert_eq!(a.num_columns(), b.num_columns(), "num columns differ");

    for col_idx in 0..a.num_columns() {
        let a_col = a.column(col_idx).unwrap();
        let b_col = b.column(col_idx).unwrap();

        assert_arrays_eq(a_col, b_col);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::selection::SelectionVector;

    #[test]
    fn arrays_eq() {
        let a = Array2::from_iter([1, 2, 3]);
        let b = Array2::from_iter([1, 2, 3]);

        assert_arrays_eq(&a, &b);
    }

    #[test]
    fn arrays_eq_with_selection() {
        let a = Array2::from_iter([2, 2, 2]);
        let mut b = Array2::from_iter([2]);
        b.select_mut(SelectionVector::repeated(3, 0));

        assert_arrays_eq(&a, &b);
    }

    #[test]
    #[should_panic]
    fn arrays_not_eq() {
        let a = Array2::from_iter([1, 2, 3]);
        let b = Array2::from_iter(["a", "b", "c"]);

        assert_arrays_eq(&a, &b);
    }
}
