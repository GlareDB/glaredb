//! Helpers for creating batches.

/// Helper for generating an array.
///
/// Essentially just calls `try_from_iter`.
#[macro_export]
macro_rules! generate_array {
    ($array_values:expr) => {{
        use $crate::util::iter::TryFromExactSizeIterator;
        $crate::arrays::array::Array::try_from_iter($array_values).unwrap()
    }};
}

/// Helper macro for generating batches to use in tests.
// TODO: Remove `macro_export`. Only done to make this visible outside of the
// crate. We should probably just make a separate `testutil` crate with all
// these helpers (and then I wouldn't really care if this macros gets exported
// at the root).
#[macro_export]
macro_rules! generate_batch {
    ( $( $array_values:expr ),+ $(,)? ) => {{
        use $crate::util::iter::TryFromExactSizeIterator;
        $crate::arrays::batch::Batch::from_arrays([
            $(
                $crate::arrays::array::Array::try_from_iter($array_values).unwrap()
            ),+
        ]).unwrap()
    }};
}
pub(crate) use generate_batch;

#[cfg(test)]
mod tests {
    use crate::arrays::array::Array;
    use crate::arrays::batch::Batch;
    use crate::testutil::arrays::{assert_arrays_eq, assert_batches_eq};
    use crate::util::iter::TryFromExactSizeIterator;

    #[test]
    fn generate_array_simple() {
        let arr = generate_array!([1, 2, 3]);
        let expected = Array::try_from_iter([1, 2, 3]).unwrap();
        assert_arrays_eq(&expected, &arr);
    }

    #[test]
    fn generate_batch_single_column() {
        let batch = generate_batch!(["a", "b", "c"]);
        let expected =
            Batch::from_arrays([Array::try_from_iter(["a", "b", "c"]).unwrap()]).unwrap();

        assert_batches_eq(&expected, &batch);
    }

    #[test]
    fn generate_batch_multiple_columns() {
        let batch = generate_batch!(["a", "b", "c"], [1, 2, 3], [16.0, 17.0, 18.0]);
        let expected = Batch::from_arrays([
            Array::try_from_iter(["a", "b", "c"]).unwrap(),
            Array::try_from_iter([1, 2, 3]).unwrap(),
            Array::try_from_iter([16.0, 17.0, 18.0]).unwrap(),
        ])
        .unwrap();

        assert_batches_eq(&expected, &batch);
    }
}
