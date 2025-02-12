//! Helpers for creating batches.

/// Helper macro for generating batches to use in tests.
macro_rules! generate_batch {
    ( $( $array_values:expr ),+ $(,)? ) => {{
        use stdutil::iter::TryFromExactSizeIterator;
        crate::arrays::batch::Batch::try_from_arrays([
            $(
                crate::arrays::array::Array::try_from_iter($array_values).unwrap()
            ),+
        ]).unwrap()
    }};
}
pub(crate) use generate_batch;

#[cfg(test)]
mod tests {
    use stdutil::iter::TryFromExactSizeIterator;

    use crate::arrays::array::Array;
    use crate::arrays::batch::Batch;
    use crate::testutil::arrays::assert_batches_eq;

    #[test]
    fn generate_batch_single_column() {
        let batch = generate_batch!(["a", "b", "c"]);
        let expected =
            Batch::try_from_arrays([Array::try_from_iter(["a", "b", "c"]).unwrap()]).unwrap();

        assert_batches_eq(&expected, &batch);
    }

    #[test]
    fn generate_batch_multiple_columns() {
        let batch = generate_batch!(["a", "b", "c"], [1, 2, 3], [16.0, 17.0, 18.0]);
        let expected = Batch::try_from_arrays([
            Array::try_from_iter(["a", "b", "c"]).unwrap(),
            Array::try_from_iter([1, 2, 3]).unwrap(),
            Array::try_from_iter([16.0, 17.0, 18.0]).unwrap(),
        ])
        .unwrap();

        assert_batches_eq(&expected, &batch);
    }
}
