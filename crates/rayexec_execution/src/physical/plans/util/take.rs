/// Take values at the provided indexes.
pub fn take_indexes<T: Copy>(values: &[T], indexes: &[usize]) -> Vec<T> {
    indexes.iter().map(|idx| values[*idx]).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn take_indexes_u64() {
        #[derive(Debug)]
        struct TestCase {
            values: Vec<u64>,
            indexes: Vec<usize>,
            expected: Vec<u64>,
        }

        let test_cases = [
            TestCase {
                values: vec![],
                indexes: vec![],
                expected: vec![],
            },
            TestCase {
                values: vec![88, 89, 90, 91],
                indexes: vec![],
                expected: vec![],
            },
            TestCase {
                values: vec![88, 89, 90, 91],
                indexes: vec![0, 3],
                expected: vec![88, 91],
            },
        ];

        for test_case in test_cases {
            let taken = take_indexes(&test_case.values, &test_case.indexes);
            assert_eq!(test_case.expected, taken, "test case: {test_case:?}");
        }
    }
}
