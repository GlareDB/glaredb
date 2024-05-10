//! Scalar executors for generic vectorized execution over different types of
//! arrays.
//!
//! Structs may be extended to include a buffer in the future to avoid
//! operations having to allows strings or vecs when operating on string and
//! binary arrays.
//!
//! Explicit generic typing is used for unary, binary, and ternary operations as
//! those are likely to be the most common, so have these operations be
//! monomorphized is probably a good thing.

use crate::array::{ArrayAccessor, ArrayBuilder};
use rayexec_error::{RayexecError, Result};

/// Execute an operation on a single array.
#[derive(Debug, Clone, Copy)]
pub struct UnaryExecutor;

impl UnaryExecutor {
    pub fn execute<A1, T1, I1, T2>(
        array: A1,
        mut operation: impl FnMut(T1) -> T2,
        builder: &mut impl ArrayBuilder<T2>,
    ) -> Result<()>
    where
        A1: ArrayAccessor<T1, ValueIter = I1>,
        I1: Iterator<Item = T1>,
    {
        // TODO: Union validity, skip over values as needed.

        for val in array.values_iter() {
            let out = operation(val);
            builder.push_value(out);
        }

        Ok(())
    }
}

/// Execute an operation on two arrays.
#[derive(Debug, Clone, Copy)]
pub struct BinaryExecutor;

impl BinaryExecutor {
    pub fn execute<A1, T1, I1, A2, T2, I2, T3>(
        left: A1,
        right: A2,
        mut operation: impl FnMut(T1, T2) -> T3,
        builder: &mut impl ArrayBuilder<T3>,
    ) -> Result<()>
    where
        A1: ArrayAccessor<T1, ValueIter = I1>,
        A2: ArrayAccessor<T2, ValueIter = I2>,
        I1: Iterator<Item = T1>,
        I2: Iterator<Item = T2>,
    {
        if left.len() != right.len() {
            return Err(RayexecError::new(format!(
                "Differing lengths of arrays, got {} and {}",
                left.len(),
                right.len()
            )));
        }

        // TODO: Union validity, skip over values as needed.

        for (left_val, right_val) in left.values_iter().zip(right.values_iter()) {
            let out = operation(left_val, right_val);
            builder.push_value(out);
        }

        Ok(())
    }
}

/// Execute an operation on three arrays.
#[derive(Debug, Clone, Copy)]
pub struct TernaryExecutor;

impl TernaryExecutor {
    pub fn execute<A1, T1, I1, A2, T2, I2, A3, T3, I3, T4>(
        first: A1,
        second: A2,
        third: A3,
        mut operation: impl FnMut(T1, T2, T3) -> T4,
        builder: &mut impl ArrayBuilder<T4>,
    ) -> Result<()>
    where
        A1: ArrayAccessor<T1, ValueIter = I1>,
        A2: ArrayAccessor<T2, ValueIter = I2>,
        A3: ArrayAccessor<T3, ValueIter = I3>,
        I1: Iterator<Item = T1>,
        I2: Iterator<Item = T2>,
        I3: Iterator<Item = T3>,
    {
        if first.len() != second.len() || second.len() != third.len() {
            return Err(RayexecError::new(format!(
                "Differing lengths of arrays, got {}, {}, and {}",
                first.len(),
                second.len(),
                third.len(),
            )));
        }

        // TODO: Union validity, skip over values as needed.

        for (first, (second, third)) in first
            .values_iter()
            .zip(second.values_iter().zip(third.values_iter()))
        {
            let out = operation(first, second, third);
            builder.push_value(out);
        }

        Ok(())
    }
}

/// Execute an operation on a uniform variadic number of arrays.
#[derive(Debug, Clone, Copy)]
pub struct UniformExecutor;

impl UniformExecutor {
    pub fn execute<A, T1, I1, T2>(
        arrays: &[A],
        mut operation: impl FnMut(&[T1]) -> T2,
        builder: &mut impl ArrayBuilder<T2>,
    ) -> Result<()>
    where
        A: ArrayAccessor<T1, ValueIter = I1>,
        I1: Iterator<Item = T1>,
    {
        let len = arrays[0].len();

        // TODO: Length check

        let mut values_iters: Vec<_> = arrays.iter().map(|arr| arr.values_iter()).collect();
        let _validities = arrays.iter().map(|arr| arr.validity());

        let mut row_vals = Vec::with_capacity(arrays.len());

        // TODO: Union validity, skip over values as needed.

        for _idx in 0..len {
            row_vals.clear();

            for iter in values_iters.iter_mut() {
                let val = iter.next().expect("value to exist");
                row_vals.push(val);
            }

            let out = operation(&row_vals);
            builder.push_value(out);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{
        Int32Array, Int64Array, PrimitiveArrayBuilder, Utf8Array, VarlenArrayBuilder,
    };

    use super::*;

    #[test]
    fn binary_simple_add() {
        // Simple binary operation with differing input types.

        let left = Int32Array::from_iter([1, 2, 3]);
        let right = Int64Array::from_iter([4, 5, 6]);

        let mut builder = PrimitiveArrayBuilder::with_capacity(3);

        let op = |a, b| (a as i64) + b;

        BinaryExecutor::execute(&left, &right, op, &mut builder).unwrap();

        let got = builder.into_typed_array();
        let expected = Int64Array::from_iter([5, 7, 9]);

        assert_eq!(expected, got);
    }

    #[test]
    fn binary_string_repeat() {
        let left = Int32Array::from_iter([1, 2, 3]);
        let right = Utf8Array::from_iter(["hello", "world", "goodbye!"]);

        let mut builder = VarlenArrayBuilder::<_, i32>::new();

        let op = |a: i32, b: &str| b.repeat(a as usize);

        BinaryExecutor::execute(&left, &right, op, &mut builder).unwrap();

        let got = builder.into_typed_array();
        let expected = Utf8Array::from_iter(["hello", "worldworld", "goodbye!goodbye!goodbye!"]);

        assert_eq!(expected, got);
    }

    #[test]
    fn ternary_substr() {
        let first = Utf8Array::from_iter(["alphabet"]);
        let second = Int32Array::from_iter([3]);
        let third = Int32Array::from_iter([2]);

        let mut builder = VarlenArrayBuilder::<_, i32>::new();

        let op = |s: &str, from: i32, count: i32| {
            s.chars()
                .skip((from - 1) as usize) // To match postgres' 1-indexing
                .take(count as usize)
                .collect::<String>()
        };

        TernaryExecutor::execute(&first, &second, &third, op, &mut builder).unwrap();

        let got = builder.into_typed_array();
        let expected = Utf8Array::from_iter(["ph"]);

        assert_eq!(expected, got);
    }

    #[test]
    fn uniform_string_concat_row_wise() {
        let first = Utf8Array::from_iter(["a", "b", "c"]);
        let second = Utf8Array::from_iter(["1", "2", "3"]);
        let third = Utf8Array::from_iter(["dog", "cat", "horse"]);

        let mut builder = VarlenArrayBuilder::<_, i32>::new();

        let op = |strings: &[&str]| strings.join("");

        UniformExecutor::execute(&[&first, &second, &third], op, &mut builder).unwrap();

        let got = builder.into_typed_array();
        let expected = Utf8Array::from_iter(["a1dog", "b2cat", "c3horse"]);

        assert_eq!(expected, got);
    }
}
