use std::fmt::Debug;

use crate::{
    array::{validity::union_validities, ArrayAccessor, ValuesBuffer},
    bitmap::Bitmap,
};
use rayexec_error::{RayexecError, Result};

/// Execute an operation on three arrays.
#[derive(Debug, Clone, Copy)]
pub struct TernaryExecutor;

impl TernaryExecutor {
    pub fn execute<Array1, Type1, Iter1, Array2, Type2, Iter2, Array3, Type3, Iter3, Output>(
        first: Array1,
        second: Array2,
        third: Array3,
        mut operation: impl FnMut(Type1, Type2, Type3) -> Output,
        buffer: &mut impl ValuesBuffer<Output>,
    ) -> Result<Option<Bitmap>>
    where
        Output: Debug,
        Array1: ArrayAccessor<Type1, ValueIter = Iter1>,
        Array2: ArrayAccessor<Type2, ValueIter = Iter2>,
        Array3: ArrayAccessor<Type3, ValueIter = Iter3>,
        Iter1: Iterator<Item = Type1>,
        Iter2: Iterator<Item = Type2>,
        Iter3: Iterator<Item = Type3>,
    {
        if first.len() != second.len() || second.len() != third.len() {
            return Err(RayexecError::new(format!(
                "Differing lengths of arrays, got {}, {}, and {}",
                first.len(),
                second.len(),
                third.len(),
            )));
        }

        let validity = union_validities([first.validity(), second.validity(), third.validity()])?;

        match &validity {
            Some(validity) => {
                for ((first, (second, third)), valid) in first
                    .values_iter()
                    .zip(second.values_iter().zip(third.values_iter()))
                    .zip(validity.iter())
                {
                    if valid {
                        let out = operation(first, second, third);
                        buffer.push_value(out);
                    } else {
                        buffer.push_null();
                    }
                }
            }
            None => {
                for (first, (second, third)) in first
                    .values_iter()
                    .zip(second.values_iter().zip(third.values_iter()))
                {
                    let out = operation(first, second, third);
                    buffer.push_value(out);
                }
            }
        }

        Ok(validity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::{Int32Array, Utf8Array, VarlenArray, VarlenValuesBuffer};

    #[test]
    fn ternary_substr() {
        let first = Utf8Array::from_iter(["alphabet"]);
        let second = Int32Array::from_iter([3]);
        let third = Int32Array::from_iter([2]);

        let mut buffer = VarlenValuesBuffer::default();

        let op = |s: &str, from: i32, count: i32| {
            s.chars()
                .skip((from - 1) as usize) // To match postgres' 1-indexing
                .take(count as usize)
                .collect::<String>()
        };

        let validity = TernaryExecutor::execute(&first, &second, &third, op, &mut buffer).unwrap();

        let got = VarlenArray::new(buffer, validity);
        let expected = Utf8Array::from_iter(["ph"]);

        assert_eq!(expected, got);
    }
}
