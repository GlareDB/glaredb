use crate::{
    array::{validity::union_validities, ArrayAccessor, ValuesBuffer},
    bitmap::Bitmap,
    compute::util::IntoExtactSizeIterator,
};
use rayexec_error::{RayexecError, Result};

pub trait OutputSelection {
    /// Exact size of the selection.
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Sets the provided index as selected or not selected.
    fn set(&mut self, idx: usize, selected: bool);
}

/// Execute an operation on two arrays.
#[derive(Debug, Clone, Copy)]
pub struct BinaryExecutor;

impl BinaryExecutor {
    /// Executes a binary operator on two arrays.
    pub fn execute<Array1, Type1, Iter1, Array2, Type2, Iter2, Output>(
        left: Array1,
        right: Array2,
        mut operation: impl FnMut(Type1, Type2) -> Output,
        buffer: &mut impl ValuesBuffer<Output>,
    ) -> Result<Option<Bitmap>>
    where
        Array1: ArrayAccessor<Type1, ValueIter = Iter1>,
        Array2: ArrayAccessor<Type2, ValueIter = Iter2>,
        Iter1: Iterator<Item = Type1>,
        Iter2: Iterator<Item = Type2>,
    {
        if left.len() != right.len() {
            return Err(RayexecError::new(format!(
                "Differing lengths of arrays, got {} and {}",
                left.len(),
                right.len()
            )));
        }

        let validity = union_validities([left.validity(), right.validity()])?;

        match &validity {
            Some(validity) => {
                for ((left_val, right_val), valid) in left
                    .values_iter()
                    .zip(right.values_iter())
                    .zip(validity.iter())
                {
                    if valid {
                        let out = operation(left_val, right_val);
                        buffer.push_value(out);
                    } else {
                        buffer.push_null();
                    }
                }
            }
            None => {
                for (left_val, right_val) in left.values_iter().zip(right.values_iter()) {
                    let out = operation(left_val, right_val);
                    buffer.push_value(out);
                }
            }
        }

        Ok(validity)
    }

    /// Executes a boolean operation on two array, writing the output to
    /// `output`.
    ///
    /// The provided `selection` is used to skip executing on certain rows.
    ///
    /// Invalid values are considered not selected, and so `set` will be called
    /// with false for that index.
    ///
    /// This provides a way of repeatedly pruning down rows of interest during
    /// conditional execution. The the CASE physical expression for what this is
    /// for.
    pub fn select<Array1, Type1, Iter1, Array2, Type2, Iter2>(
        left: Array1,
        right: Array2,
        selection: impl IntoExtactSizeIterator<Item = bool>,
        output: &mut impl OutputSelection,
        mut operation: impl FnMut(Type1, Type2) -> bool,
    ) -> Result<()>
    where
        Array1: ArrayAccessor<Type1, ValueIter = Iter1>,
        Array2: ArrayAccessor<Type2, ValueIter = Iter2>,
        Iter1: Iterator<Item = Type1>,
        Iter2: Iterator<Item = Type2>,
    {
        if left.len() != right.len() {
            return Err(RayexecError::new(format!(
                "Differing lengths of arrays, got {} and {}",
                left.len(),
                right.len()
            )));
        }

        let selection = selection.into_iter();

        if selection.len() != right.len() {
            return Err(RayexecError::new(format!(
                "Selection has incorrecnt length, got {}, want{}",
                selection.len(),
                right.len()
            )));
        }

        if output.len() != right.len() {
            return Err(RayexecError::new(format!(
                "Output has incorrecnt length, got {}, want{}",
                output.len(),
                right.len()
            )));
        }

        // TODO: Don't need to allocate this.
        let validity = union_validities([left.validity(), right.validity()])?;

        match &validity {
            Some(validity) => {
                for (row_idx, (((left_val, right_val), valid), selected)) in left
                    .values_iter()
                    .zip(right.values_iter())
                    .zip(validity.iter())
                    .zip(selection)
                    .enumerate()
                {
                    if !selected {
                        // Skip
                        continue;
                    }

                    if valid {
                        // Compute, write to output.
                        let out = operation(left_val, right_val);
                        output.set(row_idx, out);
                    } else {
                        // Not valid, write false
                        output.set(row_idx, false);
                    }
                }
            }
            None => {
                for (row_idx, ((left_val, right_val), selected)) in left
                    .values_iter()
                    .zip(right.values_iter())
                    .zip(selection)
                    .enumerate()
                {
                    if !selected {
                        // Skip
                        continue;
                    }

                    // Compute, write to output.
                    let out = operation(left_val, right_val);
                    output.set(row_idx, out);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{
        Int32Array, Int64Array, PrimitiveArray, Utf8Array, VarlenArray, VarlenValuesBuffer,
    };

    use super::*;

    #[test]
    fn binary_simple_add() {
        // Simple binary operation with differing input types.

        let left = Int32Array::from_iter([1, 2, 3]);
        let right = Int64Array::from_iter([4, 5, 6]);

        let mut buffer = Vec::with_capacity(3);

        let op = |a, b| (a as i64) + b;

        let validity = BinaryExecutor::execute(&left, &right, op, &mut buffer).unwrap();

        let got = PrimitiveArray::new(buffer, validity);
        let expected = Int64Array::from_iter([5, 7, 9]);

        assert_eq!(expected, got);
    }

    #[test]
    fn binary_string_repeat() {
        let left = Int32Array::from_iter([1, 2, 3]);
        let right = Utf8Array::from_iter(["hello", "world", "goodbye!"]);

        let mut buffer = VarlenValuesBuffer::default();

        let op = |a: i32, b: &str| b.repeat(a as usize);

        let validity = BinaryExecutor::execute(&left, &right, op, &mut buffer).unwrap();

        let got = VarlenArray::new(buffer, validity);
        let expected = Utf8Array::from_iter(["hello", "worldworld", "goodbye!goodbye!goodbye!"]);

        assert_eq!(expected, got);
    }
}
