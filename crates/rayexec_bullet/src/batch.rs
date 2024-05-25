use crate::{array::Array, row::ScalarRow};
use rayexec_error::{RayexecError, Result};
use std::sync::Arc;

/// A batch of same-length arrays.
#[derive(Debug, PartialEq)]
pub struct Batch {
    /// Columns that make up this batch.
    cols: Vec<Arc<Array>>,

    /// Number of rows in this batch. Needed to allow for a batch that has no
    /// columns but a non-zero number of rows.
    num_rows: usize,
}

impl Batch {
    pub fn empty() -> Self {
        Batch {
            cols: Vec::new(),
            num_rows: 0,
        }
    }

    pub fn empty_with_num_rows(num_rows: usize) -> Self {
        Batch {
            cols: Vec::new(),
            num_rows,
        }
    }

    /// Create a new batch from some number of arrays.
    ///
    /// All arrays should be of the same length.
    pub fn try_new<A>(cols: impl IntoIterator<Item = A>) -> Result<Self>
    where
        A: Into<Arc<Array>>,
    {
        let cols: Vec<_> = cols.into_iter().map(|arr| arr.into()).collect();
        let len = match cols.first() {
            Some(arr) => arr.len(),
            None => return Ok(Self::empty()),
        };

        for col in &cols {
            if col.len() != len {
                return Err(RayexecError::new(format!(
                    "Expected column length to be {len}, got {}",
                    col.len()
                )));
            }
        }

        Ok(Batch {
            cols,
            num_rows: len,
        })
    }

    /// Project a batch using the provided indices.
    ///
    /// Panics if any index is out of bounds.
    pub fn project(&self, indices: &[usize]) -> Self {
        let cols = indices.iter().map(|idx| self.cols[*idx].clone()).collect();

        Batch {
            cols,
            num_rows: self.num_rows,
        }
    }

    /// Try to push a column to the end of the column list.
    ///
    /// Errors if the column does not have the same number of rows as in the
    /// batch.
    pub fn try_push_column(&mut self, col: impl Into<Arc<Array>>) -> Result<()> {
        let col = col.into();
        if col.len() != self.num_rows {
            return Err(RayexecError::new(format!(
                "Attempt to push a column with invalid number of rows, expected: {}, got: {}",
                self.num_rows,
                col.len()
            )));
        }

        self.cols.push(col);

        Ok(())
    }

    /// Try to pop the right-most column off the batch.
    pub fn try_pop_column(&mut self) -> Result<Arc<Array>> {
        self.cols.pop().ok_or_else(|| {
            RayexecError::new("Attempted to pop a column from a batch with no columns")
        })
    }

    /// Get the row at some index.
    pub fn row(&self, idx: usize) -> Option<ScalarRow> {
        if idx >= self.num_rows {
            return None;
        }

        // Non-zero number of rows, but no actual columns. Just return an empty
        // row.
        if self.cols.len() == 0 {
            return Some(ScalarRow::empty());
        }

        let row = self.cols.iter().map(|col| col.scalar(idx).unwrap());

        Some(ScalarRow::from_iter(row))
    }

    pub fn column(&self, idx: usize) -> Option<&Arc<Array>> {
        self.cols.get(idx)
    }

    pub fn columns(&self) -> &[Arc<Array>] {
        &self.cols
    }

    pub fn num_columns(&self) -> usize {
        self.cols.len()
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        array::{Int32Array, Utf8Array},
        scalar::ScalarValue,
    };

    use super::*;

    #[test]
    fn get_row_simple() {
        let batch = Batch::try_new([
            Array::Int32(Int32Array::from_iter([1, 2, 3])),
            Array::Utf8(Utf8Array::from_iter(["a", "b", "c"])),
        ])
        .unwrap();

        // Expected rows at index 0, 1, and 2
        let expected = [
            ScalarRow::from_iter([ScalarValue::Int32(1), ScalarValue::Utf8("a".into())]),
            ScalarRow::from_iter([ScalarValue::Int32(2), ScalarValue::Utf8("b".into())]),
            ScalarRow::from_iter([ScalarValue::Int32(3), ScalarValue::Utf8("c".into())]),
        ];

        for idx in 0..3 {
            let got = batch.row(idx).unwrap();
            assert_eq!(expected[idx], got);
        }
    }

    #[test]
    fn get_row_out_of_bounds() {
        let batch = Batch::try_new([
            Array::Int32(Int32Array::from_iter([1, 2, 3])),
            Array::Utf8(Utf8Array::from_iter(["a", "b", "c"])),
        ])
        .unwrap();

        let got = batch.row(3);
        assert_eq!(None, got);
    }

    #[test]
    fn get_row_no_columns_non_zero_rows() {
        let batch = Batch::empty_with_num_rows(3);

        for idx in 0..3 {
            let got = batch.row(idx).unwrap();
            assert_eq!(ScalarRow::empty(), got);
        }
    }
}
