use std::sync::Arc;

use rayexec_error::{RayexecError, Result};

use crate::array::ArrayOld;
use crate::executor::scalar::concat_with_exact_total_len;
use crate::row::ScalarRow;
use crate::selection::SelectionVector;

/// A batch of same-length arrays.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchOld {
    /// Columns that make up this batch.
    cols: Vec<ArrayOld>,

    /// Number of rows in this batch. Needed to allow for a batch that has no
    /// columns but a non-zero number of rows.
    num_rows: usize,
}

impl BatchOld {
    pub const fn empty() -> Self {
        BatchOld {
            cols: Vec::new(),
            num_rows: 0,
        }
    }

    pub fn empty_with_num_rows(num_rows: usize) -> Self {
        BatchOld {
            cols: Vec::new(),
            num_rows,
        }
    }

    /// Concat multiple batches into one.
    ///
    /// Batches are requried to have the same logical schemas.
    pub fn concat(batches: &[BatchOld]) -> Result<Self> {
        let num_cols = match batches.first() {
            Some(batch) => batch.num_columns(),
            None => return Err(RayexecError::new("Cannot concat zero batches")),
        };

        for batch in batches {
            if batch.num_columns() != num_cols {
                return Err(RayexecError::new(format!(
                    "Cannot concat batches with different number of columns, got {} and {}",
                    num_cols,
                    batch.num_columns()
                )));
            }
        }

        let num_rows: usize = batches.iter().map(|b| b.num_rows).sum();

        // Special case for zero col batches. The true number of rows wouldn't
        // be reflected if we just attempted to concat no array.
        if num_cols == 0 {
            return Ok(BatchOld::empty_with_num_rows(num_rows));
        }

        let mut output_cols = Vec::with_capacity(num_cols);

        let mut working_arrays = Vec::with_capacity(batches.len());
        for col_idx in 0..num_cols {
            batches
                .iter()
                .for_each(|b| working_arrays.push(b.column(col_idx).unwrap()));

            let out = concat_with_exact_total_len(&working_arrays, num_rows)?;
            output_cols.push(out);

            working_arrays.clear();
        }

        BatchOld::try_new(output_cols)
    }

    /// Create a new batch from some number of arrays.
    ///
    /// All arrays should have the same logical length.
    pub fn try_new(cols: impl IntoIterator<Item = ArrayOld>) -> Result<Self> {
        let cols: Vec<_> = cols.into_iter().collect();
        let len = match cols.first() {
            Some(arr) => arr.logical_len(),
            None => return Ok(Self::empty()),
        };

        for (idx, col) in cols.iter().enumerate() {
            if col.logical_len() != len {
                return Err(RayexecError::new(format!(
                    "Expected column length to be {len}, got {}. Column idx: {idx}",
                    col.logical_len()
                )));
            }
        }

        Ok(BatchOld {
            cols,
            num_rows: len,
        })
    }

    // TODO: Owned variant
    pub fn project(&self, indices: &[usize]) -> Self {
        let cols = indices.iter().map(|idx| self.cols[*idx].clone()).collect();

        BatchOld {
            cols,
            num_rows: self.num_rows,
        }
    }

    pub fn slice(&self, offset: usize, count: usize) -> Self {
        let cols = self.cols.iter().map(|c| c.slice(offset, count)).collect();
        BatchOld {
            cols,
            num_rows: count,
        }
    }

    /// Selects rows in the batch.
    ///
    /// This accepts an Arc selection as it'll be cloned for each array in the
    /// batch.
    pub fn select(&self, selection: Arc<SelectionVector>) -> BatchOld {
        let cols = self
            .cols
            .iter()
            .map(|c| {
                let mut col = c.clone();
                col.select_mut(selection.clone());
                col
            })
            .collect();

        BatchOld {
            cols,
            num_rows: selection.as_ref().num_rows(),
        }
    }

    /// Get the row at some index.
    pub fn row(&self, idx: usize) -> Option<ScalarRow> {
        if idx >= self.num_rows {
            return None;
        }

        // Non-zero number of rows, but no actual columns. Just return an empty
        // row.
        if self.cols.is_empty() {
            return Some(ScalarRow::empty());
        }

        let row = self.cols.iter().map(|col| col.logical_value(idx).unwrap());

        Some(ScalarRow::from_iter(row))
    }

    pub fn column(&self, idx: usize) -> Option<&ArrayOld> {
        self.cols.get(idx)
    }

    pub fn columns(&self) -> &[ArrayOld] {
        &self.cols
    }

    pub fn columns_mut(&mut self) -> &mut [ArrayOld] {
        &mut self.cols
    }

    pub fn num_columns(&self) -> usize {
        self.cols.len()
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn into_arrays(self) -> Vec<ArrayOld> {
        self.cols
    }
}
