use crate::column::{BoolVec, ColumnError, NullableColumnVec};
use crate::datatype::{RelationSchema, Row};
use bitvec::vec::BitVec;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum BatchError {
    #[error("size mismatch between columns, got: {got}, expected: {expected}")]
    ColumnSizeMismatch { expected: usize, got: usize },
    #[error(transparent)]
    ColumnError(#[from] ColumnError),
    #[error("invalid selectivity length, got: {got}, expected: {expected}")]
    InvalidSelectivityLen { expected: usize, got: usize },
}

/// Representations of batches.
#[derive(Debug)]
pub enum BatchRepr {
    /// A basic batch.
    Batch(Batch),
    /// A batch with a row selectivity vector.
    Selectivity(SelectivityBatch),
}

impl BatchRepr {
    /// Get the underlying batch reference.
    pub fn get_batch(&self) -> &Batch {
        match self {
            BatchRepr::Batch(batch) => batch,
            BatchRepr::Selectivity(sel) => &sel.batch,
        }
    }
}

impl From<Batch> for BatchRepr {
    fn from(batch: Batch) -> Self {
        BatchRepr::Batch(batch)
    }
}

impl From<SelectivityBatch> for BatchRepr {
    fn from(batch: SelectivityBatch) -> Self {
        BatchRepr::Selectivity(batch)
    }
}

/// A batch of rows, represented as typed columns.
#[derive(Debug, Clone)]
pub struct Batch {
    pub columns: Vec<Arc<NullableColumnVec>>,
}

impl Batch {
    pub fn empty() -> Batch {
        Batch {
            columns: Vec::new(),
        }
    }

    pub fn new_from_schema(schema: &RelationSchema, rows_cap: usize) -> Batch {
        let mut columns = Vec::with_capacity(schema.columns.len());
        for typ in schema.columns.iter() {
            let vec = NullableColumnVec::with_capacity(rows_cap, &typ.datatype);
            columns.push(Arc::new(vec));
        }
        Batch { columns }
    }

    /// Create a new batch from the provided columns. Each column must be the
    /// same length.
    pub fn from_columns<I>(columns: I) -> Result<Batch, BatchError>
    where
        I: IntoIterator<Item = NullableColumnVec>,
    {
        let mut iter = columns.into_iter();
        let first = match iter.next() {
            Some(col) => col,
            None => {
                return Ok(Batch {
                    columns: Vec::new(),
                })
            }
        };

        let len = first.len();
        let mut columns = vec![Arc::new(first)];
        for col in iter {
            if col.len() != len {
                return Err(BatchError::ColumnSizeMismatch {
                    expected: len,
                    got: col.len(),
                });
            }
            columns.push(Arc::new(col));
        }

        Ok(Batch { columns })
    }

    /// Push a row onto the batch.
    ///
    /// Columns will be cloned if there are any outstanding references.
    pub fn push_row(&mut self, row: Row) -> Result<(), BatchError> {
        if row.arity() != self.arity() {
            return Err(BatchError::ColumnSizeMismatch {
                expected: self.arity(),
                got: row.arity(),
            });
        }

        let iter = row.0.into_iter().zip(self.columns.iter_mut());
        for (value, orig) in iter {
            (*Arc::make_mut(orig)).push_value(&value)?;
        }

        Ok(())
    }

    pub fn get_column(&self, idx: usize) -> Option<&Arc<NullableColumnVec>> {
        self.columns.get(idx)
    }

    pub fn num_rows(&self) -> usize {
        match self.columns.first() {
            Some(v) => v.len(),
            None => 0,
        }
    }

    pub fn arity(&self) -> usize {
        self.columns.len()
    }
}

/// A batch with a selectivity bit vector indicating "visible" rows.
#[derive(Debug)]
pub struct SelectivityBatch {
    pub batch: Batch,
    pub selectivity: BitVec,
}

impl SelectivityBatch {
    pub fn new(batch: Batch, selectivity: BitVec) -> Result<SelectivityBatch, BatchError> {
        if batch.num_rows() != selectivity.len() {
            return Err(BatchError::InvalidSelectivityLen {
                expected: batch.num_rows(),
                got: selectivity.len(),
            });
        }
        Ok(SelectivityBatch { batch, selectivity })
    }

    pub fn new_with_bool_vec(
        batch: Batch,
        selectivity: &BoolVec,
    ) -> Result<SelectivityBatch, BatchError> {
        let mut b = BitVec::with_capacity(selectivity.len());
        for val in selectivity.iter() {
            b.push(*val);
        }
        Self::new(batch, b)
    }

    pub fn num_rows(&self) -> usize {
        self.selectivity.count_ones()
    }
}
