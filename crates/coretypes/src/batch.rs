use crate::datatype::{DataType, DataValue, RelationSchema, Row};
use crate::expr::EvaluatedExpr;
use crate::vec::{BoolVec, ColumnVec};
use anyhow::anyhow;
use bitvec::vec::BitVec;
use std::marker::PhantomData;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum BatchError {
    #[error("size mismatch between columns, got: {got}, expected: {expected}")]
    ColumnSizeMismatch { expected: usize, got: usize },
    #[error("invalid selectivity length, got: {got}, expected: {expected}")]
    InvalidSelectivityLen { expected: usize, got: usize },
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

/// Representations of batches.
#[derive(Debug)]
pub enum BatchRepr {
    /// A column-oriented batch of records.
    Batch(Batch),
    /// A column-oriented batch with a row selectivity vector.
    Selectivity(SelectivityBatch),
}

impl BatchRepr {
    pub fn empty() -> BatchRepr {
        Batch::empty().into()
    }

    /// Get the underlying batch reference.
    pub fn get_batch(&self) -> &Batch {
        match self {
            BatchRepr::Batch(batch) => batch,
            BatchRepr::Selectivity(sel) => &sel.batch,
        }
    }

    pub fn into_shrunk_batch(self) -> Batch {
        match self {
            BatchRepr::Batch(batch) => batch,
            BatchRepr::Selectivity(batch) => batch.shrink_to_selected(),
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
///
/// Acceptable for a larger number of records.
#[derive(Debug, Clone)]
pub struct Batch {
    pub columns: Vec<Arc<ColumnVec>>,
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
            let vec = ColumnVec::with_capacity_for_type(rows_cap, &typ.datatype);
            columns.push(Arc::new(vec));
        }
        Batch { columns }
    }

    /// Create a new batch from the provided columns. Each column must be the
    /// same length.
    pub fn from_columns<I>(columns: I) -> Result<Batch, BatchError>
    where
        I: IntoIterator<Item = ColumnVec>,
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

    pub fn from_expression_results<I>(results: I) -> Result<Batch, BatchError>
    where
        I: IntoIterator<Item = EvaluatedExpr>,
    {
        // TODO: Handle values from expression results better.

        let mut iter = results.into_iter();
        let first = match iter.next() {
            Some(col) => col.into_arc_vec(),
            None => {
                return Ok(Batch {
                    columns: Vec::new(),
                })
            }
        };

        let len = first.len();
        let mut columns = vec![first];
        for col in iter {
            let col = col.into_arc_vec();
            if col.len() != len {
                return Err(BatchError::ColumnSizeMismatch {
                    expected: len,
                    got: col.len(),
                });
            }
            columns.push(col);
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

    pub fn get_column(&self, idx: usize) -> Option<&Arc<ColumnVec>> {
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
        for val in selectivity.iter_values() {
            b.push(*val);
        }
        Self::new(batch, b)
    }

    pub fn num_rows(&self) -> usize {
        self.selectivity.count_ones()
    }

    /// Return a batch with all items not selected removed.
    pub fn shrink_to_selected(mut self) -> Batch {
        for column in self.batch.columns.iter_mut() {
            (*Arc::make_mut(column)).retain(&self.selectivity);
        }
        self.batch
    }
}
