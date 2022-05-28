use crate::column::{ColumnError, NullableColumnVec};
use std::borrow::Cow;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Relation {
    columns: Vec<Arc<NullableColumnVec>>,
}

impl Relation {
    /// Create a relation from an iterator of owned columns. Each column must
    /// have the same length.
    pub fn from_col_iter<I>(cols: I) -> Result<Self, ColumnError>
    where
        I: IntoIterator<Item = NullableColumnVec>,
    {
        let mut iter = cols.into_iter();
        match iter.next() {
            Some(first) => {
                let len = first.len();
                let mut columns = vec![Arc::new(first)];
                for col in iter {
                    if col.len() != len {
                        return Err(ColumnError::LengthMismatch(len, col.len()));
                    }
                    columns.push(Arc::new(col));
                }

                Ok(Relation { columns })
            }
            None => Ok(Relation {
                columns: Vec::new(),
            }),
        }
    }

    /// Append a new column to the relation, returning the column's index.
    pub fn append_column(&mut self, col: NullableColumnVec) -> Result<usize, ColumnError> {
        if let Some(existing) = self.columns.first() {
            if existing.len() != col.len() {
                return Err(ColumnError::LengthMismatch(existing.len(), col.len()));
            }
        }

        self.columns.push(Arc::new(col));
        let idx = self.columns.len() - 1;
        Ok(idx)
    }

    /// Get a reference to a column.
    pub fn get_column(&self, idx: usize) -> Option<&NullableColumnVec> {
        let col = self.columns.get(idx)?;
        Some(&col)
    }
}
