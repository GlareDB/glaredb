use crate::datatype::{DataType, DataValue, RelationSchema, Row};
use crate::expr::EvaluatedExpr;
use crate::vec::{BoolVec, ColumnVec};
use anyhow::{anyhow, Result};
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
#[derive(Debug, Clone)]
pub enum BatchRepr {
    /// A column-oriented batch of records.
    Batch(Batch),
    /// A column-oriented batch with a row selectivity vector.
    Selectivity(SelectivityBatch),
    /// The output of a cross join of two batches.
    CrossJoin(CrossJoinBatch),
}

impl BatchRepr {
    pub fn empty() -> BatchRepr {
        Batch::empty().into()
    }

    /// Make any required changes to the underlying columns in order to return a
    /// valid batch for this representation.
    pub fn into_batch(self) -> Batch {
        match self {
            BatchRepr::Batch(batch) => batch,
            BatchRepr::Selectivity(batch) => batch.shrink_to_selected(),
            BatchRepr::CrossJoin(batch) => batch.into_batch(),
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

impl From<CrossJoinBatch> for BatchRepr {
    fn from(batch: CrossJoinBatch) -> Self {
        BatchRepr::CrossJoin(batch)
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
    /// Create a new empty batch.
    ///
    /// The resulting batch will have an arity of zero.
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
    pub fn push_row(&mut self, row: &Row) -> Result<(), BatchError> {
        if row.arity() != self.arity() {
            return Err(BatchError::ColumnSizeMismatch {
                expected: self.arity(),
                got: row.arity(),
            });
        }

        let iter = row.0.iter().zip(self.columns.iter_mut());
        for (value, orig) in iter {
            (*Arc::make_mut(orig)).push_value(value)?;
        }

        Ok(())
    }

    pub fn get_row(&self, idx: usize) -> Option<Row> {
        if idx >= self.num_rows() {
            return None;
        }
        let mut row = Vec::with_capacity(self.arity());
        for col in self.columns.iter() {
            row.push(col.get_value(idx).unwrap());
        }
        Some(row.into())
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

    pub fn row_iter(&self) -> RowIter<'_> {
        RowIter {
            batch: self,
            idx: 0,
        }
    }
}

/// A relatively inefficient iterator over a batch returning rows.
///
/// This should really only be used in tests and not during real execution of
/// the database. During execution, everything should be handling batches.
pub struct RowIter<'a> {
    batch: &'a Batch,
    idx: usize,
}

impl<'a> Iterator for RowIter<'a> {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.batch.get_row(self.idx)?;
        self.idx += 1;
        Some(row)
    }
}

/// A batch with a selectivity bit vector indicating "visible" rows.
#[derive(Debug, Clone)]
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

    /// Create a new selecticity batch using the result of an expression
    /// evaluation. The result should either be a bool vector, or a bool value.
    pub fn new_with_eval_result(batch: Batch, result: &EvaluatedExpr) -> Result<SelectivityBatch> {
        let with_col = |batch: Batch, col: &ColumnVec| {
            Self::new_with_bool_vec(
                batch,
                col.try_as_bool_vec()
                    .ok_or(anyhow!("column not a bool vec"))?,
            )
        };
        Ok(match result {
            EvaluatedExpr::ColumnRef(col) => with_col(batch, col)?,
            EvaluatedExpr::Column(col) => with_col(batch, col)?,
            EvaluatedExpr::Value(DataValue::Bool(b), _) => {
                let num_rows = batch.num_rows();
                Self::new(batch, BitVec::repeat(*b, num_rows))?
            }
            _ => return Err(anyhow!("eval result not a bool vec or bool value")),
        })
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

/// A cross join representation of two batches.
#[derive(Debug, Clone)]
pub struct CrossJoinBatch {
    left: Batch,
    left_repeat: usize,
    right: Batch,
    right_repeat: usize,
}

impl CrossJoinBatch {
    pub fn from_batches(left: Batch, right: Batch) -> CrossJoinBatch {
        let left_repeat = right.num_rows();
        let right_repeat = left.num_rows();
        CrossJoinBatch {
            left,
            left_repeat,
            right,
            right_repeat,
        }
    }

    pub fn num_row(&self) -> usize {
        self.left.num_rows() * self.left_repeat
    }

    /// Materialize the resulting columns from the cross join.
    // TODO: Pass in optional selectivitity filter.
    pub fn into_batch(self) -> Batch {
        // TODO: Not terribly efficient.

        // Extend out the left side of the join by repeating each row.
        unimplemented!()
        // let mut left_cols = Vec::with_capacity(self.left.arity());
        // for row in self.left.row_iter() {
        //     for _i in 0..self.left_repeat {
        //         let iter = row.0.iter().zip(self.columns.iter_mut());
        //         for (value, orig) in iter {
        //             (*Arc::make_mut(orig)).push_value(value)?;
        //         }

        //         temp_left.push_row(&row).unwrap();
        //     }
        // }

        // let mut right_cols = Vec::with_capacity(self.right.arity());
        // for right in self.right.columns.into_iter() {
        //     let right = Arc::try_unwrap(right).unwrap_or_else(|v| (*v).clone());
        //     let mut repeats = vec![right; self.right_repeat].into_iter();
        //     let mut right = match repeats.next() {
        //         Some(v) => v,
        //         None => return Batch::empty(),
        //     };
        //     for repeat in repeats {
        //         right.append(repeat).unwrap();
        //     }
        //     right_cols.push(right)
        // }

        // let mut cols = temp_left.columns;
        // cols.extend(right_cols.into_iter().map(|col| Arc::new(col)));

        // Batch { columns: cols }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vec::native_vec::Int64Vec;

    #[test]
    fn cross_join_simple() {
        let b1 = Batch::from_columns([
            Int64Vec::from_iter_all_valid([1, 2]).into(),
            Int64Vec::from_iter_all_valid([3, 4]).into(),
        ])
        .unwrap();

        let b2 = Batch::from_columns([
            Int64Vec::from_iter_all_valid([5, 6, 7]).into(),
            Int64Vec::from_iter_all_valid([8, 9, 10]).into(),
        ])
        .unwrap();

        let cross = CrossJoinBatch::from_batches(b1, b2);
        // let batch = cross.into_batch();
        // println!("batch: {:#?}", batch);

        // let rows: Vec<_> = batch.row_iter().collect();

        // panic!("rows: {:#?}", rows);
        // assert_eq!(4, rows.len());
    }
}
