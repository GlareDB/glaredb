use crate::arrow::column::Column;
use crate::arrow::datatype::DataType;

use crate::arrow::row::Row;
use crate::errors::{internal, LemurError, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeSchema(pub Vec<DataType>);

impl TypeSchema {
    pub fn num_columns(&self) -> usize {
        self.0.len()
    }
}

impl From<Vec<DataType>> for TypeSchema {
    fn from(types: Vec<DataType>) -> Self {
        TypeSchema(types)
    }
}

/// A column orient set of data. Every column in the chunk must be of the same
/// length.
#[derive(Debug, Clone)]
pub struct Chunk {
    schema: TypeSchema,
    columns: Vec<Column>,
}

impl Chunk {
    /// Create an empty chunk with zero logical columns.
    pub fn empty() -> Chunk {
        Chunk {
            schema: TypeSchema::default(),
            columns: Vec::new(),
        }
    }

    /// Create an empty chunk with some number of columns that match the
    /// provided schema.
    pub fn empty_with_schema(schema: TypeSchema) -> Chunk {
        let mut columns = Vec::with_capacity(schema.num_columns());
        for _i in 0..columns.len() {
            columns.push(Column::empty());
        }
        Chunk { schema, columns }
    }

    /// Create a chunk from the provided row iterator.
    ///
    /// Errors if all rows do not have the same schema and/or number of columns.
    pub fn from_rows(rows: impl IntoIterator<Item = Row>) -> Result<Chunk> {
        let mut rows: Vec<_> = rows.into_iter().collect();
        let first = match rows.first() {
            Some(first) => first,
            None => return Ok(Self::empty()),
        };

        let schema = first.type_schema();
        let mut columns = Vec::with_capacity(schema.num_columns());

        // TODO: Kinda inefficient.
        for datatype in schema.0.iter() {
            let mut scalars = Vec::with_capacity(rows.len());
            // Note that we're starting from the last column, so we'll need to
            // reverse everything at the end.
            for row in rows.iter_mut() {
                scalars.push(
                    row.pop_last()
                        .ok_or_else(|| internal!("missing column for row"))?,
                );
            }
            let column = Column::try_from_scalars(*datatype, scalars)?;
            columns.push(column);
        }
        columns.reverse();

        Ok(Chunk { schema, columns })
    }

    /// Get a row at some index.
    ///
    /// PERF: Allocates.
    pub fn get_row(&self, idx: usize) -> Option<Row> {
        let scalars = self
            .columns
            .iter()
            .map(|col| col.get_owned_scalar(idx))
            .collect::<Option<Vec<_>>>()?;
        Some(scalars.into())
    }

    /// Iterate over rows.
    ///
    /// PERF: This makes a relatively large number of allocations. Do not use in
    /// performance sensitive areas.
    pub fn row_iter(&self) -> RowIter<'_> {
        RowIter {
            chunk: self,
            idx: 0,
        }
    }

    pub fn type_schema(&self) -> &TypeSchema {
        &self.schema
    }

    /// Get a column at some index.
    pub fn get_column(&self, idx: usize) -> Option<&Column> {
        self.columns.get(idx)
    }

    /// Horizontally stack two chunks.
    ///
    /// Errors if both chunks do not have the same number of rows.
    pub fn hstack(&self, other: &Self) -> Result<Chunk> {
        if self.num_rows() != other.num_rows() {
            return Err(LemurError::StaggeredLengths);
        }
        let columns: Vec<_> = self
            .columns
            .iter()
            .chain(other.columns.iter())
            .cloned()
            .collect();

        columns.try_into()
    }

    /// Vertically stack two chunks.
    ///
    /// Errors if the chunks do not have the same type schema.
    pub fn vstack(&self, other: &Self) -> Result<Chunk> {
        if self.type_schema() != other.type_schema() {
            return Err(LemurError::TypeMismatch);
        }

        let columns = self
            .columns
            .iter()
            .zip(other.columns.iter())
            .map(|(left, right)| left.concat(right))
            .collect::<Result<Vec<_>>>()?;

        columns.try_into()
    }

    pub fn num_cols(&self) -> usize {
        self.columns.len()
    }

    pub fn num_rows(&self) -> usize {
        self.columns.first().map(|col| col.len()).unwrap_or(0)
    }
}

pub struct RowIter<'a> {
    chunk: &'a Chunk,
    idx: usize,
}

impl<'a> Iterator for RowIter<'a> {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.chunk.num_rows() {
            return None;
        }
        let row = self.chunk.get_row(self.idx);
        self.idx += 1;
        row
    }
}

impl TryFrom<Vec<Column>> for Chunk {
    type Error = LemurError;
    fn try_from(columns: Vec<Column>) -> Result<Self> {
        let mut iter = columns.iter();
        let first = match iter.next() {
            Some(first) => first.len(),
            None => return Ok(Self::empty()),
        };

        if !iter.all(|v| v.len() == first) {
            return Err(LemurError::StaggeredLengths);
        }

        let schema = columns
            .iter()
            .map(|col| col.get_datatype())
            .collect::<Result<Vec<_>>>()?;

        Ok(Chunk {
            columns,
            schema: schema.into(),
        })
    }
}
