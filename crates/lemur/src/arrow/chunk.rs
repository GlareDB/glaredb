use crate::arrow::column::{BoolColumn, Column};
use crate::arrow::datatype::DataType;
use crate::errors::{LemurError, Result};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TypeSchema(pub Vec<DataType>);

impl From<Vec<DataType>> for TypeSchema {
    fn from(types: Vec<DataType>) -> Self {
        TypeSchema(types)
    }
}

#[derive(Debug)]
pub struct Chunk {
    schema: TypeSchema,
    columns: Vec<Column>,
}

impl Chunk {
    pub fn empty() -> Chunk {
        Chunk {
            schema: TypeSchema::default(),
            columns: Vec::new(),
        }
    }

    pub fn type_schema(&self) -> &TypeSchema {
        &self.schema
    }

    pub fn get_column(&self, idx: usize) -> Option<&Column> {
        self.columns.get(idx)
    }

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

        Ok(columns.try_into()?)
    }

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
        self.columns
            .first()
            .and_then(|col| Some(col.len()))
            .unwrap_or(0)
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
