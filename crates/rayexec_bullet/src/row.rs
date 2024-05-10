use rayexec_error::{RayexecError, Result};

use crate::{array::Array, scalar::ScalarValue};

/// Representation of a single row.
#[derive(Debug, Clone, PartialEq)]
pub struct Row<'a> {
    pub columns: Vec<ScalarValue<'a>>,
}

/// A row with full ownership of all its values.
pub type OwnedRow = Row<'static>;

impl<'a> Row<'a> {
    /// Create an empty row.
    pub const fn empty() -> Self {
        Row {
            columns: Vec::new(),
        }
    }

    /// Create a new row representation backed by data from arrays.
    pub fn try_new_from_arrays(arrays: &[&'a Array], row: usize) -> Result<Row<'a>> {
        let scalars = arrays
            .iter()
            .map(|arr| arr.scalar(row))
            .collect::<Option<Vec<_>>>();
        let scalars =
            scalars.ok_or_else(|| RayexecError::new("Row {idx} does not exist in arrays"))?;

        Ok(Row { columns: scalars })
    }

    /// Return an iterator over all columns in the row.
    pub fn iter(&self) -> impl Iterator<Item = &ScalarValue> {
        self.columns.iter()
    }

    pub fn into_owned(self) -> OwnedRow {
        Row {
            columns: self
                .columns
                .into_iter()
                .map(|scalar| scalar.into_owned())
                .collect(),
        }
    }
}

impl<'a> FromIterator<ScalarValue<'a>> for Row<'a> {
    fn from_iter<T: IntoIterator<Item = ScalarValue<'a>>>(iter: T) -> Self {
        Row {
            columns: iter.into_iter().collect(),
        }
    }
}

/// Representation of multiple rows.
#[derive(Debug, Clone, PartialEq)]
pub struct Rows<'a>(pub Vec<Row<'a>>);

/// A set of rows with full ownership of all values.
pub type OwnedRows = Rows<'static>;

impl<'a> Rows<'a> {
    /// Return an iterator over all rows.
    pub fn iter(&self) -> impl Iterator<Item = &Row> {
        self.0.iter()
    }

    pub fn into_owned(self) -> OwnedRows {
        Rows(self.0.into_iter().map(|row| row.into_owned()).collect())
    }
}
