pub mod encoding;

use rayexec_error::{RayexecError, Result};

use crate::{array::Array, scalar::ScalarValue};

/// Scalar representation of a single row.
#[derive(Debug, Clone, PartialEq)]
pub struct ScalarRow<'a> {
    pub columns: Vec<ScalarValue<'a>>,
}

/// A row with full ownership of all its values.
pub type OwnedScalarRow = ScalarRow<'static>;

impl<'a> ScalarRow<'a> {
    /// Create an empty row.
    pub const fn empty() -> Self {
        ScalarRow {
            columns: Vec::new(),
        }
    }

    /// Create a new row representation backed by data from arrays.
    pub fn try_new_from_arrays(arrays: &[&'a Array], row: usize) -> Result<ScalarRow<'a>> {
        let scalars = arrays
            .iter()
            .map(|arr| arr.scalar(row))
            .collect::<Option<Vec<_>>>();
        let scalars =
            scalars.ok_or_else(|| RayexecError::new("Row {idx} does not exist in arrays"))?;

        Ok(ScalarRow { columns: scalars })
    }

    /// Return an iterator over all columns in the row.
    pub fn iter(&self) -> impl Iterator<Item = &ScalarValue> {
        self.columns.iter()
    }

    pub fn into_owned(self) -> OwnedScalarRow {
        ScalarRow {
            columns: self
                .columns
                .into_iter()
                .map(|scalar| scalar.into_owned())
                .collect(),
        }
    }
}

impl<'a> FromIterator<ScalarValue<'a>> for ScalarRow<'a> {
    fn from_iter<T: IntoIterator<Item = ScalarValue<'a>>>(iter: T) -> Self {
        ScalarRow {
            columns: iter.into_iter().collect(),
        }
    }
}
