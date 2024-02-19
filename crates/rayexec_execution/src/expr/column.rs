use arrow_array::{new_empty_array, ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use rayexec_error::{RayexecError, Result};
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use super::PhysicalExpr;

/// Expression for referencing a column in a record batch either by its index or
/// name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ColumnExpr {
    Index(usize),
    Name(String),
}

impl ColumnExpr {
    fn get_field<'a>(&self, input: &'a Schema) -> Result<&'a Arc<Field>> {
        match self {
            Self::Index(idx) => {
                let field = input
                    .fields()
                    .get(*idx)
                    .ok_or_else(|| RayexecError::new(format!("missing column for index {idx}")))?;
                Ok(field)
            }
            Self::Name(name) => {
                let (_, field) = input
                    .fields()
                    .find(name)
                    .ok_or_else(|| RayexecError::new(format!("missing column for name {name}")))?;
                Ok(field)
            }
        }
    }
}

impl fmt::Display for ColumnExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Index(idx) => write!(f, "{idx}"),
            Self::Name(name) => write!(f, "{name}"),
        }
    }
}

impl PhysicalExpr for ColumnExpr {
    fn data_type(&self, input: &Schema) -> Result<DataType> {
        let field = self.get_field(input)?;
        Ok(field.data_type().clone())
    }

    fn nullable(&self, input: &Schema) -> Result<bool> {
        let field = self.get_field(input)?;
        Ok(field.is_nullable())
    }

    fn eval(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        match self {
            Self::Index(idx) => Ok(batch.column(*idx).clone()),
            Self::Name(name) => Ok(batch.column_by_name(name).expect("column to exist").clone()),
        }
    }
}
