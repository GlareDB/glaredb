use arrow_array::{new_empty_array, ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use rayexec_error::{RayexecError, Result};
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use crate::types::batch::{DataBatch, DataBatchSchema};

use super::PhysicalExpr;

/// Expression for referencing a column in a record batch by its index.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnIndex(pub usize);

impl ColumnIndex {
    fn get_field<'a>(&self, input: &'a Schema) -> Result<&'a Arc<Field>> {
        let field = input
            .fields()
            .get(self.0)
            .ok_or_else(|| RayexecError::new(format!("missing column for index {}", self.0)))?;
        Ok(field)
    }
}

impl fmt::Display for ColumnIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Column({})", self.0)
    }
}

impl PhysicalExpr for ColumnIndex {
    fn data_type(&self, input: &DataBatchSchema) -> Result<DataType> {
        Ok(input.get_types().get(self.0).unwrap().clone())
    }

    fn nullable(&self, input: &DataBatchSchema) -> Result<bool> {
        // let field = self.get_field(input)?;
        Ok(true)
    }

    fn eval(&self, batch: &DataBatch) -> Result<ArrayRef> {
        Ok(batch.column(self.0).unwrap().clone())
    }
}
