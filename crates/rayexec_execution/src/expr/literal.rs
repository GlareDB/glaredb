use crate::types::batch::{DataBatch, DataBatchSchema};

use super::{scalar::ScalarValue, PhysicalExpr};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Schema};
use rayexec_error::Result;
use std::fmt;

/// An expression whose return value is always a literal.
///
/// When evaluating against a record batch, returned array's length will match
/// the number of rows in the batch.
#[derive(Debug, Clone, PartialEq)]
pub struct LiteralExpr {
    pub value: ScalarValue,
}

impl LiteralExpr {
    pub fn new(value: ScalarValue) -> Self {
        LiteralExpr { value }
    }
}

impl fmt::Display for LiteralExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl PhysicalExpr for LiteralExpr {
    fn data_type(&self, _input: &DataBatchSchema) -> Result<DataType> {
        Ok(self.value.data_type())
    }

    fn nullable(&self, _input: &DataBatchSchema) -> Result<bool> {
        Ok(matches!(self.value, ScalarValue::Null))
    }

    fn eval(&self, batch: &DataBatch) -> Result<ArrayRef> {
        self.value.as_array(batch.num_rows())
    }
}
