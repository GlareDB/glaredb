use super::{scalar::ScalarValue, PhysicalExpr};
use crate::errors::Result;
use arrow_array::{new_empty_array, ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Schema};
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub struct LiteralExpr {
    pub value: ScalarValue,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalLiteralExpr {
    pub value: ScalarValue,
}

impl fmt::Display for PhysicalLiteralExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl PhysicalExpr for PhysicalLiteralExpr {
    fn data_type(&self, input: &Schema) -> Result<DataType> {
        unimplemented!()
    }

    fn nullable(&self, input: &Schema) -> Result<bool> {
        unimplemented!()
    }

    fn eval(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        unimplemented!()
    }
}
