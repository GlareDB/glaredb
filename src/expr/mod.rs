pub mod binary;
pub mod column;
pub mod literal;
pub mod logical;
pub mod scalar;
pub mod table;

use arrow_array::{ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Schema};
use std::fmt::{self, Debug, Display};

use crate::errors::Result;

pub trait PhysicalExpr: Send + Sync + Display + Debug {
    /// Data type of the result based on the input schema.
    fn data_type(&self, input: &Schema) -> Result<DataType>;

    /// If the result is nullable based on the input schema.
    fn nullable(&self, input: &Schema) -> Result<bool>;

    /// Evaluate the expr on the batch.
    fn eval(&self, batch: &RecordBatch) -> Result<ArrayRef>;

    /// Evaluate the expr on only a selection of the batch.
    fn eval_selection(&self, batch: &RecordBatch, selection: &BooleanArray) -> Result<ArrayRef> {
        unimplemented!()
    }
}
