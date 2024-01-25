use arrow_array::{ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Schema};
use std::fmt::{self, Debug, Display};

use crate::errors::Result;

pub trait Expr: Send + Sync + Display + Debug {
    fn data_type(&self, input: &Schema) -> Result<DataType>;
    fn nullable(&self, input: &Schema) -> Result<bool>;
    fn eval(&self, batch: &RecordBatch) -> Result<ArrayRef>;
    fn eval_selection(&self, batch: &RecordBatch, selection: &BooleanArray) -> Result<ArrayRef> {
        unimplemented!()
    }
}
