pub mod execute;
pub mod binary;
pub mod literal;
pub mod scalar;

use self::scalar::ScalarValue;
use crate::{
    planner::{operator::LogicalOperator, scope::ColumnRef},
    types::batch::{DataBatch, DataBatchSchema},
};
use arrow_array::{ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Schema};
use rayexec_error::Result;
use std::fmt::{self, Debug, Display};

#[derive(Debug)]
pub enum Expression {
    Literal(ScalarValue),
}
