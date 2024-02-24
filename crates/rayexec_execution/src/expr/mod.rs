pub mod binary;
pub mod column;
pub mod execute;
pub mod literal;
pub mod scalar;
pub mod table;

use arrow_array::{ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Schema};
use std::fmt::{self, Debug, Display};

use rayexec_error::Result;

use crate::{
    planner::operator::LogicalOperator,
    types::batch::{DataBatch, DataBatchSchema},
};

use self::{binary::BinaryExpr, column::ColumnIndex, scalar::ScalarValue};

pub trait PhysicalExpr: Send + Sync + Display + Debug {
    /// Data type of the result based on the input schema.
    fn data_type(&self, input: &DataBatchSchema) -> Result<DataType>;

    /// If the result is nullable based on the input schema.
    fn nullable(&self, input: &DataBatchSchema) -> Result<bool>;

    /// Evaluate the expr on the batch.
    fn eval(&self, batch: &DataBatch) -> Result<ArrayRef>;

    /// Evaluate the expr on only a selection of the batch.
    fn eval_selection(&self, batch: &RecordBatch, selection: &BooleanArray) -> Result<ArrayRef> {
        unimplemented!()
    }
}

/// A scalar expression.
#[derive(Debug)]
pub enum Expression {
    Literal(ScalarValue),
    Binary(BinaryExpr),
    Column(ColumnIndex),
    Subquery(Subquery),
}

impl Expression {
    pub fn into_physical_expression(self) -> Result<Box<dyn PhysicalExpr>> {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct Subquery {
    pub plan: Box<LogicalOperator>,
}
