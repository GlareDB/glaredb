use arrow_array::{ArrayRef, BooleanArray};
use arrow_schema::DataType;
use rayexec_error::{RayexecError, Result};

use crate::types::batch::{DataBatch, DataBatchSchema};

use super::Expression;

/// Execute scalar expressions on input batches.
// TODO: This should hold a reference to query params for prepared queries. This
// would allow us to cache the physical plans for a prepared query, and
// dynamically insert the parameters during execution.
#[derive(Debug)]
pub struct ScalarExecutor {
    expr: Expression,
}

impl ScalarExecutor {
    pub fn try_new(expr: Expression) -> Result<Self> {
        // TODO: Check is scalar.
        Ok(ScalarExecutor { expr })
    }

    /// Get a reference to the expression of this executor.
    pub fn expression(&self) -> &Expression {
        &self.expr
    }

    /// Get the data type of the output of an expression based on the given
    /// input schema.
    pub fn data_type(&self, input: &DataBatchSchema) -> Result<DataType> {
        unimplemented!()
    }

    /// Evaluate the expression on the given input batch.
    pub fn eval(&self, input: &DataBatch) -> Result<ArrayRef> {
        unimplemented!()
    }

    /// Evaluate the expression on a selection of the input batch.
    pub fn eval_selection(&self, input: &DataBatch, selection: &BooleanArray) -> Result<ArrayRef> {
        unimplemented!()
    }
}
