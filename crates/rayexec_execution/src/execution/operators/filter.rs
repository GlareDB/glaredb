use super::simple::{SimpleOperator, StatelessOperation};
use crate::expr::PhysicalScalarExpression;
use rayexec_bullet::{array::Array, batch::Batch, compute::filter::filter};
use rayexec_error::{RayexecError, Result};

pub type PhysicalFilter = SimpleOperator<FilterOperation>;

#[derive(Debug)]
pub struct FilterOperation {
    predicate: PhysicalScalarExpression,
}

impl FilterOperation {
    pub fn new(predicate: PhysicalScalarExpression) -> Self {
        FilterOperation { predicate }
    }
}

impl StatelessOperation for FilterOperation {
    fn execute(&self, batch: Batch) -> Result<Batch> {
        let selection = self.predicate.eval(&batch)?;
        let selection = match selection.as_ref() {
            Array::Boolean(arr) => arr,
            other => {
                return Err(RayexecError::new(format!(
                    "Expected filter predicate to evaluate to a boolean, got {}",
                    other.datatype()
                )))
            }
        };

        let filtered_arrays = batch
            .columns()
            .iter()
            .map(|a| filter(a, selection))
            .collect::<Result<Vec<_>, _>>()?;

        let batch = if filtered_arrays.is_empty() {
            // If we're working on an empty input batch, just produce an new
            // empty batch with num rows equaling the number of trues in the
            // selection.
            Batch::empty_with_num_rows(selection.true_count())
        } else {
            // Otherwise use the actual filtered arrays.
            Batch::try_new(filtered_arrays)?
        };

        Ok(batch)
    }
}
