use super::StatelessOperator;
use crate::expr::PhysicalScalarExpression;
use crate::physical::TaskContext;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};

use rayexec_bullet::array::Array;
use rayexec_bullet::batch::Batch;
use rayexec_bullet::compute::filter::filter;
use rayexec_error::{RayexecError, Result};

use tracing::trace;

#[derive(Debug)]
pub struct PhysicalFilter {
    predicate: PhysicalScalarExpression,
}

impl PhysicalFilter {
    pub fn try_new(predicate: PhysicalScalarExpression) -> Result<Self> {
        trace!(?predicate, "creating physical filter");
        Ok(PhysicalFilter { predicate })
    }
}

impl StatelessOperator for PhysicalFilter {
    fn execute(&self, _task_cx: &TaskContext, input: Batch) -> Result<Batch> {
        let selection = self.predicate.eval(&input)?;
        let selection = match selection.as_ref() {
            Array::Boolean(arr) => arr,
            other => {
                return Err(RayexecError::new(format!(
                    "Expected filter predicate to evaluate to a boolean, got {}",
                    other.datatype()
                )))
            }
        };

        let filtered_arrays = input
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

impl Explainable for PhysicalFilter {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Filter")
    }
}
