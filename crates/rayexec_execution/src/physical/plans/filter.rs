use super::PhysicalOperator;
use crate::expr::{Expression, PhysicalScalarExpression};
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::types::batch::{DataBatch, DataBatchSchema};
use arrow::compute::{filter, FilterBuilder};
use arrow_array::cast::AsArray;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema};
use rayexec_error::{RayexecError, Result, ResultExt};
use std::task::{Context, Poll};
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

impl PhysicalOperator for PhysicalFilter {
    fn execute(&self, input: DataBatch) -> Result<DataBatch> {
        let selection = self.predicate.eval(&input)?;
        // TODO: Need to check that this is actually a boolean somewhere.
        let selection = selection.as_boolean();

        let filtered_arrays = input
            .columns()
            .iter()
            .map(|a| filter(a, &selection))
            .collect::<Result<Vec<_>, _>>()?;

        let batch = if filtered_arrays.is_empty() {
            // If we're working on an empty input batch, just produce an new
            // empty batch with num rows equaling the number of trues in the
            // selection.
            DataBatch::empty_with_num_rows(selection.true_count())
        } else {
            // Otherwise use the actual filtered arrays.
            DataBatch::try_new(filtered_arrays)?
        };

        Ok(batch)
    }
}

impl Explainable for PhysicalFilter {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Filter")
    }
}
