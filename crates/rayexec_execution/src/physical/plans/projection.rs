use crate::expr::{Expression, PhysicalScalarExpression};
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::types::batch::{DataBatch, DataBatchSchema};
use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};
use rayexec_error::{RayexecError, Result};

use super::PhysicalOperator;

#[derive(Debug)]
pub struct PhysicalProjection {
    exprs: Vec<PhysicalScalarExpression>,
}

impl PhysicalProjection {
    pub fn try_new(exprs: Vec<PhysicalScalarExpression>) -> Result<Self> {
        Ok(PhysicalProjection { exprs })
    }
}

impl PhysicalOperator for PhysicalProjection {
    fn execute(&self, input: DataBatch) -> Result<DataBatch> {
        let arrs = self
            .exprs
            .iter()
            .map(|expr| expr.eval(&input))
            .collect::<Result<Vec<_>>>()?;

        let batch = DataBatch::try_new(arrs)?;

        Ok(batch)
    }
}

impl Explainable for PhysicalProjection {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Projection")
    }
}
