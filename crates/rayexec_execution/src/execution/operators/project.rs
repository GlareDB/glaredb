use super::simple::{SimpleOperator, StatelessOperation};
use crate::expr::PhysicalScalarExpression;
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;

pub type PhysicalProject = SimpleOperator<ProjectOperation>;

#[derive(Debug)]
pub struct ProjectOperation {
    exprs: Vec<PhysicalScalarExpression>,
}

impl ProjectOperation {
    pub fn new(exprs: Vec<PhysicalScalarExpression>) -> Self {
        ProjectOperation { exprs }
    }
}

impl StatelessOperation for ProjectOperation {
    fn execute(&self, batch: Batch) -> Result<Batch> {
        let arrs = self
            .exprs
            .iter()
            .map(|expr| expr.eval(&batch))
            .collect::<Result<Vec<_>>>()?;

        let batch = Batch::try_new(arrs)?;

        Ok(batch)
    }
}
