use rayexec_error::Result;

use super::simple::{SimpleOperator, StatelessOperation};
use crate::arrays::batch::Batch;
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::PhysicalScalarExpression;
use crate::proto::DatabaseProtoConv;

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

        Batch::try_from_arrays(arrs)
    }
}

impl Explainable for ProjectOperation {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Project").with_values("projections", &self.exprs)
    }
}

impl DatabaseProtoConv for PhysicalProject {
    type ProtoType = rayexec_proto::generated::execution::PhysicalProject;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            exprs: self
                .operation
                .exprs
                .iter()
                .map(|e| e.to_proto_ctx(context))
                .collect::<Result<Vec<_>>>()?,
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            operation: ProjectOperation {
                exprs: proto
                    .exprs
                    .into_iter()
                    .map(|e| PhysicalScalarExpression::from_proto_ctx(e, context))
                    .collect::<Result<Vec<_>>>()?,
            },
        })
    }
}
