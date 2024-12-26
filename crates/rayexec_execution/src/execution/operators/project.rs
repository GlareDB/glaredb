use std::task::Context;

use rayexec_bullet::batch::BatchOld;
use rayexec_error::{OptionExt, Result};

use super::simple::{SimpleOperator, StatelessOperation};
use super::{
    ExecutableOperator,
    ExecutableOperatorOld,
    ExecuteInOutState,
    OperatorState,
    OperatorStateOld,
    PartitionState,
    PartitionStateOld,
    PollExecute,
    PollFinalize,
};
use crate::arrays::batch::Batch;
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::evaluator::ExpressionEvaluator;
use crate::expr::physical::PhysicalScalarExpression;
use crate::proto::DatabaseProtoConv;

#[derive(Debug)]
pub struct PhysicalProject {
    pub(crate) projections: Vec<PhysicalScalarExpression>,
}

#[derive(Debug)]
pub struct ProjectPartitionState {
    evaluator: ExpressionEvaluator,
}

impl ExecutableOperator for PhysicalProject {
    fn poll_execute(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        inout: ExecuteInOutState,
    ) -> Result<PollExecute> {
        let input = inout.input.required("batch input")?;
        let output = inout.output.required("batch output")?;

        let state = match partition_state {
            PartitionState::Project(state) => state,
            other => panic!("invalid state: {other:?}"),
        };

        // TODO: Reset output.

        let sel = input.generate_selection();
        state.evaluator.eval_batch(input, sel, output)?;

        Ok(PollExecute::Ready)
    }

    fn poll_finalize(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        Ok(PollFinalize::Finalized)
    }
}

impl Explainable for PhysicalProject {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        unimplemented!()
    }
}

pub type PhysicalProjectOld = SimpleOperator<ProjectOperation>;

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
    fn execute(&self, batch: BatchOld) -> Result<BatchOld> {
        let arrs = self
            .exprs
            .iter()
            .map(|expr| {
                let arr = expr.eval2(&batch)?;
                Ok(arr.into_owned())
            })
            .collect::<Result<Vec<_>>>()?;

        BatchOld::try_new(arrs)
    }
}

impl Explainable for ProjectOperation {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Project").with_values("projections", &self.exprs)
    }
}

impl DatabaseProtoConv for PhysicalProjectOld {
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
