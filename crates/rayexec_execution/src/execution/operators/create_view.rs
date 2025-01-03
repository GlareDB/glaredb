use std::fmt;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use futures::FutureExt;
use rayexec_error::{RayexecError, Result};

use super::{
    ExecutableOperator,
    ExecutionStates,
    InputOutputStates,
    OperatorState,
    PartitionState,
    PollFinalize,
    PollPull,
    PollPush,
};
use crate::arrays::batch::Batch2;
use crate::database::catalog::CatalogTx;
use crate::database::create::CreateViewInfo;
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::proto::DatabaseProtoConv;

pub struct CreateViewPartitionState {
    create: BoxFuture<'static, Result<()>>,
}

impl fmt::Debug for CreateViewPartitionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateViewPartitionState").finish()
    }
}

#[derive(Debug)]
pub struct PhysicalCreateView {
    pub(crate) catalog: String,
    pub(crate) schema: String,
    pub(crate) info: CreateViewInfo,
}

impl ExecutableOperator for PhysicalCreateView {
    fn create_states(
        &self,
        context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        if partitions[0] != 1 {
            return Err(RayexecError::new(
                "Create schema operator can only handle 1 partition",
            ));
        }

        // TODO: Placeholder.
        let tx = CatalogTx::new();

        let database = context.get_database(&self.catalog)?;
        let schema_ent = database
            .catalog
            .get_schema(&tx, &self.schema)?
            .ok_or_else(|| {
                RayexecError::new(format!("Missing schema for view create: {}", self.schema))
            })?;

        let info = self.info.clone();
        let create = Box::pin(async move {
            let _ = schema_ent.create_view(&tx, &info)?;
            // TODO: And persist some how (write to log, flush on commit)
            // TODO: Probably doesn't even need to be async...
            Ok(())
        });

        Ok(ExecutionStates {
            operator_state: Arc::new(OperatorState::None),
            partition_states: InputOutputStates::OneToOne {
                partition_states: vec![PartitionState::CreateView(CreateViewPartitionState {
                    create,
                })],
            },
        })
    }

    fn poll_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        _batch: Batch2,
    ) -> Result<PollPush> {
        Err(RayexecError::new("Cannot push to physical create view"))
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        Err(RayexecError::new("Cannot push to physical create view"))
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        match partition_state {
            PartitionState::CreateView(state) => match state.create.poll_unpin(cx) {
                Poll::Ready(Ok(_)) => Ok(PollPull::Exhausted),
                Poll::Ready(Err(e)) => Err(e),
                Poll::Pending => Ok(PollPull::Pending),
            },
            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl Explainable for PhysicalCreateView {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CreateView").with_value("view", &self.info.name)
    }
}

impl DatabaseProtoConv for PhysicalCreateView {
    type ProtoType = rayexec_proto::generated::execution::PhysicalCreateSchema;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        unimplemented!()
    }

    fn from_proto_ctx(_proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        unimplemented!()
    }
}
