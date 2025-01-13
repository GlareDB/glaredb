use std::fmt;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use futures::FutureExt;
use rayexec_error::{OptionExt, RayexecError, Result};
use rayexec_proto::ProtoConv;

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
use crate::arrays::batch::Batch;
use crate::database::catalog::CatalogTx;
use crate::database::create::CreateSchemaInfo;
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::proto::DatabaseProtoConv;

pub struct CreateSchemaPartitionState {
    create: BoxFuture<'static, Result<()>>,
}

impl fmt::Debug for CreateSchemaPartitionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateSchemaPartitionState").finish()
    }
}

#[derive(Debug)]
pub struct PhysicalCreateSchema {
    pub(crate) catalog: String,
    pub(crate) info: CreateSchemaInfo,
}

impl PhysicalCreateSchema {
    pub fn new(catalog: impl Into<String>, info: CreateSchemaInfo) -> Self {
        PhysicalCreateSchema {
            catalog: catalog.into(),
            info,
        }
    }
}

impl ExecutableOperator for PhysicalCreateSchema {
    fn create_states2(
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

        let catalog = context.get_database(&self.catalog)?.catalog.clone();
        let info = self.info.clone();
        let create = Box::pin(async move {
            catalog.create_schema(&tx, &info)?;
            // TODO: And persist some how (write to log, flush on commit)
            // TODO: Probably doesn't even need to be async...
            Ok(())
        });

        Ok(ExecutionStates {
            operator_state: Arc::new(OperatorState::None),
            partition_states: InputOutputStates::OneToOne {
                partition_states: vec![PartitionState::CreateSchema(CreateSchemaPartitionState {
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
        _batch: Batch,
    ) -> Result<PollPush> {
        Err(RayexecError::new("Cannot push to physical create table"))
    }

    fn poll_finalize(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        Err(RayexecError::new("Cannot push to physical create table"))
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        match partition_state {
            PartitionState::CreateSchema(state) => match state.create.poll_unpin(cx) {
                Poll::Ready(Ok(_)) => Ok(PollPull::Exhausted),
                Poll::Ready(Err(e)) => Err(e),
                Poll::Pending => Ok(PollPull::Pending),
            },
            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl Explainable for PhysicalCreateSchema {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CreateSchema").with_value("schema", &self.info.name)
    }
}

impl DatabaseProtoConv for PhysicalCreateSchema {
    type ProtoType = rayexec_proto::generated::execution::PhysicalCreateSchema;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            catalog: self.catalog.clone(),
            info: Some(self.info.to_proto()?),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            catalog: proto.catalog,
            info: CreateSchemaInfo::from_proto(proto.info.required("info")?)?,
        })
    }
}
