use crate::{
    database::{catalog::CatalogTx, create::CreateSchemaInfo, DatabaseContext},
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
};
use futures::{future::BoxFuture, FutureExt};
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::task::{Context, Poll};
use std::{fmt, sync::Arc};

use super::{
    ExecutionStates, InputOutputStates, OperatorState, PartitionState, PhysicalOperator,
    PollFinalize, PollPull, PollPush,
};

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
    catalog: String,
    info: CreateSchemaInfo,
}

impl PhysicalCreateSchema {
    pub fn new(catalog: impl Into<String>, info: CreateSchemaInfo) -> Self {
        PhysicalCreateSchema {
            catalog: catalog.into(),
            info,
        }
    }
}

impl PhysicalOperator for PhysicalCreateSchema {
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

        let catalog = context.get_catalog(&self.catalog)?.catalog_modifier(&tx)?;
        let create = catalog.create_schema(self.info.clone());

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

    fn poll_finalize_push(
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
