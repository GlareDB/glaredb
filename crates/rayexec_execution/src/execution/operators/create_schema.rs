use crate::{
    database::{catalog::CatalogTx, create::CreateSchemaInfo, ddl::CreateFut, DatabaseContext},
    planner::explainable::{ExplainConfig, ExplainEntry, Explainable},
};
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::task::{Context, Poll};

use super::{OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush};

#[derive(Debug)]
pub struct CreateSchemaPartitionState {
    create: Box<dyn CreateFut<Output = ()>>,
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

    pub fn try_create_state(
        &self,
        context: &DatabaseContext,
    ) -> Result<CreateSchemaPartitionState> {
        // TODO: Placeholder.
        let tx = CatalogTx::new();

        let create = context
            .get_catalog(&self.catalog)?
            .catalog_modifier(&tx)?
            .create_schema(self.info.clone())?;

        Ok(CreateSchemaPartitionState { create })
    }
}

impl PhysicalOperator for PhysicalCreateSchema {
    fn poll_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        _batch: Batch,
    ) -> Result<PollPush> {
        Err(RayexecError::new("Cannot push to physical create table"))
    }

    fn finalize_push(
        &self,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<()> {
        Err(RayexecError::new("Cannot push to physical create table"))
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        match partition_state {
            PartitionState::CreateSchema(state) => match state.create.poll_create(cx) {
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
