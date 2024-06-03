use crate::{
    database::{
        catalog::CatalogTx, create::CreateTableInfo, ddl::CreateFut, table::DataTable,
        DatabaseContext,
    },
    planner::explainable::{ExplainConfig, ExplainEntry, Explainable},
};
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::task::{Context, Poll};

use super::{OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush};

#[derive(Debug)]
pub struct CreateTablePartitionState {
    create: Box<dyn CreateFut<Output = Box<dyn DataTable>>>,
}

#[derive(Debug)]
pub struct PhysicalCreateTable {
    catalog: String,
    schema: String,
    info: CreateTableInfo,
}

impl PhysicalCreateTable {
    pub fn new(
        catalog: impl Into<String>,
        schema: impl Into<String>,
        info: CreateTableInfo,
    ) -> Self {
        PhysicalCreateTable {
            catalog: catalog.into(),
            schema: schema.into(),
            info,
        }
    }

    pub fn try_create_state(&self, context: &DatabaseContext) -> Result<CreateTablePartitionState> {
        // TODO: Placeholder.
        let tx = CatalogTx::new();

        let create = context
            .get_catalog(&self.catalog)?
            .catalog_modifier(&tx)?
            .create_table(&self.schema, self.info.clone())?;

        Ok(CreateTablePartitionState { create })
    }
}

impl PhysicalOperator for PhysicalCreateTable {
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
            PartitionState::CreateTable(state) => match state.create.poll_create(cx) {
                Poll::Ready(Ok(_)) => Ok(PollPull::Exhausted),
                Poll::Ready(Err(e)) => Err(e),
                Poll::Pending => Ok(PollPull::Pending),
            },
            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl Explainable for PhysicalCreateTable {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CreateTable").with_value("table", &self.info.name)
    }
}
