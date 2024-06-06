use crate::{
    database::{catalog::CatalogTx, create::CreateTableInfo, table::DataTable, DatabaseContext},
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
};
use futures::{future::BoxFuture, FutureExt};
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::fmt;
use std::task::{Context, Poll};

use super::{OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush};

pub struct CreateTablePartitionState {
    create: BoxFuture<'static, Result<Box<dyn DataTable>>>,
}

impl fmt::Debug for CreateTablePartitionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateTablePartitionState").finish()
    }
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

        let catalog = context.get_catalog(&self.catalog)?.catalog_modifier(&tx)?;
        let create = catalog.create_table(&self.schema, self.info.clone());

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
            PartitionState::CreateTable(state) => match state.create.poll_unpin(cx) {
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
