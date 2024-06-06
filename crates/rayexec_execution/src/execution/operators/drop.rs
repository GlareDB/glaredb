use crate::{
    database::{catalog::CatalogTx, drop::DropInfo, DatabaseContext},
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
};
use futures::{future::BoxFuture, FutureExt};
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::fmt;
use std::task::{Context, Poll};

use super::{OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush};

pub struct DropPartitionState {
    drop: BoxFuture<'static, Result<()>>,
}

impl fmt::Debug for DropPartitionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DropPartitionState").finish()
    }
}

#[derive(Debug)]
pub struct PhysicalDrop {
    info: DropInfo,
}

impl PhysicalDrop {
    pub fn new(info: DropInfo) -> Self {
        PhysicalDrop { info }
    }

    pub fn try_create_state(&self, context: &DatabaseContext) -> Result<DropPartitionState> {
        // TODO: Placeholder.
        let tx = CatalogTx::new();

        let catalog = context
            .get_catalog(&self.info.catalog)?
            .catalog_modifier(&tx)?;
        let drop = catalog.drop_entry(self.info.clone());

        Ok(DropPartitionState { drop })
    }
}

impl PhysicalOperator for PhysicalDrop {
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
            PartitionState::Drop(state) => match state.drop.poll_unpin(cx) {
                Poll::Ready(Ok(_)) => Ok(PollPull::Exhausted),
                Poll::Ready(Err(e)) => Err(e),
                Poll::Pending => Ok(PollPull::Pending),
            },
            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl Explainable for PhysicalDrop {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Drop")
    }
}
