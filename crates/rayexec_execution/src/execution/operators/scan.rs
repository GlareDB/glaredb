use crate::{
    database::{catalog::CatalogTx, entry::TableEntry, table::DataTableScan, DatabaseContext},
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
};
use futures::{future::BoxFuture, FutureExt};
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::{fmt, task::Poll};
use std::{sync::Arc, task::Context};

use super::{
    util::futures::make_static, ExecutionStates, InputOutputStates, OperatorState, PartitionState,
    PhysicalOperator, PollFinalize, PollPull, PollPush,
};

pub struct ScanPartitionState {
    scan: Box<dyn DataTableScan>,
    /// In progress pull we're working on.
    future: Option<BoxFuture<'static, Result<Option<Batch>>>>,
}

impl fmt::Debug for ScanPartitionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScanPartitionState").finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct PhysicalScan {
    catalog: String,
    schema: String,
    table: TableEntry,
}

impl PhysicalScan {
    pub fn new(catalog: impl Into<String>, schema: impl Into<String>, table: TableEntry) -> Self {
        PhysicalScan {
            catalog: catalog.into(),
            schema: schema.into(),
            table,
        }
    }
}

impl PhysicalOperator for PhysicalScan {
    fn create_states(
        &self,
        context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        // TODO: Placeholder for now. Transaction info should probably go on the
        // operator.
        let tx = CatalogTx::new();

        let data_table =
            context
                .get_catalog(&self.catalog)?
                .data_table(&tx, &self.schema, &self.table)?;

        // TODO: Pushdown projections, filters
        let scans = data_table.scan(partitions[0])?;

        let states = scans
            .into_iter()
            .map(|scan| PartitionState::Scan(ScanPartitionState { scan, future: None }))
            .collect();

        Ok(ExecutionStates {
            operator_state: Arc::new(OperatorState::None),
            partition_states: InputOutputStates::OneToOne {
                partition_states: states,
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
        Err(RayexecError::new("Cannot push to physical scan"))
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        Err(RayexecError::new("Cannot push to physical scan"))
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        match partition_state {
            PartitionState::Scan(state) => {
                if let Some(future) = &mut state.future {
                    match future.poll_unpin(cx) {
                        Poll::Ready(Ok(Some(batch))) => {
                            state.future = None; // Future complete, next pull with create a new one.
                            return Ok(PollPull::Batch(batch));
                        }
                        Poll::Ready(Ok(None)) => return Ok(PollPull::Exhausted),
                        Poll::Ready(Err(e)) => return Err(e),
                        Poll::Pending => return Ok(PollPull::Pending),
                    }
                }

                let mut future = state.scan.pull();
                match future.poll_unpin(cx) {
                    Poll::Ready(Ok(Some(batch))) => Ok(PollPull::Batch(batch)),
                    Poll::Ready(Ok(None)) => Ok(PollPull::Exhausted),
                    Poll::Ready(Err(e)) => Err(e),
                    Poll::Pending => {
                        // SAFETY: Scan lives on the partition state and
                        // outlives this future.
                        state.future = Some(unsafe { make_static(future) });
                        Ok(PollPull::Pending)
                    }
                }
            }

            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl Explainable for PhysicalScan {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Scan").with_value("table", &self.table.name)
    }
}
