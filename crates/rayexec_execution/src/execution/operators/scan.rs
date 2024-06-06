use crate::{
    database::{catalog::CatalogTx, entry::TableEntry, table::DataTableScan, DatabaseContext},
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
};
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::task::Context;

use super::{OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush};

#[derive(Debug)]
pub struct ScanPartitionState {
    scan: Box<dyn DataTableScan>,
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

    pub fn try_create_states(
        &self,
        context: &DatabaseContext,
        num_partitions: usize,
    ) -> Result<Vec<ScanPartitionState>> {
        // TODO: Placeholder for now. Transaction info should probably go on the
        // operator.
        let tx = CatalogTx::new();

        let data_table =
            context
                .get_catalog(&self.catalog)?
                .data_table(&tx, &self.schema, &self.table)?;

        // TODO: Pushdown projections, filters
        let scans = data_table.scan(num_partitions)?;

        let states = scans
            .into_iter()
            .map(|scan| ScanPartitionState { scan })
            .collect();

        Ok(states)
    }
}

impl PhysicalOperator for PhysicalScan {
    fn poll_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        _batch: Batch,
    ) -> Result<PollPush> {
        Err(RayexecError::new("Cannot push to physical scan"))
    }

    fn finalize_push(
        &self,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<()> {
        Err(RayexecError::new("Cannot push to physical scan"))
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        match partition_state {
            PartitionState::Scan(state) => state.scan.poll_pull(cx),
            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl Explainable for PhysicalScan {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Scan").with_value("table", &self.table.name)
    }
}
