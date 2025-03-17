use std::sync::Arc;
use std::task::Context;

use glaredb_error::Result;

use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::catalog::Catalog;
use crate::catalog::drop::DropInfo;
use crate::catalog::entry::CatalogEntryInner;
use crate::catalog::memory::MemoryCatalog;
use crate::execution::operators::{BaseOperator, ExecutionProperties, PollPull, PullOperator};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::storage::storage_manager::StorageManager;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropPartitionState {
    Drop,
    Skip,
}

#[derive(Debug)]
pub struct PhysicalDrop {
    pub(crate) storage: Arc<StorageManager>,
    pub(crate) catalog: Arc<MemoryCatalog>,
    pub(crate) info: DropInfo,
}

impl BaseOperator for PhysicalDrop {
    type OperatorState = ();

    fn create_operator_state(&self, _props: ExecutionProperties) -> Result<Self::OperatorState> {
        Ok(())
    }

    fn output_types(&self) -> &[DataType] {
        &[]
    }
}

impl PullOperator for PhysicalDrop {
    type PartitionPullState = DropPartitionState;

    fn create_partition_pull_states(
        &self,
        _operator_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionPullState>> {
        debug_assert!(partitions >= 1);
        let mut states = vec![DropPartitionState::Drop];
        states.resize(partitions, DropPartitionState::Skip);

        Ok(states)
    }

    fn poll_pull(
        &self,
        _cx: &mut Context,
        _operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPullState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        if *state == DropPartitionState::Drop {
            // TODO: Trasactions and stuff.
            let ent = self.catalog.drop_entry(&self.info)?;

            if let Some(ent) = ent {
                if let CatalogEntryInner::Table(table) = &ent.entry {
                    // TODO: Drop on commit instead of here.
                    self.storage.drop_table(table.storage_id)?;
                }
            }
        }

        output.set_num_rows(0)?;
        Ok(PollPull::Exhausted)
    }
}

impl Explainable for PhysicalDrop {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Drop")
    }
}
