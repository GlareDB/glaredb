use std::sync::Arc;
use std::task::Context;

use glaredb_error::Result;

use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::catalog::create::CreateTableInfo;
use crate::catalog::memory::MemorySchema;
use crate::catalog::Schema;
use crate::config::session::DEFAULT_BATCH_SIZE;
use crate::execution::operators::{BaseOperator, ExecutionProperties, PollPull, PullOperator};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::storage::datatable::DataTable;
use crate::storage::storage_manager::StorageManager;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateTablePartitionState {
    Create,
    Skip,
}

#[derive(Debug)]
pub struct PhysicalCreateTable {
    pub(crate) storage: Arc<StorageManager>,
    pub(crate) schema: Arc<MemorySchema>,
    pub(crate) info: CreateTableInfo,
}

impl BaseOperator for PhysicalCreateTable {
    type OperatorState = ();

    fn create_operator_state(&self, _props: ExecutionProperties) -> Result<Self::OperatorState> {
        Ok(())
    }

    fn output_types(&self) -> &[DataType] {
        &[]
    }
}

impl PullOperator for PhysicalCreateTable {
    type PartitionPullState = CreateTablePartitionState;

    fn create_partition_pull_states(
        &self,
        _operator_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionPullState>> {
        debug_assert!(partitions >= 1);
        let mut states = vec![CreateTablePartitionState::Create];
        states.resize(partitions, CreateTablePartitionState::Skip);

        Ok(states)
    }

    fn poll_pull(
        &self,
        _cx: &mut Context,
        _operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPullState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        if *state == CreateTablePartitionState::Create {
            // TODO: How do we want to configure datatables?
            let datatable = DataTable::new(
                self.info.columns.iter().map(|f| f.datatype.clone()),
                16,
                DEFAULT_BATCH_SIZE,
            );
            let storage_id = self.storage.insert_table(Arc::new(datatable))?;
            self.schema.create_table(&self.info, storage_id)?;
        }
        output.set_num_rows(0)?;
        Ok(PollPull::Exhausted)
    }
}

impl Explainable for PhysicalCreateTable {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CreateTable")
    }
}
