use std::sync::Arc;
use std::task::Context;

use rayexec_error::Result;

use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::catalog::create::CreateSchemaInfo;
use crate::catalog::memory::MemoryCatalog;
use crate::catalog::Catalog;
use crate::execution::operators::{BaseOperator, ExecutionProperties, PollPull, PullOperator};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateSchemaPartitionState {
    Create,
    Skip,
}

#[derive(Debug)]
pub struct PhysicalCreateSchema {
    pub(crate) catalog: Arc<MemoryCatalog>,
    pub(crate) info: CreateSchemaInfo,
}

impl BaseOperator for PhysicalCreateSchema {
    type OperatorState = ();

    fn create_operator_state(&self, _props: ExecutionProperties) -> Result<Self::OperatorState> {
        Ok(())
    }

    fn output_types(&self) -> &[DataType] {
        &[]
    }
}

impl PullOperator for PhysicalCreateSchema {
    type PartitionPullState = CreateSchemaPartitionState;

    fn create_partition_pull_states(
        &self,
        _operator_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionPullState>> {
        debug_assert!(partitions >= 1);
        let mut states = vec![CreateSchemaPartitionState::Create];
        states.resize(partitions, CreateSchemaPartitionState::Skip);

        Ok(states)
    }

    fn poll_pull(
        &self,
        _cx: &mut Context,
        _operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPullState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        if *state == CreateSchemaPartitionState::Create {
            self.catalog.create_schema(&self.info)?;
        }
        output.set_num_rows(0)?;
        Ok(PollPull::Exhausted)
    }
}

impl Explainable for PhysicalCreateSchema {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CreateSchema")
    }
}
