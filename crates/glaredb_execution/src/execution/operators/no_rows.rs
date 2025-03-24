use std::task::Context;

use glaredb_error::Result;

use super::{BaseOperator, ExecutionProperties, PollPull, PullOperator};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

/// Operator that emits no rows, but knows its output types.
#[derive(Debug)]
pub struct PhysicalNoRows {
    pub(crate) datatypes: Vec<DataType>,
}

impl BaseOperator for PhysicalNoRows {
    type OperatorState = ();

    fn create_operator_state(&self, _props: ExecutionProperties) -> Result<Self::OperatorState> {
        Ok(())
    }

    fn output_types(&self) -> &[DataType] {
        &self.datatypes
    }
}

impl PullOperator for PhysicalNoRows {
    type PartitionPullState = ();

    fn create_partition_pull_states(
        &self,
        _operator_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionPullState>> {
        Ok(vec![(); partitions])
    }

    fn poll_pull(
        &self,
        _cx: &mut Context,
        _operator_state: &Self::OperatorState,
        _state: &mut Self::PartitionPullState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        output.set_num_rows(0)?;
        Ok(PollPull::Exhausted)
    }
}

impl Explainable for PhysicalNoRows {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("NoRows")
    }
}
