use std::task::Context;

use rayexec_error::Result;

use super::{BaseOperator, ExecutionProperties, PollPull, PullOperator};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmptyPartitionState {
    Emit,
    NoEmit,
}

/// A dummy operator that emits a single row across all partitions.
#[derive(Debug)]
pub struct PhysicalEmpty;

impl BaseOperator for PhysicalEmpty {
    type OperatorState = ();

    fn create_operator_state(&self, _props: ExecutionProperties) -> Result<Self::OperatorState> {
        Ok(())
    }

    fn output_types(&self) -> &[DataType] {
        &[]
    }
}

impl PullOperator for PhysicalEmpty {
    type PartitionPullState = EmptyPartitionState;

    fn create_partition_pull_states(
        &self,
        _operator_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionPullState>> {
        debug_assert!(partitions >= 1);

        let mut states = vec![EmptyPartitionState::Emit];
        states.resize(partitions, EmptyPartitionState::NoEmit);

        Ok(states)
    }

    fn poll_pull(
        &self,
        _cx: &mut Context,
        _operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPullState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        match state {
            EmptyPartitionState::Emit => output.set_num_rows(1)?,
            EmptyPartitionState::NoEmit => output.set_num_rows(0)?,
        }
        Ok(PollPull::Exhausted)
    }
}

impl Explainable for PhysicalEmpty {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Empty")
    }
}
