use std::task::Context;

use glaredb_error::Result;

use super::{BaseOperator, ExecutionProperties, PollPull, PullOperator};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SingleRowScanPartitionState {
    Emit,
    NoEmit,
}

/// An operator that emits a single row with no columns across all partitions.
#[derive(Debug)]
pub struct PhysicalSingleRow;

impl BaseOperator for PhysicalSingleRow {
    const OPERATOR_NAME: &str = "SingleRow";

    type OperatorState = ();

    fn create_operator_state(&self, _props: ExecutionProperties) -> Result<Self::OperatorState> {
        Ok(())
    }

    fn output_types(&self) -> &[DataType] {
        &[]
    }
}

impl PullOperator for PhysicalSingleRow {
    type PartitionPullState = SingleRowScanPartitionState;

    fn create_partition_pull_states(
        &self,
        _operator_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionPullState>> {
        debug_assert!(partitions >= 1);

        let mut states = vec![SingleRowScanPartitionState::Emit];
        states.resize(partitions, SingleRowScanPartitionState::NoEmit);

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
            SingleRowScanPartitionState::Emit => output.set_num_rows(1)?,
            SingleRowScanPartitionState::NoEmit => output.set_num_rows(0)?,
        }
        Ok(PollPull::Exhausted)
    }
}

impl Explainable for PhysicalSingleRow {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new(Self::OPERATOR_NAME)
    }
}
