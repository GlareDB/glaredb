use std::task::Context;

use glaredb_error::Result;

use super::{BaseOperator, ExecutionProperties, PollPull, PullOperator};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::functions::table::{
    AnyTableOperatorState,
    AnyTablePartitionState,
    PlannedTableFunction,
};
use crate::storage::projections::Projections;

#[derive(Debug)]
pub struct ScanOperatorState {
    state: AnyTableOperatorState,
}

#[derive(Debug)]
pub struct ScanPartitionState {
    state: AnyTablePartitionState,
}

#[derive(Debug)]
pub struct PhysicalScan {
    pub(crate) projections: Projections,
    pub(crate) output_types: Vec<DataType>,
    pub(crate) function: PlannedTableFunction,
}

impl PhysicalScan {
    pub fn new(projections: Projections, function: PlannedTableFunction) -> Self {
        let output_types = projections
            .indices()
            .iter()
            .map(|&idx| function.bind_state.schema.fields[idx].datatype.clone())
            .collect();

        PhysicalScan {
            projections,
            output_types,
            function,
        }
    }
}

impl BaseOperator for PhysicalScan {
    const OPERATOR_NAME: &str = "Scan";

    type OperatorState = ScanOperatorState;

    fn create_operator_state(&self, props: ExecutionProperties) -> Result<Self::OperatorState> {
        let op_state = self.function.raw.call_create_pull_operator_state(
            &self.function.bind_state,
            &self.projections,
            props,
        )?;

        Ok(ScanOperatorState { state: op_state })
    }

    fn output_types(&self) -> &[DataType] {
        &self.output_types
    }
}

impl PullOperator for PhysicalScan {
    type PartitionPullState = ScanPartitionState;

    fn create_partition_pull_states(
        &self,
        operator_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionPullState>> {
        let states = self.function.raw.call_create_pull_partition_states(
            &operator_state.state,
            props,
            partitions,
        )?;
        let states = states
            .into_iter()
            .map(|state| ScanPartitionState { state })
            .collect();

        Ok(states)
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPullState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        output.reset_for_write()?;
        self.function
            .raw
            .call_poll_pull(cx, &operator_state.state, &mut state.state, output)
    }
}

impl Explainable for PhysicalScan {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new(Self::OPERATOR_NAME).with_value("source", self.function.name)
    }
}
