use std::task::Context;

use glaredb_error::Result;

use super::{BaseOperator, ExecutionProperties, PollPull, PullOperator};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::explain::explainable::{EntryBuilder, ExplainConfig, ExplainEntry, Explainable};
use crate::functions::table::{
    AnyTableOperatorState,
    AnyTablePartitionState,
    PlannedTableFunction,
};
use crate::storage::projections::Projections;
use crate::storage::scan_filter::PhysicalScanFilter;

#[derive(Debug)]
pub struct ScanOperatorState {
    state: AnyTableOperatorState,
}

#[derive(Debug)]
pub struct ScanPartitionState {
    state: AnyTablePartitionState,
    reset_batch: bool,
}

#[derive(Debug)]
pub struct PhysicalScan {
    /// Projections for both the "data" and "metadata" columns.
    pub(crate) projections: Projections,
    /// Filters for the "data" columns.
    pub(crate) filters: Vec<PhysicalScanFilter>,
    /// Output types of both "data" and "metadata" columns.
    ///
    /// [data, metadata]
    pub(crate) output_types: Vec<DataType>,
    pub(crate) function: PlannedTableFunction,
}

impl PhysicalScan {
    pub fn new(
        projections: Projections,
        filters: Vec<PhysicalScanFilter>,
        function: PlannedTableFunction,
    ) -> Self {
        let num_cols = function.bind_state.data_schema.fields.len()
            + function
                .bind_state
                .meta_schema
                .as_ref()
                .map(|s| s.fields.len())
                .unwrap_or(0);
        let mut output_types = Vec::with_capacity(num_cols);

        // Push normal data column types.
        output_types.extend(
            projections
                .data_indices()
                .iter()
                .map(|&idx| function.bind_state.data_schema.fields[idx].datatype.clone()),
        );

        // Push metadata column types if we have it.
        if let Some(meta_schema) = &function.bind_state.meta_schema {
            output_types.extend(
                projections
                    .meta_indices()
                    .iter()
                    .map(|&idx| meta_schema.fields[idx].datatype.clone()),
            );
        }

        PhysicalScan {
            projections,
            filters,
            output_types,
            function,
        }
    }
}

impl BaseOperator for PhysicalScan {
    const OPERATOR_NAME: &str = "Scan";

    type OperatorState = ScanOperatorState;

    fn create_operator_state(&self, props: ExecutionProperties) -> Result<Self::OperatorState> {
        let op_state = unsafe {
            self.function.raw.call_create_pull_operator_state(
                &self.function.bind_state,
                self.projections.clone(),
                &self.filters,
                props,
            )?
        };

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
        let states = unsafe {
            self.function.raw.call_create_pull_partition_states(
                &self.function.bind_state,
                &operator_state.state,
                props,
                partitions,
            )?
        };
        let states = states
            .into_iter()
            .map(|state| ScanPartitionState {
                state,
                reset_batch: true,
            })
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
        if state.reset_batch {
            output.reset_for_write()?;
            state.reset_batch = false;
        }

        let poll = unsafe {
            self.function.raw.call_poll_pull(
                cx,
                &self.function.bind_state,
                &operator_state.state,
                &mut state.state,
                output,
            )?
        };

        // We want to keep the batch state the same for Pending since that's
        // logically re-executing on the same state. Only trigger a reset for
        // HasMore.
        if poll == PollPull::HasMore {
            state.reset_batch = true;
        }

        Ok(poll)
    }
}

impl Explainable for PhysicalScan {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        EntryBuilder::new(Self::OPERATOR_NAME, conf)
            .with_value("source", self.function.name)
            .build()
    }
}
