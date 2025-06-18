use std::task::Context;

use glaredb_error::Result;

use super::{BaseOperator, ExecutionProperties, PollPull, PullOperator};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::explain::explainable::{EntryBuilder, ExplainConfig, ExplainEntry, Explainable};
use crate::runtime::system::SystemRuntime;

/// Operator that emits no rows, but knows its output types.
#[derive(Debug)]
pub struct PhysicalNoRows {
    pub(crate) datatypes: Vec<DataType>,
}

impl<R: SystemRuntime> BaseOperator<R> for PhysicalNoRows {
    const OPERATOR_NAME: &str = "NoRows";

    type OperatorState = ();

    fn create_operator_state(&self, _props: ExecutionProperties) -> Result<Self::OperatorState> {
        Ok(())
    }

    fn output_types(&self) -> &[DataType] {
        &self.datatypes
    }
}

impl<R: SystemRuntime> PullOperator<R> for PhysicalNoRows {
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
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        EntryBuilder::new("NoRows", conf).build()
    }
}
