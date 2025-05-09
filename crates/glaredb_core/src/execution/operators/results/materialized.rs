use std::sync::Arc;
use std::task::Context;

use glaredb_error::Result;

use crate::arrays::batch::Batch;
use crate::arrays::collection::concurrent::{
    ColumnCollectionAppendState,
    ConcurrentColumnCollection,
};
use crate::arrays::datatype::DataType;
use crate::execution::operators::{
    BaseOperator,
    ExecutionProperties,
    PollFinalize,
    PollPush,
    PushOperator,
};
use crate::explain::explainable::{EntryBuilder, ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug)]
pub struct MaterializedResultsOperatorState {
    collection: Arc<ConcurrentColumnCollection>,
}

#[derive(Debug)]
pub struct MaterializedResultsPartitionState {
    append_state: ColumnCollectionAppendState,
}

#[derive(Debug)]
pub struct PhysicalMaterializedResults {
    pub(crate) collection: Arc<ConcurrentColumnCollection>,
}

impl BaseOperator for PhysicalMaterializedResults {
    const OPERATOR_NAME: &str = "MaterializedResults";

    type OperatorState = MaterializedResultsOperatorState;

    fn create_operator_state(&self, _props: ExecutionProperties) -> Result<Self::OperatorState> {
        Ok(MaterializedResultsOperatorState {
            collection: self.collection.clone(),
        })
    }

    fn output_types(&self) -> &[DataType] {
        self.collection.datatypes()
    }
}

impl PushOperator for PhysicalMaterializedResults {
    type PartitionPushState = MaterializedResultsPartitionState;

    fn create_partition_push_states(
        &self,
        operator_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionPushState>> {
        let states = (0..partitions)
            .map(|_| MaterializedResultsPartitionState {
                append_state: operator_state.collection.init_append_state(),
            })
            .collect();

        Ok(states)
    }

    fn poll_push(
        &self,
        _cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPushState,
        input: &mut Batch,
    ) -> Result<PollPush> {
        operator_state
            .collection
            .append_batch(&mut state.append_state, input)?;

        Ok(PollPush::NeedsMore)
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPushState,
    ) -> Result<PollFinalize> {
        operator_state.collection.flush(&mut state.append_state)?;

        Ok(PollFinalize::Finalized)
    }
}

impl Explainable for PhysicalMaterializedResults {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        EntryBuilder::new(Self::OPERATOR_NAME, conf).build()
    }
}
