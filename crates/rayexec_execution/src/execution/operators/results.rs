use std::sync::Arc;
use std::task::Context;

use rayexec_error::Result;

use super::{BaseOperator, ExecutionProperties, PollFinalize, PollPush, PushOperator};
use crate::arrays::batch::Batch;
use crate::arrays::collection::concurrent::{
    ColumnCollectionAppendState,
    ConcurrentColumnCollection,
};
use crate::arrays::datatype::DataType;
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug)]
pub struct ResultsOperatorState {
    collection: Arc<ConcurrentColumnCollection>,
}

#[derive(Debug)]
pub struct ResultsPartitionState {
    append_state: ColumnCollectionAppendState,
}

#[derive(Debug)]
pub struct PhysicalResults {
    pub(crate) collection: Arc<ConcurrentColumnCollection>,
}

impl BaseOperator for PhysicalResults {
    type OperatorState = ResultsOperatorState;

    fn create_operator_state(
        &self,
        _context: &DatabaseContext,
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(ResultsOperatorState {
            collection: self.collection.clone(),
        })
    }

    fn output_types(&self) -> &[DataType] {
        self.collection.datatypes()
    }
}

impl PushOperator for PhysicalResults {
    type PartitionPushState = ResultsPartitionState;

    fn create_partition_push_states(
        &self,
        operator_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionPushState>> {
        let states = (0..partitions)
            .map(|_| ResultsPartitionState {
                append_state: operator_state.collection.init_append_state(),
            })
            .collect();

        Ok(states)
    }

    fn poll_push(
        &self,
        _cx: &mut Context,
        state: &mut Self::PartitionPushState,
        operator_state: &Self::OperatorState,
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
        state: &mut Self::PartitionPushState,
        operator_state: &Self::OperatorState,
    ) -> Result<PollFinalize> {
        operator_state.collection.flush(&mut state.append_state)?;

        Ok(PollFinalize::Finalized)
    }
}

impl Explainable for PhysicalResults {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Results")
    }
}
