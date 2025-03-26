use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use futures::Stream;
use glaredb_error::{DbError, Result};
use parking_lot::Mutex;

use crate::arrays::batch::Batch;
use crate::arrays::collection::concurrent::{
    ColumnCollectionAppendState,
    ConcurrentColumnCollection,
};
use crate::arrays::collection::equal::verify_collections_eq;
use crate::arrays::datatype::DataType;
use crate::execution::operators::{
    BaseOperator,
    ExecutionProperties,
    PollFinalize,
    PollPush,
    PushOperator,
};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug)]
pub struct VerifyOperatorState {
    batch_size: usize,
    inputs_remaining: Mutex<usize>,
}

#[derive(Debug)]
pub struct VerifyPartitionPushState {
    append_state: ColumnCollectionAppendState,
}

#[derive(Debug)]
pub(crate) struct VerifyState {
    /// Data collected from the "left" plan.
    left: ConcurrentColumnCollection,
    /// Data collected from the "right" plan.
    right: ConcurrentColumnCollection,
    completion: Mutex<CompletionState>,
}

#[derive(Debug)]
struct CompletionState {
    /// If left is completed.
    left_complete: bool,
    /// If right is completed.
    right_complete: bool,
    /// Waker for the stream. The stream cannot begin to emit batches prior to
    /// both plans completing.
    // TODO: Well it could... but collected both sides is easier to implement.
    stream_waker: Option<Waker>,
    query_error: Option<DbError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifySide {
    Left,
    Right,
}

#[derive(Debug)]
pub struct PhysicalVerify {
    pub(crate) side: VerifySide,
    pub(crate) state: Arc<VerifyState>,
}

impl BaseOperator for PhysicalVerify {
    const OPERATOR_NAME: &str = "Verify";

    type OperatorState = VerifyOperatorState;

    fn create_operator_state(&self, props: ExecutionProperties) -> Result<Self::OperatorState> {
        Ok(VerifyOperatorState {
            batch_size: props.batch_size,
            inputs_remaining: Mutex::new(0), // Set when creating push states.
        })
    }

    fn output_types(&self) -> &[DataType] {
        &[]
    }
}

impl PushOperator for PhysicalVerify {
    type PartitionPushState = VerifyPartitionPushState;

    fn create_partition_push_states(
        &self,
        operator_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionPushState>> {
        let mut inputs_remaining = operator_state.inputs_remaining.lock();
        *inputs_remaining = partitions;

        let collection = match self.side {
            VerifySide::Left => &self.state.left,
            VerifySide::Right => &self.state.right,
        };

        let states = (0..partitions)
            .map(|_| VerifyPartitionPushState {
                append_state: collection.init_append_state(),
            })
            .collect();

        Ok(states)
    }

    fn poll_push(
        &self,
        _cx: &mut Context,
        _operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPushState,
        input: &mut Batch,
    ) -> Result<PollPush> {
        let collection = match self.side {
            VerifySide::Left => &self.state.left,
            VerifySide::Right => &self.state.right,
        };

        collection.append_batch(&mut state.append_state, input)?;

        Ok(PollPush::NeedsMore)
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        operator_state: &Self::OperatorState,
        _state: &mut Self::PartitionPushState,
    ) -> Result<PollFinalize> {
        let mut inputs_remaining = operator_state.inputs_remaining.lock();
        *inputs_remaining -= 1;
        if *inputs_remaining != 0 {
            // Other partitions still pushing.
            return Ok(PollFinalize::Finalized);
        }
        std::mem::drop(inputs_remaining);

        // Last partition to finalize for this "side".

        let mut completion = self.state.completion.lock();
        match self.side {
            VerifySide::Left => completion.left_complete = true,
            VerifySide::Right => completion.right_complete = true,
        }

        if completion.left_complete && completion.right_complete {
            std::mem::drop(completion);

            // Verify the collections.
            verify_collections_eq(
                &self.state.left,
                &self.state.right,
                operator_state.batch_size,
            )?;
        }

        Ok(PollFinalize::Finalized)
    }
}

impl Explainable for PhysicalVerify {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new(Self::OPERATOR_NAME)
    }
}
