use std::task::{Context, Waker};

use glaredb_error::{DbError, Result};
use parking_lot::Mutex;

use super::{ExecuteOperator, PollExecute};
use crate::arrays::batch::Batch;
use crate::arrays::collection::concurrent::{
    ColumnCollectionAppendState,
    ColumnCollectionScanState,
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
use crate::storage::projections::Projections;

#[derive(Debug)]
pub struct VerifyOperatorState {
    /// Data collected from the "left" plan.
    left: ConcurrentColumnCollection,
    /// Data collected from the "right" plan.
    right: ConcurrentColumnCollection,
    /// Projections used for the scan (all columns).
    projections: Projections,
    /// Batch size to use during verification.
    batch_size: usize,
    completion: Mutex<CompletionState>,
}

#[derive(Debug)]
struct CompletionState {
    /// Number of inputs we're waiting on from the push side.
    remaining_left_inputs: usize,
    /// Number of inputs we're waiting on from the execute side.
    remaining_right_inputs: usize,
    /// Waker for the execute partition that's responsible for
    execute_waker: Option<Waker>,
}

#[derive(Debug)]
pub struct VerifyPartitionPushState {
    append_state: ColumnCollectionAppendState,
}

#[derive(Debug)]
pub enum VerifyPartitionExecuteState {
    Appending {
        /// State used to append to the collection.
        append_state: ColumnCollectionAppendState,
    },
    Scanning {
        /// State used for scanning.
        ///
        /// Only a single partition will verify/scan.
        scan_state: ColumnCollectionScanState,
        /// If the left side has been completed.
        left_is_complete: bool,
        /// Did we verify yet?
        did_verify: bool,
    },
    /// Partition immediately exhausted.
    Exhausted,
}

/// Verifies that two inputs produce the same batches.
///
/// The "left" (push) and "right" (execute) sides are both collected in their
/// entirety. Once all rows are collected, we verify that both collection
/// contents are equal.
///
/// This requires that both sides have consistent ordering to pass verification.
///
/// Only a single execute partition will produce rows. This is done to ensure
/// insertion order into the collection is maintained in the output.
#[derive(Debug)]
pub struct PhysicalVerify {
    pub(crate) datatypes: Vec<DataType>,
}

impl PhysicalVerify {
    pub fn new(datatypes: impl IntoIterator<Item = DataType>) -> Self {
        PhysicalVerify {
            datatypes: datatypes.into_iter().collect(),
        }
    }
}

impl BaseOperator for PhysicalVerify {
    const OPERATOR_NAME: &str = "Verify";

    type OperatorState = VerifyOperatorState;

    fn create_operator_state(&self, props: ExecutionProperties) -> Result<Self::OperatorState> {
        let state = VerifyOperatorState {
            left: ConcurrentColumnCollection::new(self.datatypes.clone(), 4, props.batch_size),
            right: ConcurrentColumnCollection::new(self.datatypes.clone(), 4, props.batch_size),
            projections: Projections::new(0..self.datatypes.len()),
            batch_size: props.batch_size,
            completion: Mutex::new(CompletionState {
                remaining_left_inputs: 0,  // Set when creating push states.
                remaining_right_inputs: 0, // Set when creating execute states.
                execute_waker: None,
            }),
        };

        Ok(state)
    }

    fn output_types(&self) -> &[DataType] {
        &self.datatypes
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
        operator_state.completion.lock().remaining_left_inputs = partitions;

        let states = (0..partitions)
            .map(|_| VerifyPartitionPushState {
                append_state: operator_state.left.init_append_state(),
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
            .left
            .append_batch(&mut state.append_state, input)?;

        Ok(PollPush::NeedsMore)
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPushState,
    ) -> Result<PollFinalize> {
        operator_state.left.flush(&mut state.append_state)?;

        let mut completion = operator_state.completion.lock();
        completion.remaining_left_inputs -= 1;

        if completion.remaining_left_inputs == 0 {
            // We've collected everything for the left, wake up the execute side
            // if needed.
            if let Some(waker) = completion.execute_waker.take() {
                waker.wake();
            }
        }

        Ok(PollFinalize::Finalized)
    }
}

impl ExecuteOperator for PhysicalVerify {
    type PartitionExecuteState = VerifyPartitionExecuteState;

    fn create_partition_execute_states(
        &self,
        operator_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionExecuteState>> {
        operator_state.completion.lock().remaining_right_inputs = partitions;

        let states = (0..partitions)
            .map(|_| VerifyPartitionExecuteState::Appending {
                append_state: operator_state.right.init_append_state(),
            })
            .collect();

        Ok(states)
    }

    fn poll_execute(
        &self,
        cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionExecuteState,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute> {
        match state {
            VerifyPartitionExecuteState::Appending { append_state } => {
                // Just pushing to the collection.
                operator_state.right.append_batch(append_state, input)?;
                Ok(PollExecute::NeedsMore)
            }
            VerifyPartitionExecuteState::Scanning {
                scan_state,
                left_is_complete,
                did_verify,
            } => {
                if !*left_is_complete {
                    // Check to see if it actually is ready.
                    let mut completion = operator_state.completion.lock();
                    if completion.remaining_left_inputs == 0 {
                        // We're good.
                        *left_is_complete = true
                    } else {
                        // Come back later.
                        completion.execute_waker = Some(cx.waker().clone());
                        return Ok(PollExecute::Pending);
                    }
                }

                if !*did_verify {
                    verify_collections_eq(
                        &operator_state.left,
                        &operator_state.right,
                        operator_state.batch_size,
                    )?;
                    *did_verify = true;
                }

                let count =
                    operator_state
                        .right
                        .scan(&operator_state.projections, scan_state, output)?;
                // Note that unlike the materialize scan, we don't need to worry
                // about missed rows here since we can only begin scanning once all
                // rows have been collected.
                if count == 0 {
                    return Ok(PollExecute::Exhausted);
                } else {
                    return Ok(PollExecute::HasMore);
                }
            }
            VerifyPartitionExecuteState::Exhausted => Ok(PollExecute::Exhausted),
        }
    }

    fn poll_finalize_execute(
        &self,
        _cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionExecuteState,
    ) -> Result<PollFinalize> {
        match state {
            VerifyPartitionExecuteState::Appending { append_state } => {
                operator_state.right.flush(append_state)?
            }
            _ => return Err(DbError::new("Partition in invalid state")),
        }

        let mut completion = operator_state.completion.lock();
        completion.remaining_right_inputs -= 1;

        if completion.remaining_right_inputs == 0 {
            // We're the last partition to finalize for the execute side. We'll
            // be the ones to verify + scan.
            *state = VerifyPartitionExecuteState::Scanning {
                scan_state: operator_state.right.init_scan_state(),
                left_is_complete: completion.remaining_left_inputs == 0,
                did_verify: false,
            };

            Ok(PollFinalize::NeedsDrain)
        } else {
            // Other partitions still finalizing. This partition will never
            // produce anything else.
            *state = VerifyPartitionExecuteState::Exhausted;

            Ok(PollFinalize::Finalized)
        }
    }
}

impl Explainable for PhysicalVerify {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new(Self::OPERATOR_NAME)
    }
}
