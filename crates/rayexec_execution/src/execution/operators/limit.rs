use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::{database::DatabaseContext, proto::DatabaseProtoConv};
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::{
    sync::Arc,
    task::{Context, Waker},
};

use super::{
    ExecutableOperator, ExecutionStates, InputOutputStates, OperatorState, PartitionState,
    PollFinalize, PollPull, PollPush,
};

#[derive(Debug)]
pub struct LimitPartitionState {
    /// Remaining offset before we can actually start sending rows.
    remaining_offset: usize,

    /// Remaining number of rows before we stop sending batches.
    ///
    /// Initialized to the operator `limit`.
    remaining_count: usize,

    /// A buffered batch.
    buffer: Option<Batch>,

    /// Waker on pull side if no batch is ready.
    pull_waker: Option<Waker>,

    /// Waker on push side if this partition is already buffering an output
    /// batch.
    push_waker: Option<Waker>,

    /// If inputs are finished.
    finished: bool,
}

/// Operator for LIMIT and OFFSET clauses.
///
/// The provided `limit` and `offset` values work on a per-partition basis. A
/// global limit/offset should be done by using a single partition.
#[derive(Debug)]
pub struct PhysicalLimit {
    /// Number of rows to limit to.
    limit: usize,

    /// Offset to start limiting from.
    offset: Option<usize>,
}

impl PhysicalLimit {
    pub fn new(limit: usize, offset: Option<usize>) -> Self {
        PhysicalLimit { limit, offset }
    }
}

impl ExecutableOperator for PhysicalLimit {
    fn create_states(
        &self,
        _context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        let partitions = partitions[0];

        Ok(ExecutionStates {
            operator_state: Arc::new(OperatorState::None),
            partition_states: InputOutputStates::OneToOne {
                partition_states: (0..partitions)
                    .map(|_| {
                        PartitionState::Limit(LimitPartitionState {
                            remaining_count: self.limit,
                            remaining_offset: self.offset.unwrap_or(0),
                            buffer: None,
                            pull_waker: None,
                            push_waker: None,
                            finished: false,
                        })
                    })
                    .collect(),
            },
        })
    }

    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        let state = match partition_state {
            PartitionState::Limit(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        if state.buffer.is_some() {
            state.push_waker = Some(cx.waker().clone());
            return Ok(PollPush::Pending(batch));
        }

        let batch = if state.remaining_offset > 0 {
            // Offset greater than the number of rows in this batch. Discard the
            // batch, and keep asking for more input.
            if state.remaining_offset >= batch.num_rows() {
                state.remaining_offset -= batch.num_rows();
                return Ok(PollPush::NeedsMore);
            }

            // Otherwise we have to slice the batch at the offset point.
            let count = std::cmp::min(
                batch.num_rows() - state.remaining_offset,
                state.remaining_count,
            );

            let batch = batch.slice(state.remaining_offset, count);

            state.remaining_offset = 0;
            state.remaining_count -= batch.num_rows();
            batch
        } else if state.remaining_count < batch.num_rows() {
            // Remaining offset is 0, and input batch is has more rows than we
            // need, just slice to the right size.
            let batch = batch.slice(0, state.remaining_count);
            state.remaining_count = 0;
            batch
        } else {
            // Remaing offset is 0, and input batch has more rows than our
            // limit, so just use the batch as-is.
            state.remaining_count -= batch.num_rows();
            batch
        };

        state.buffer = Some(batch);
        if let Some(waker) = state.pull_waker.take() {
            waker.wake();
        }

        // We're done, no more inputs should arrive.
        if state.remaining_count == 0 {
            // When returning `Break`, we do not call `finalize_push`, and
            // instead the partition pipeline will immediately start to pull
            // from this operator.
            state.finished = true;
            Ok(PollPush::Break)
        } else {
            Ok(PollPush::Pushed)
        }
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        let state = match partition_state {
            PartitionState::Limit(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        state.finished = true;
        if let Some(waker) = state.pull_waker.take() {
            waker.wake();
        }

        Ok(PollFinalize::Finalized)
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        let state = match partition_state {
            PartitionState::Limit(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match state.buffer.take() {
            Some(batch) => Ok(PollPull::Computed(batch.into())),
            None => {
                if state.finished {
                    return Ok(PollPull::Exhausted);
                }
                state.pull_waker = Some(cx.waker().clone());
                if let Some(waker) = state.push_waker.take() {
                    waker.wake();
                }
                Ok(PollPull::Pending)
            }
        }
    }
}

impl Explainable for PhysicalLimit {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        let mut ent = ExplainEntry::new("Limit").with_value("limit", self.limit);
        if let Some(offset) = self.offset {
            ent = ent.with_value("offset", offset);
        }
        ent
    }
}

impl DatabaseProtoConv for PhysicalLimit {
    type ProtoType = rayexec_proto::generated::execution::PhysicalLimit;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            limit: self.limit as u64,
            offset: self.offset.map(|o| o as u64),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            limit: proto.limit as usize,
            offset: proto.offset.map(|o| o as usize),
        })
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::scalar::ScalarValue;

    use crate::execution::operators::test_util::{
        logical_value, make_i32_batch, test_database_context, unwrap_poll_pull_batch,
        TestWakerContext,
    };
    use std::sync::Arc;

    use super::*;

    fn create_states(operator: &PhysicalLimit, partitions: usize) -> Vec<PartitionState> {
        let context = test_database_context();
        let states = operator.create_states(&context, vec![partitions]).unwrap();

        match states.partition_states {
            InputOutputStates::OneToOne { partition_states } => partition_states,
            other => panic!("invalid states: {other:?}"),
        }
    }

    #[test]
    fn limit_single_partition() {
        let mut inputs = vec![
            make_i32_batch([1, 2, 3, 4]),
            make_i32_batch([5, 6, 7, 8, 9, 10]),
        ];

        let operator = Arc::new(PhysicalLimit::new(5, None));
        let operator_state = Arc::new(OperatorState::None);
        let mut partition_states = create_states(&operator, 1);

        // Try to pull before we have a batch ready.
        let pull_cx = TestWakerContext::new();
        let poll_pull = pull_cx
            .poll_pull(&operator, &mut partition_states[0], &operator_state)
            .unwrap();
        assert_eq!(PollPull::Pending, poll_pull);

        // Push our first batch.
        let push_cx = TestWakerContext::new();
        let poll_push = push_cx
            .poll_push(
                &operator,
                &mut partition_states[0],
                &operator_state,
                inputs.remove(0),
            )
            .unwrap();
        assert_eq!(PollPush::Pushed, poll_push);

        // Pull side should have been woken.
        assert_eq!(1, pull_cx.wake_count());
        let poll_pull = pull_cx
            .poll_pull(&operator, &mut partition_states[0], &operator_state)
            .unwrap();
        let output = unwrap_poll_pull_batch(poll_pull);
        let expected = make_i32_batch([1, 2, 3, 4]); // Matches the first batch pushed.
        assert_eq!(expected, output);

        // Push next batch
        let poll_push = push_cx
            .poll_push(
                &operator,
                &mut partition_states[0],
                &operator_state,
                inputs.remove(0),
            )
            .unwrap();
        assert_eq!(PollPush::Break, poll_push);

        // We did _not_ store a new pull waker, the current count for the pull
        // waker should still be one.
        //
        // This not being 1 would indicate a bug with not actually clearing the
        // waker once it's woken.
        assert_eq!(1, pull_cx.wake_count());

        // Get next batch, result should only contain the first element from the
        // second batch.
        let poll_pull = pull_cx
            .poll_pull(&operator, &mut partition_states[0], &operator_state)
            .unwrap();
        let output = unwrap_poll_pull_batch(poll_pull);

        assert_eq!(ScalarValue::Int32(5), logical_value(&output, 0, 0));
    }

    #[test]
    fn limit_offset_single_partition_first_batch_partial() {
        let mut inputs = vec![
            make_i32_batch([1, 2, 3, 4]),
            make_i32_batch([5, 6, 7, 8, 9, 10]),
        ];

        let operator = Arc::new(PhysicalLimit::new(5, Some(2)));
        let operator_state = Arc::new(OperatorState::None);
        let mut partition_states = create_states(&operator, 1);

        // Push our first batch, will be part of the output.
        let push_cx = TestWakerContext::new();
        let poll_push = push_cx
            .poll_push(
                &operator,
                &mut partition_states[0],
                &operator_state,
                inputs.remove(0),
            )
            .unwrap();
        assert_eq!(PollPush::Pushed, poll_push);

        let pull_cx = TestWakerContext::new();
        let poll_pull = pull_cx
            .poll_pull(&operator, &mut partition_states[0], &operator_state)
            .unwrap();
        let output = unwrap_poll_pull_batch(poll_pull);
        // First two elements skipped.
        assert_eq!(ScalarValue::Int32(3), logical_value(&output, 0, 0),);
        assert_eq!(ScalarValue::Int32(4), logical_value(&output, 0, 1),);

        // Push next batch
        let poll_push = push_cx
            .poll_push(
                &operator,
                &mut partition_states[0],
                &operator_state,
                inputs.remove(0),
            )
            .unwrap();
        assert_eq!(PollPush::Break, poll_push);

        // Pull part of next batch.
        let poll_pull = pull_cx
            .poll_pull(&operator, &mut partition_states[0], &operator_state)
            .unwrap();
        let output = unwrap_poll_pull_batch(poll_pull);

        assert_eq!(ScalarValue::Int32(5), logical_value(&output, 0, 0),);
        assert_eq!(ScalarValue::Int32(6), logical_value(&output, 0, 1),);
        assert_eq!(ScalarValue::Int32(7), logical_value(&output, 0, 2),);
    }

    #[test]
    fn limit_offset_single_partition_first_batch_skipped() {
        let mut inputs = vec![
            make_i32_batch([1, 2, 3, 4]),
            make_i32_batch([5, 6, 7, 8, 9, 10]),
        ];

        let operator = Arc::new(PhysicalLimit::new(2, Some(5)));
        let operator_state = Arc::new(OperatorState::None);
        let mut partition_states = create_states(&operator, 1);

        // Push our first batch, will be skipped. Operator will return
        // indicating it needs more input.
        let push_cx = TestWakerContext::new();
        let poll_push = push_cx
            .poll_push(
                &operator,
                &mut partition_states[0],
                &operator_state,
                inputs.remove(0),
            )
            .unwrap();
        assert_eq!(PollPush::NeedsMore, poll_push);

        // Keep pushing...
        let poll_push = push_cx
            .poll_push(
                &operator,
                &mut partition_states[0],
                &operator_state,
                inputs.remove(0),
            )
            .unwrap();
        assert_eq!(PollPush::Break, poll_push);

        let pull_cx = TestWakerContext::new();
        let poll_pull = pull_cx
            .poll_pull(&operator, &mut partition_states[0], &operator_state)
            .unwrap();
        let output = unwrap_poll_pull_batch(poll_pull);

        assert_eq!(ScalarValue::Int32(6), logical_value(&output, 0, 0),);
        assert_eq!(ScalarValue::Int32(7), logical_value(&output, 0, 1),);
    }

    #[test]
    fn limit_break_exhaust() {
        let mut inputs = vec![make_i32_batch([1, 2, 3, 4]), make_i32_batch([5, 6, 7, 8])];

        let operator = Arc::new(PhysicalLimit::new(2, None));
        let operator_state = Arc::new(OperatorState::None);
        let mut partition_states = create_states(&operator, 1);

        let push_cx = TestWakerContext::new();
        let poll_push = push_cx
            .poll_push(
                &operator,
                &mut partition_states[0],
                &operator_state,
                inputs.remove(0),
            )
            .unwrap();
        assert_eq!(PollPush::Break, poll_push);

        let pull_cx = TestWakerContext::new();
        let poll_pull = pull_cx
            .poll_pull(&operator, &mut partition_states[0], &operator_state)
            .unwrap();
        let _ = unwrap_poll_pull_batch(poll_pull);

        let poll_pull = pull_cx
            .poll_pull(&operator, &mut partition_states[0], &operator_state)
            .unwrap();
        assert_eq!(PollPull::Exhausted, poll_pull);
    }
}
