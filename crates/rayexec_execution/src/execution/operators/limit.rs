use crate::logical::explainable::{ExplainConfig, ExplainEntry, Explainable};
use rayexec_bullet::batch::Batch;
use rayexec_bullet::compute;
use rayexec_error::Result;
use std::task::{Context, Waker};

use super::{OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush};

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

    /// Create states for this operator.
    ///
    /// Limit has no global states, only partition-local states.
    pub fn create_states(&self, partitions: usize) -> Vec<LimitPartitionState> {
        (0..partitions)
            .map(|_| LimitPartitionState {
                remaining_count: self.limit,
                remaining_offset: self.offset.unwrap_or(0),
                buffer: None,
                pull_waker: None,
                push_waker: None,
                finished: false,
            })
            .collect()
    }
}

impl PhysicalOperator for PhysicalLimit {
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

            let cols = batch
                .columns()
                .iter()
                .map(|arr| compute::slice::slice(arr.as_ref(), state.remaining_offset, count))
                .collect::<Result<Vec<_>>>()?;

            let batch = Batch::try_new(cols)?;
            state.remaining_offset = 0;
            state.remaining_count -= batch.num_rows();
            batch
        } else if state.remaining_count < batch.num_rows() {
            // Remaining offset is 0, and input batch is has more rows than we
            // need, just slice to the right size.
            let cols = batch
                .columns()
                .iter()
                .map(|arr| compute::slice::slice(arr.as_ref(), 0, state.remaining_count))
                .collect::<Result<Vec<_>>>()?;
            let batch = Batch::try_new(cols)?;
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

    fn finalize_push(
        &self,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<()> {
        let state = match partition_state {
            PartitionState::Limit(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        state.finished = true;
        if let Some(waker) = state.pull_waker.take() {
            waker.wake();
        }

        Ok(())
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
            Some(batch) => Ok(PollPull::Batch(batch)),
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

#[cfg(test)]
mod tests {
    use crate::execution::operators::test_util::{
        make_i32_batch, unwrap_poll_pull_batch, TestContext,
    };
    use std::sync::Arc;

    use super::*;

    fn create_states(operator: &PhysicalLimit, partitions: usize) -> Vec<PartitionState> {
        operator
            .create_states(partitions)
            .into_iter()
            .map(PartitionState::Limit)
            .collect()
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
        let pull_cx = TestContext::new();
        let poll_pull = pull_cx
            .poll_pull(&operator, &mut partition_states[0], &operator_state)
            .unwrap();
        assert_eq!(PollPull::Pending, poll_pull);

        // Push our first batch.
        let push_cx = TestContext::new();
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
        let expected = make_i32_batch([5]);
        assert_eq!(expected, output);
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
        let push_cx = TestContext::new();
        let poll_push = push_cx
            .poll_push(
                &operator,
                &mut partition_states[0],
                &operator_state,
                inputs.remove(0),
            )
            .unwrap();
        assert_eq!(PollPush::Pushed, poll_push);

        let pull_cx = TestContext::new();
        let poll_pull = pull_cx
            .poll_pull(&operator, &mut partition_states[0], &operator_state)
            .unwrap();
        let output = unwrap_poll_pull_batch(poll_pull);
        let expected = make_i32_batch([3, 4]); // First two elements skipped.
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

        // Pull part of next batch.
        let poll_pull = pull_cx
            .poll_pull(&operator, &mut partition_states[0], &operator_state)
            .unwrap();
        let output = unwrap_poll_pull_batch(poll_pull);
        let expected = make_i32_batch([5, 6, 7]);
        assert_eq!(expected, output);
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
        let push_cx = TestContext::new();
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

        let pull_cx = TestContext::new();
        let poll_pull = pull_cx
            .poll_pull(&operator, &mut partition_states[0], &operator_state)
            .unwrap();
        let output = unwrap_poll_pull_batch(poll_pull);
        let expected = make_i32_batch([6, 7]);
        assert_eq!(expected, output);
    }

    #[test]
    fn limit_break_exhaust() {
        let mut inputs = vec![make_i32_batch([1, 2, 3, 4]), make_i32_batch([5, 6, 7, 8])];

        let operator = Arc::new(PhysicalLimit::new(2, None));
        let operator_state = Arc::new(OperatorState::None);
        let mut partition_states = create_states(&operator, 1);

        let push_cx = TestContext::new();
        let poll_push = push_cx
            .poll_push(
                &operator,
                &mut partition_states[0],
                &operator_state,
                inputs.remove(0),
            )
            .unwrap();
        assert_eq!(PollPush::Break, poll_push);

        let pull_cx = TestContext::new();
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

impl Explainable for PhysicalLimit {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        let mut ent = ExplainEntry::new("Limit").with_value("limit", self.limit);
        if let Some(offset) = self.offset {
            ent = ent.with_value("offset", offset);
        }
        ent
    }
}
