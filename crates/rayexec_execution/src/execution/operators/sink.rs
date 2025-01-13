use std::fmt::{self, Debug};
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use futures::FutureExt;
use parking_lot::Mutex;
use rayexec_error::{OptionExt, RayexecError, Result};

use super::util::futures::make_static;
use super::{
    ExecutableOperator,
    ExecuteInOutState,
    OperatorState,
    PartitionAndOperatorStates,
    PartitionState,
    PollExecute,
    PollFinalize,
};
use crate::arrays::batch::Batch;
use crate::arrays::scalar::ScalarValue;
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

/// Operation for sending batches somewhere.
pub trait SinkOperation: Debug + Send + Sync + Explainable {
    /// Create an exact number of partition sinks for the query.
    ///
    /// This is guaranteed to only be called once during pipeline execution.
    fn create_partition_sinks(
        &self,
        context: &DatabaseContext,
        num_sinks: usize,
    ) -> Result<Vec<Box<dyn PartitionSink>>>;

    /// Return an optional partitioning requirement for this sink.
    ///
    /// Called during executable pipeline planning.
    fn partition_requirement(&self) -> Option<usize>;
}

impl SinkOperation for Box<dyn SinkOperation> {
    fn create_partition_sinks(
        &self,
        context: &DatabaseContext,
        num_sinks: usize,
    ) -> Result<Vec<Box<dyn PartitionSink>>> {
        self.as_ref().create_partition_sinks(context, num_sinks)
    }

    fn partition_requirement(&self) -> Option<usize> {
        self.as_ref().partition_requirement()
    }
}

impl Explainable for Box<dyn SinkOperation> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.as_ref().explain_entry(conf)
    }
}

pub trait PartitionSink: Debug + Send {
    /// Push a batch to the sink.
    ///
    /// Batches are pushed in the order they're received in.
    fn push2(&mut self, batch: Batch) -> BoxFuture<'_, Result<()>> {
        unimplemented!()
    }

    fn push(&mut self, batch: &mut Batch) -> BoxFuture<'_, Result<()>> {
        unimplemented!()
    }

    /// Finalize the sink.
    ///
    /// Called once only after all batches have been pushed. If there's any
    /// pending work that needs to happen (flushing), it should happen here.
    /// Once this returns, the sink is complete.
    fn finalize(&mut self) -> BoxFuture<'_, Result<()>>;
}

pub enum SinkPartitionState {
    Writing {
        inner: Option<SinkInnerPartitionState>,
        /// Future we're working on for a pending write.
        future: Option<BoxFuture<'static, Result<()>>>,
    },
    Finalizing {
        inner: Option<SinkInnerPartitionState>,
        /// Future we're working on for a pending finalize. This should never be
        /// None, as if there's nothing for us to do, the state should be in
        /// `Finished`.
        future: BoxFuture<'static, Result<()>>,
    },
    Finished,
}

impl fmt::Debug for SinkPartitionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SinkPartitionState").finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct SinkInnerPartitionState {
    sink: Box<dyn PartitionSink>,
    current_row_count: usize,
}

#[derive(Debug)]
pub struct SinkOperatorState {
    inner: Mutex<SinkOperatorStateInner>,
}

#[derive(Debug)]
struct SinkOperatorStateInner {
    /// Row count from all partitions.
    global_row_count: usize,
    /// Number of partitions still writing to the sink.
    ///
    /// The last partition to complete should write out the global row count.
    partitions_remaining: usize,
}

/// An operator that writes batches to a partition sink.
///
/// The output batch for this operator is a single column containing the number
/// of rows passed through this operator.
///
/// Insert, CopyTo, CreateTable all use this. CreateTable uses this to enable
/// CTAS semantics easily.
#[derive(Debug)]
pub struct SinkOperator<S: SinkOperation> {
    pub(crate) sink: S,
}

impl<S: SinkOperation> SinkOperator<S> {
    pub fn new(sink: S) -> Self {
        SinkOperator { sink }
    }
}

impl<S: SinkOperation> ExecutableOperator for SinkOperator<S> {
    fn create_states(
        &self,
        context: &DatabaseContext,
        _batch_size: usize,
        partitions: usize,
    ) -> Result<PartitionAndOperatorStates> {
        let states: Vec<_> = self
            .sink
            .create_partition_sinks(context, partitions)?
            .into_iter()
            .map(|sink| {
                PartitionState::Sink(SinkPartitionState::Writing {
                    inner: Some(SinkInnerPartitionState {
                        sink,
                        current_row_count: 0,
                    }),
                    future: None,
                })
            })
            .collect();

        let operator_state = OperatorState::Sink(SinkOperatorState {
            inner: Mutex::new(SinkOperatorStateInner {
                global_row_count: 0,
                partitions_remaining: partitions,
            }),
        });

        Ok(PartitionAndOperatorStates::Branchless {
            operator_state,
            partition_states: states,
        })
    }

    fn poll_execute(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        inout: ExecuteInOutState,
    ) -> Result<PollExecute> {
        match partition_state {
            PartitionState::Sink(state) => match state {
                SinkPartitionState::Writing { inner, future } => {
                    if let Some(curr_future) = future {
                        match curr_future.poll_unpin(cx) {
                            Poll::Ready(Ok(_)) => {
                                // Future complete, unset and continue on.
                                //
                                // Unsetting is required here to avoid polling a
                                // completed future after returning early.
                                *future = None;
                            }
                            Poll::Ready(Err(e)) => return Err(e),
                            Poll::Pending => return Ok(PollExecute::Pending),
                        }
                    }

                    let input = inout.input.required("input batch required")?;

                    let inner = inner.as_mut().unwrap();
                    inner.current_row_count += input.num_rows();

                    let mut push_future = inner.sink.push(input);
                    match push_future.poll_unpin(cx) {
                        Poll::Ready(Ok(_)) => {
                            // Future completed, need more batches.
                            Ok(PollExecute::NeedsMore)
                        }
                        Poll::Ready(Err(e)) => Err(e),
                        Poll::Pending => {
                            // We successfully pushed.

                            // SAFETY: Lifetime of the Sink (on the
                            // partition state) outlives the future.
                            *future = Some(unsafe { make_static(push_future) });

                            Ok(PollExecute::Pending)
                        }
                    }
                }
                SinkPartitionState::Finished => {
                    // This should only be called by one partition. Determined
                    // by which partition gets 'NeedsDrain' from finalize.

                    // TODO: Should probably have all partitions go through this
                    // path, with all but one just setting the output batch to 0
                    // rows.

                    let op_state = match operator_state {
                        OperatorState::Sink(state) => state.inner.lock(),
                        other => panic!("invalid operator state: {other:?}"),
                    };

                    let output = inout.output.required("output batch required")?;
                    output.arrays[0]
                        .set_value(0, &ScalarValue::Int64(op_state.global_row_count as i64))?;

                    output.set_num_rows(1)?;

                    Ok(PollExecute::Exhausted)
                }
                other => Err(RayexecError::new(format!(
                    "Sink operator in wrong state: {other:?}"
                ))),
            },
            other => panic!("invalid partition state: {other:?}"),
        }
    }

    fn poll_finalize(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        match partition_state {
            PartitionState::Sink(state) => match state {
                SinkPartitionState::Writing { inner, future } => {
                    if future.is_some() {
                        return Err(RayexecError::new(
                            "Attempted to finalize sink with in-progress write",
                        ));
                    }

                    let mut inner = inner.take().unwrap();
                    let mut finalize_future = inner.sink.finalize();

                    match finalize_future.poll_unpin(cx) {
                        Poll::Ready(Ok(_)) => {
                            // TODO: This is duplicated iwth the below `Finalizing` match arm.

                            // We're done.
                            let mut op_state = match operator_state {
                                OperatorState::Sink(state) => state.inner.lock(),
                                other => panic!("invalid operator state: {other:?}"),
                            };

                            op_state.global_row_count += inner.current_row_count;
                            op_state.partitions_remaining -= 1;

                            *state = SinkPartitionState::Finished;

                            if op_state.partitions_remaining == 0 {
                                // This partition will drain row count.
                                Ok(PollFinalize::NeedsDrain)
                            } else {
                                // This partition drains nothing, still waiting
                                // on other partitions to complete.
                                Ok(PollFinalize::Finalized)
                            }
                        }
                        Poll::Ready(Err(e)) => Err(e),
                        Poll::Pending => {
                            // Waiting...

                            // SAFETY: Lifetime of sink outlives this future.
                            let future = unsafe { make_static(finalize_future) };

                            *state = SinkPartitionState::Finalizing {
                                inner: Some(inner),
                                future,
                            };

                            Ok(PollFinalize::Pending)
                        }
                    }
                }
                SinkPartitionState::Finalizing { inner, future } => match future.poll_unpin(cx) {
                    Poll::Ready(Ok(_)) => {
                        let mut op_state = match operator_state {
                            OperatorState::Sink(state) => state.inner.lock(),
                            other => panic!("invalid operator state: {other:?}"),
                        };

                        op_state.global_row_count += inner.as_ref().unwrap().current_row_count;
                        op_state.partitions_remaining -= 1;

                        *state = SinkPartitionState::Finished;

                        if op_state.partitions_remaining == 0 {
                            // This partition will drain row count.
                            Ok(PollFinalize::NeedsDrain)
                        } else {
                            // This partition drains nothing, still waiting
                            // on other partitions to complete.
                            Ok(PollFinalize::Finalized)
                        }
                    }
                    Poll::Ready(Err(e)) => Err(e),
                    Poll::Pending => Ok(PollFinalize::Pending),
                },
                other => Err(RayexecError::new(format!(
                    "CopyTo operator in wrong state: {other:?}"
                ))),
            },
            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl<S: SinkOperation> Explainable for SinkOperator<S> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.sink.explain_entry(conf)
    }
}

#[cfg(test)]
mod tests {
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::Array;
    use crate::arrays::datatype::DataType;
    use crate::arrays::testutil::assert_batches_eq;
    use crate::execution::operators::testutil::{test_database_context, OperatorWrapper};

    #[derive(Debug)]
    struct DummySink;

    impl SinkOperation for DummySink {
        fn create_partition_sinks(
            &self,
            _context: &DatabaseContext,
            num_sinks: usize,
        ) -> Result<Vec<Box<dyn PartitionSink>>> {
            Ok((0..num_sinks)
                .map(|_| Box::new(DummyPartitionSink) as _)
                .collect())
        }

        fn partition_requirement(&self) -> Option<usize> {
            None
        }
    }

    impl Explainable for DummySink {
        fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
            ExplainEntry::new("DummySink")
        }
    }

    #[derive(Debug)]
    struct DummyPartitionSink;

    impl PartitionSink for DummyPartitionSink {
        fn push(&mut self, _batch: &mut Batch) -> BoxFuture<'_, Result<()>> {
            Box::pin(async { Ok(()) })
        }

        fn finalize(&mut self) -> BoxFuture<'_, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[test]
    fn sink_single_partition() {
        let wrapper = OperatorWrapper::new(SinkOperator::new(DummySink));
        let (op_state, mut part_states) = wrapper
            .operator
            .create_states(&test_database_context(), 1024, 1)
            .unwrap()
            .branchless_into_states()
            .unwrap();

        let mut input = Batch::empty_with_num_rows(8);
        let mut output = Batch::try_new([DataType::Int64], 1024).unwrap();

        let poll = wrapper
            .poll_execute(
                &mut part_states[0],
                &op_state,
                ExecuteInOutState {
                    input: Some(&mut input),
                    output: Some(&mut output),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::NeedsMore, poll);

        let poll = wrapper
            .poll_finalize(&mut part_states[0], &op_state)
            .unwrap();
        assert_eq!(PollFinalize::NeedsDrain, poll);

        let poll = wrapper
            .poll_execute(
                &mut part_states[0],
                &op_state,
                ExecuteInOutState {
                    input: Some(&mut input),
                    output: Some(&mut output),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::Exhausted, poll);

        let expected = Batch::try_from_arrays([Array::try_from_iter([8_i64]).unwrap()]).unwrap();
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn sink_multiple_partitions() {
        let wrapper = OperatorWrapper::new(SinkOperator::new(DummySink));
        let (op_state, mut part_states) = wrapper
            .operator
            .create_states(&test_database_context(), 1024, 2)
            .unwrap()
            .branchless_into_states()
            .unwrap();

        let mut input0 = Batch::empty_with_num_rows(8);
        let mut output0 = Batch::try_new([DataType::Int64], 1024).unwrap();

        let poll = wrapper
            .poll_execute(
                &mut part_states[0],
                &op_state,
                ExecuteInOutState {
                    input: Some(&mut input0),
                    output: Some(&mut output0),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::NeedsMore, poll);

        // Partition 0 is done, should not try to drain.
        let poll = wrapper
            .poll_finalize(&mut part_states[0], &op_state)
            .unwrap();
        assert_eq!(PollFinalize::Finalized, poll);

        let mut input1 = Batch::empty_with_num_rows(4);
        let mut output1 = Batch::try_new([DataType::Int64], 1024).unwrap();

        let poll = wrapper
            .poll_execute(
                &mut part_states[1],
                &op_state,
                ExecuteInOutState {
                    input: Some(&mut input1),
                    output: Some(&mut output1),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::NeedsMore, poll);

        let poll = wrapper
            .poll_finalize(&mut part_states[1], &op_state)
            .unwrap();
        assert_eq!(PollFinalize::NeedsDrain, poll);

        let poll = wrapper
            .poll_execute(
                &mut part_states[1],
                &op_state,
                ExecuteInOutState {
                    input: Some(&mut input1),
                    output: Some(&mut output1),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::Exhausted, poll);

        let expected = Batch::try_from_arrays([Array::try_from_iter([12_i64]).unwrap()]).unwrap();
        assert_batches_eq(&expected, &output1);
    }
}
