use std::sync::Arc;
use std::task::{Context, Waker};

use parking_lot::Mutex;
use rayexec_error::{OptionExt, Result};

use super::cross_product::CrossProductState;
use super::outer_join_tracker::OuterJoinTracker;
use crate::arrays::array::selection::Selection;
use crate::arrays::datatype::DataType;
use crate::database::DatabaseContext;
use crate::execution::operators::join::empty_output_on_empty_build;
use crate::execution::operators::materialize::batch_collection::BatchCollection;
use crate::execution::operators::{
    BinaryInputStates,
    ExecutableOperator,
    ExecuteInOutState,
    OperatorState,
    PartitionState,
    PollExecute,
    PollFinalize,
};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::selection_evaluator::SelectionEvaluator;
use crate::expr::physical::PhysicalScalarExpression;
use crate::logical::logical_join::JoinType;

#[derive(Debug)]
pub struct NestedLoopJoinBuildPartitionState {}

#[derive(Debug)]
pub struct NestedLoopJoinProbePartitionState {
    /// Index for this partition.
    partition_idx: usize,
    /// Cross product state. If None, check global state.
    cross_product: Option<CrossProductState>,
    /// Right outer join row tracker.
    right_outer: Option<OuterJoinTracker>,
    /// Evaluator if this join has a condition. If None, this is a cross join.
    evaluator: Option<SelectionEvaluator>,
}

#[derive(Debug)]
pub struct NestedLoopJoinOperatorState {
    inner: Mutex<OperatorStateInner>,
}

#[derive(Debug)]
enum OperatorStateInner {
    CollectingLeft {
        remaining_inputs: usize,
        collected: BatchCollection,
        probe_wakers: Vec<Option<Waker>>,
    },
    ReadyForProbe {
        collected: Arc<BatchCollection>,
    },
}

#[derive(Debug)]
pub struct PhysicalNestedLoopJoin {
    pub(crate) join_type: JoinType,
    pub(crate) left_types: Vec<DataType>,
    pub(crate) right_types: Vec<DataType>,
    pub(crate) output_types: Vec<DataType>,
    pub(crate) filter: Option<PhysicalScalarExpression>,
}

impl PhysicalNestedLoopJoin {
    pub fn new(
        join_type: JoinType,
        left_types: impl IntoIterator<Item = DataType>,
        right_types: impl IntoIterator<Item = DataType>,
        filter: Option<PhysicalScalarExpression>,
    ) -> Self {
        let left_types: Vec<_> = left_types.into_iter().collect();
        let right_types: Vec<_> = right_types.into_iter().collect();
        let output_types = left_types
            .iter()
            .cloned()
            .chain(right_types.iter().cloned())
            .collect();

        PhysicalNestedLoopJoin {
            join_type,
            left_types,
            right_types,
            output_types,
            filter,
        }
    }
}

impl ExecutableOperator for PhysicalNestedLoopJoin {
    type States = BinaryInputStates;

    fn output_types(&self) -> &[DataType] {
        &self.output_types
    }

    fn create_states(
        &mut self,
        _context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<Self::States> {
        let sink_states: Vec<_> = (0..partitions)
            .map(|_| PartitionState::NestedLoopJoinBuild(NestedLoopJoinBuildPartitionState {}))
            .collect();

        let probe_states: Vec<_> = (0..partitions)
            .map(|idx| {
                let evaluator = match &self.filter {
                    Some(filter) => Some(SelectionEvaluator::try_new(filter.clone(), batch_size)?),
                    None => None,
                };

                Ok(PartitionState::NestedLoopJoinProbe(
                    NestedLoopJoinProbePartitionState {
                        partition_idx: idx,
                        cross_product: None, // Set when we have completed build side.
                        right_outer: None,   // TODO
                        evaluator,
                    },
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let op_state = OperatorState::NestedLoopJoin(NestedLoopJoinOperatorState {
            inner: Mutex::new(OperatorStateInner::CollectingLeft {
                remaining_inputs: partitions,
                collected: BatchCollection::new(self.left_types.clone(), batch_size),
                probe_wakers: (0..partitions).map(|_| None).collect(),
            }),
        });

        Ok(BinaryInputStates {
            operator_state: op_state,
            sink_states,
            inout_states: probe_states,
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
            PartitionState::NestedLoopJoinBuild(_state) => match operator_state {
                OperatorState::NestedLoopJoin(op_state) => {
                    let mut op_state = op_state.inner.lock();

                    match &mut *op_state {
                        OperatorStateInner::CollectingLeft { collected, .. } => {
                            let input = inout.input.required("input batch required")?;
                            collected.append(input)?;

                            Ok(PollExecute::NeedsMore)
                        }
                        OperatorStateInner::ReadyForProbe { .. } => {
                            panic!("nlj in unexpected probe state")
                        }
                    }
                }
                other => panic!("invalid operator state: {other:?}"),
            },
            PartitionState::NestedLoopJoinProbe(state) => {
                if state.cross_product.is_none() {
                    // Try to get collection from global.
                    let mut op_state = match operator_state {
                        OperatorState::NestedLoopJoin(op_state) => op_state.inner.lock(),
                        other => panic!("invalid operator state: {other:?}"),
                    };

                    match &mut *op_state {
                        OperatorStateInner::CollectingLeft { probe_wakers, .. } => {
                            // Still collecting left side, store waker and come
                            // back later.
                            probe_wakers[state.partition_idx] = Some(cx.waker().clone());
                            return Ok(PollExecute::Pending);
                        }
                        OperatorStateInner::ReadyForProbe { collected } => {
                            // Left is collected, put in local state.
                            state.cross_product = Some(CrossProductState::new(collected.clone()));
                            // Fall through.
                        }
                    }
                }

                let input = inout.input.required("input batch required")?;
                let output = inout.output.required("input batch required")?;

                let cross_product = state.cross_product.as_mut().unwrap(); // Should have been set above.

                if cross_product.collection().row_count() == 0 {
                    if empty_output_on_empty_build(self.join_type) {
                        output.set_num_rows(0)?;
                        return Ok(PollExecute::Exhausted);
                    } else {
                        unimplemented!()
                    }
                }

                let did_write_out = cross_product.try_set_next_row(input, output)?;
                if !did_write_out {
                    match self.join_type {
                        JoinType::LeftSemi => unimplemented!(),
                        JoinType::LeftAnti => unimplemented!(),
                        _ => (),
                    }

                    if let Some(right_outer) = &mut state.right_outer {
                        // Set any unvisited rows on output.
                        right_outer.set_right_join_output(input, output)?;
                        right_outer.reset();

                        // Push batch through rest of pipeline.
                        return Ok(PollExecute::Ready);
                    }

                    // Need to get the next input batch.
                    return Ok(PollExecute::NeedsMore);
                }

                // Eval condition if we have it.
                if let Some(evaluator) = &mut state.evaluator {
                    let selection = evaluator.select(output)?;

                    match self.join_type {
                        JoinType::LeftSemi | JoinType::LeftAnti => {
                            // TODO
                            unimplemented!()
                        }
                        _ => {
                            // Set rows matched by selection.
                            if let Some(right_outer) = &mut state.right_outer {
                                right_outer.set_matches(selection.iter().copied());
                            }

                            // Apply selection to output.
                            output.select(Selection::slice(selection))?;

                            Ok(PollExecute::HasMore)
                        }
                    }
                } else {
                    // Just normal cross product, output already has everything,
                    // so keep going with same input.
                    Ok(PollExecute::HasMore)
                }
            }
            other => panic!("invalid partition state: {other:?}"),
        }
    }

    fn poll_finalize(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        match partition_state {
            PartitionState::NestedLoopJoinBuild(_state) => {
                let mut op_state = match operator_state {
                    OperatorState::NestedLoopJoin(op_state) => op_state.inner.lock(),
                    other => panic!("invalid operator state: {other:?}"),
                };

                match &mut *op_state {
                    OperatorStateInner::CollectingLeft {
                        collected,
                        remaining_inputs,
                        probe_wakers,
                    } => {
                        *remaining_inputs -= 1;

                        if *remaining_inputs == 0 {
                            // We're the last partition to complete building,
                            // swap states and wak up probers.

                            // TODO: Try not to need the dummy collection.
                            let collected =
                                std::mem::replace(collected, BatchCollection::new([], 0));

                            for waker in probe_wakers {
                                if let Some(waker) = waker.take() {
                                    waker.wake();
                                }
                            }

                            *op_state = OperatorStateInner::ReadyForProbe {
                                collected: Arc::new(collected),
                            };
                        }

                        Ok(PollFinalize::Finalized)
                    }
                    _ => {
                        panic!("nlj in unexpected state")
                    }
                }
            }
            PartitionState::NestedLoopJoinProbe(_state) => {
                // TODO: Left join drains.
                Ok(PollFinalize::Finalized)
            }
            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl Explainable for PhysicalNestedLoopJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        let mut ent = ExplainEntry::new("NestedLoopJoin").with_value("join_type", self.join_type);
        if let Some(filter) = self.filter.as_ref() {
            ent = ent.with_value("filter", filter);
        }
        ent
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::batch::Batch;
    use crate::arrays::testutil::{assert_batches_eq, generate_batch};
    use crate::execution::operators::testutil::OperatorWrapper;
    

    #[test]
    fn cross_join_single_build_batch_single_partition() {
        let mut operator = OperatorWrapper::new(PhysicalNestedLoopJoin::new(
            JoinType::Inner,
            [DataType::Utf8],
            [DataType::Int32],
            None,
        ));
        let mut states = operator.create_binary_states(1024, 1);

        // Build first.
        let mut build_input = generate_batch!(["a", "b"]);
        let poll = operator.binary_execute_sink(&mut states, 0, &mut build_input);
        assert_eq!(PollExecute::NeedsMore, poll);

        // Finish build side.
        let poll = operator.binary_finalize_sink(&mut states, 0);
        assert_eq!(PollFinalize::Finalized, poll);

        // Now probe
        let mut probe_input = generate_batch!([1, 2, 3]);
        let mut out = Batch::try_new([DataType::Utf8, DataType::Int32], 1024).unwrap();
        // First probe.
        let poll = operator.binary_execute_inout(&mut states, 0, &mut probe_input, &mut out);
        assert_eq!(PollExecute::HasMore, poll);

        let expected1 = generate_batch!(["a", "a", "a"], [1, 2, 3]);
        assert_batches_eq(&expected1, &out);

        // Second probe.
        let poll = operator.binary_execute_inout(&mut states, 0, &mut probe_input, &mut out);
        assert_eq!(PollExecute::HasMore, poll);

        let expected2 = generate_batch!(["b", "b", "b"], [1, 2, 3]);
        assert_batches_eq(&expected2, &out);

        // Last probe should indicate we need more input.
        let poll = operator.binary_execute_inout(&mut states, 0, &mut probe_input, &mut out);
        assert_eq!(PollExecute::NeedsMore, poll);

        let poll = operator.binary_finalize_inout(&mut states, 0);
        assert_eq!(PollFinalize::Finalized, poll);
    }

    // #[test]
    // fn inner_join_single_build_batch_single_partition() {
    //     // left[1] == right[0]
    //     let filter = plan_scalar(
    //         &expr::eq(expr::col_ref(0, 1), expr::col_ref(1, 0)),
    //         &[&[DataType::Int32, DataType::Utf8], &[DataType::Utf8]],
    //     );
    //     let mut operator = OperatorWrapper::new(PhysicalNestedLoopJoin::new(
    //         JoinType::Inner,
    //         [DataType::Int32, DataType::Utf8],
    //         [DataType::Utf8],
    //         Some(filter),
    //     ));
    //     let mut states = operator.create_binary_states(1024, 1);

    //     // Build and finalize build side.
    //     let mut build_input = generate_batch!([1, 2, 3], ["key1", "key2", "key3"],);
    //     let poll = operator.binary_execute_sink(&mut states, 0, &mut build_input);
    //     assert_eq!(PollExecute::NeedsMore, poll);
    //     let poll = operator.binary_finalize_sink(&mut states, 0);
    //     assert_eq!(PollFinalize::Finalized, poll);

    //     let mut out =
    //         Batch::try_new([DataType::Int32, DataType::Utf8, DataType::Utf8], 1024).unwrap();

    //     // Probe with "key2" & "key3"
    //     let mut probe_input = generate_batch!(["key2", "key3"]);
    //     let poll = operator.binary_execute_inout(&mut states, 0, &mut probe_input, &mut out);
    //     assert_eq!(PollExecute::HasMore, poll);

    //     let expected1 = generate_batch!(["key2"], [2]);
    //     assert_batches_eq(&expected1, &out);
    // }
}
