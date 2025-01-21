use std::sync::Arc;
use std::task::Waker;

use parking_lot::Mutex;
use rayexec_error::{OptionExt, Result};

use super::cross_product::CrossProductState;
use super::outer_join_tracker::OuterJoinTracker;
use crate::arrays::batch::Batch;
use crate::execution::operators::join::empty_output_on_empty_build;
use crate::execution::operators::materialize::batch_collection::BatchCollection;
use crate::execution::operators::{
    BinaryInputStates,
    ExecutableOperator,
    ExecuteInOutState,
    OperatorState,
    PartitionState,
    PollExecute,
};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::evaluator::ExpressionEvaluator;
use crate::logical::logical_join::JoinType;

#[derive(Debug)]
pub struct NestedLoopJoinBuildPartitionState {
    partition_collected: BatchCollection,
}

#[derive(Debug)]
pub struct NestedLoopJoinProbePartitionState {
    /// Index for this partition.
    partition_idx: usize,
    /// Cross product state. If None, check global state.
    cross_product: Option<CrossProductState>,
    /// Right outer join row tracker.
    right_outer: Option<OuterJoinTracker>,
    /// Evaluator if this join has a condition. If None, this is a cross join.
    evaluator: Option<ExpressionEvaluator>,
    /// Buffer for expression eval.
    eval_buffer: Batch,
}

#[derive(Debug)]
pub struct NestedLoopJoinOperatorState {
    inner: Mutex<OperatorStateInner>,
}

#[derive(Debug)]
enum OperatorStateInner {
    CollectingLeft {
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
}

impl ExecutableOperator for PhysicalNestedLoopJoin {
    type States = BinaryInputStates;

    fn poll_execute(
        &self,
        cx: &mut std::task::Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        inout: ExecuteInOutState,
    ) -> Result<PollExecute> {
        match partition_state {
            PartitionState::NestedLoopJoinBuild(state) => {
                //
                unimplemented!()
            }
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
                    // Need to get the next input batch.
                    unimplemented!()
                }

                // Eval condition if we have it.
                if let Some(evaluator) = &mut state.evaluator {
                    // state.eval_buffer.reset_for_write()?;
                    // evaluator.eval_single_expression(input, sel, output)
                }

                //
                unimplemented!()
            }
            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl Explainable for PhysicalNestedLoopJoin {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("NestedLoopJoin")
    }
}
