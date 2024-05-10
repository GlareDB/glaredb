use crate::expr::PhysicalScalarExpression;
use crate::physical::TaskContext;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use parking_lot::Mutex;
use rayexec_bullet::array::Array;
use rayexec_bullet::batch::Batch;
use rayexec_bullet::compute::filter::filter;
use rayexec_bullet::compute::take::take;
use rayexec_error::{RayexecError, Result};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Waker};

use super::{PollPull, PollPush, SinkOperator2, SourceOperator2};

/// Nested loop join for joining tables on arbitrary expressions.
#[derive(Debug)]
pub struct PhysicalNestedLoopJoin {
    /// Shared states across the sinks and source for all partitions.
    states: Arc<Vec<Mutex<LocalState>>>,

    /// The configured build sink. Expected to be taken during pipeline
    /// building.
    build_sink: Option<PhysicalNestedLoopJoinBuildSink>,

    /// The configured probe sink. Expected to be taken during pipeline
    /// building.
    probe_sink: Option<PhysicalNestedLoopJoinProbeSink>,
}

impl PhysicalNestedLoopJoin {
    pub fn new(partitions: usize, filter: Option<PhysicalScalarExpression>) -> Self {
        let states: Vec<_> = (0..partitions)
            .map(|_| {
                Mutex::new(LocalState {
                    build_finished: false,
                    probe_finished: false,
                    left_batches: BatchState::Local(Vec::new()),
                    pending_push: None,
                    pending_pull: None,
                    computed: VecDeque::new(),
                })
            })
            .collect();

        let states = Arc::new(states);
        let build_sink = PhysicalNestedLoopJoinBuildSink {
            states: states.clone(),
            remaining: states.len().into(),
        };
        let probe_sink = PhysicalNestedLoopJoinProbeSink {
            states: states.clone(),
            filter,
        };

        PhysicalNestedLoopJoin {
            states,
            build_sink: Some(build_sink),
            probe_sink: Some(probe_sink),
        }
    }

    pub fn take_build_sink(&mut self) -> Option<PhysicalNestedLoopJoinBuildSink> {
        self.build_sink.take()
    }

    pub fn take_probe_sink(&mut self) -> Option<PhysicalNestedLoopJoinProbeSink> {
        self.probe_sink.take()
    }
}

impl SourceOperator2 for PhysicalNestedLoopJoin {
    fn output_partitions(&self) -> usize {
        self.states.len()
    }

    fn poll_pull(
        &self,
        _task_cx: &TaskContext,
        cx: &mut Context,
        partition: usize,
    ) -> Result<PollPull> {
        let mut state = self.states[partition].lock();
        if !state.build_finished {
            state.pending_pull = Some(cx.waker().clone());
            return Ok(PollPull::Pending);
        }

        if let Some(batch) = state.computed.pop_front() {
            return Ok(PollPull::Batch(batch));
        }

        if state.probe_finished {
            return Ok(PollPull::Exhausted);
        }

        // Note yet finished, and there's no batches for us to take right now.
        state.pending_pull = Some(cx.waker().clone());
        Ok(PollPull::Pending)
    }
}

impl Explainable for PhysicalNestedLoopJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("PhysicalNestedLoopJoin")
    }
}

#[derive(Debug)]
enum BatchState {
    /// Local only batches.
    Local(Vec<Batch>),

    /// All batches across all partitions.
    Global(Arc<Vec<Batch>>),
}

impl BatchState {
    fn push_for_build(&mut self, batch: Batch) -> Result<()> {
        match self {
            Self::Local(batches) => batches.push(batch),
            _ => return Err(RayexecError::new("Expected batch state to be local")),
        }
        Ok(())
    }

    fn take_local(&mut self) -> Result<Vec<Batch>> {
        match self {
            Self::Local(batches) => Ok(std::mem::take(batches)),
            _ => Err(RayexecError::new("Expected batch state to be local")),
        }
    }

    fn swap_to_global(&mut self, global: Arc<Vec<Batch>>) -> Result<()> {
        match self {
            Self::Local(_) => {
                *self = BatchState::Global(global);
                Ok(())
            }
            _ => Err(RayexecError::new("Expected batch state to be local")),
        }
    }

    fn get_global_batches(&self) -> Result<Arc<Vec<Batch>>> {
        match self {
            BatchState::Global(v) => Ok(v.clone()),
            _ => Err(RayexecError::new("Expected batch state to be global")),
        }
    }
}

#[derive(Debug)]
struct LocalState {
    /// If this partition is finished for the build phase.
    build_finished: bool,

    /// If this partition is finished for the probe phase.
    probe_finished: bool,

    /// Batches collected on the left side.
    ///
    /// During the build phase, this contains only batches for this partition.
    /// During probe, this contains all batches.
    left_batches: BatchState,

    /// Waker for a pending push on the probe sink side.
    pending_push: Option<Waker>,

    /// Waker for a pending pull.
    pending_pull: Option<Waker>,

    /// Computed batches.
    computed: VecDeque<Batch>,
}

#[derive(Debug)]
pub struct PhysicalNestedLoopJoinBuildSink {
    /// Partition-local states.
    states: Arc<Vec<Mutex<LocalState>>>,

    /// Number of partitions we're still waiting on to complete.
    remaining: AtomicUsize,
}

impl SinkOperator2 for PhysicalNestedLoopJoinBuildSink {
    fn input_partitions(&self) -> usize {
        self.states.len()
    }

    fn poll_push(
        &self,
        _task_cx: &TaskContext,
        _cx: &mut Context,
        input: Batch,
        partition: usize,
    ) -> Result<PollPush> {
        let mut state = self.states[partition].lock();
        assert!(!state.build_finished);
        state.left_batches.push_for_build(input)?;
        Ok(PollPush::Pushed)
    }

    fn finish(&self, _task_cx: &TaskContext, partition: usize) -> Result<()> {
        {
            // Technically just for debugging. We want to make sure we're not
            // accidentally marking the same partition as finished multiple
            // times.
            let state = self.states[partition].lock();
            assert!(!state.build_finished);
        }

        let prev = self.remaining.fetch_sub(1, Ordering::SeqCst);
        if prev == 1 {
            // We're finished, acquire all lock and build the global state.
            let mut states: Vec<_> = self.states.iter().map(|s| s.lock()).collect();

            let mut global_batches = Vec::new();
            for state in states.iter_mut() {
                let mut local = state.left_batches.take_local()?;
                global_batches.append(&mut local);
            }

            let global_batches = Arc::new(global_batches);
            for state in states.iter_mut() {
                state.left_batches.swap_to_global(global_batches.clone())?;
            }

            // Wake pending pushes (pulls will be implicitly triggered by the
            // push).
            for mut state in states {
                state.build_finished = true;
                if let Some(waker) = state.pending_push.take() {
                    waker.wake();
                }
            }

            // The build is complete.
        }

        Ok(())
    }
}

impl Explainable for PhysicalNestedLoopJoinBuildSink {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("PhysicalNestedLoopJoinBuildSink")
    }
}

#[derive(Debug)]
pub struct PhysicalNestedLoopJoinProbeSink {
    /// Partition-local states.
    states: Arc<Vec<Mutex<LocalState>>>,

    /// Optional expression to apply to the joined batches.
    filter: Option<PhysicalScalarExpression>,
}

impl SinkOperator2 for PhysicalNestedLoopJoinProbeSink {
    fn input_partitions(&self) -> usize {
        self.states.len()
    }

    fn poll_push(
        &self,
        _task_cx: &TaskContext,
        cx: &mut Context,
        input: Batch,
        partition: usize,
    ) -> Result<PollPush> {
        let left_batches = {
            // TODO: Maybe split input/output states to allow holding this lock
            // for the entire function call.
            let mut state = self.states[partition].lock();
            if !state.build_finished {
                // We're still building, register for a wakeup.
                state.pending_push = Some(cx.waker().clone());
                return Ok(PollPush::Pending(input));
            }

            state.left_batches.get_global_batches()?
        };

        let mut joined = Vec::new();
        for left in left_batches.iter() {
            let mut batch_joined = cross_join(left, &input, self.filter.as_ref())?;
            joined.append(&mut batch_joined);
        }

        let mut state = self.states[partition].lock();
        state.computed.extend(joined);

        if let Some(waker) = state.pending_pull.take() {
            waker.wake();
        }

        Ok(PollPush::Pushed)
    }

    fn finish(&self, _task_cx: &TaskContext, partition: usize) -> Result<()> {
        let mut state = self.states[partition].lock();
        assert!(state.build_finished);
        assert!(!state.probe_finished);
        state.probe_finished = true;

        if let Some(waker) = state.pending_pull.take() {
            waker.wake();
        }

        Ok(())
    }
}

impl Explainable for PhysicalNestedLoopJoinProbeSink {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("PhysicalNestedLoopJoinProbeSink")
    }
}

/// Generate a cross product of two batches, applying an optional filter to the
/// result.
fn cross_join(
    left: &Batch,
    right: &Batch,
    filter_expr: Option<&PhysicalScalarExpression>,
) -> Result<Vec<Batch>> {
    let mut batches = Vec::with_capacity(left.num_rows() * right.num_rows());

    // For each row in the left batch, join the entirety of right.
    for left_idx in 0..left.num_rows() {
        let left_indices = vec![left_idx; right.num_rows()];

        let mut cols = Vec::new();
        for col in left.columns() {
            // TODO: It'd be nice to have a logical repeated array type instead
            // of having to physically copy the same element `n` times into a
            // new array.
            let left_repeated = take(&col, &left_indices)?;
            cols.push(Arc::new(left_repeated));
        }

        // Join all of right.
        cols.extend_from_slice(right.columns());
        let mut batch = Batch::try_new(cols)?;

        // If we have a filter, apply it to the intermediate batch.
        if let Some(filter_expr) = &filter_expr {
            let arr = filter_expr.eval(&batch)?;
            let selection = match arr.as_ref() {
                Array::Boolean(arr) => arr,
                other => {
                    return Err(RayexecError::new(format!(
                        "Expected filter predicate in cross join to return a boolean, got {}",
                        other.datatype()
                    )))
                }
            };

            let filtered = batch
                .columns()
                .iter()
                .map(|col| filter(col, selection))
                .collect::<Result<Vec<_>, _>>()?;

            batch = Batch::try_new(filtered)?;
        }

        batches.push(batch);
    }

    Ok(batches)
}
