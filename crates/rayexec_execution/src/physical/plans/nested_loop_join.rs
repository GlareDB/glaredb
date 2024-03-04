use crate::expr::PhysicalScalarExpression;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::types::batch::DataBatch;
use arrow_array::cast::AsArray;
use arrow_array::UInt32Array;
use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use super::{Sink, Source};

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

impl Source for PhysicalNestedLoopJoin {
    fn output_partitions(&self) -> usize {
        self.states.len()
    }

    fn poll_next(&self, cx: &mut Context, partition: usize) -> Poll<Option<Result<DataBatch>>> {
        let mut state = self.states[partition].lock();
        if !state.build_finished {
            state.pending_pull = Some(cx.waker().clone());
            return Poll::Pending;
        }

        if let Some(batch) = state.computed.pop_front() {
            return Poll::Ready(Some(Ok(batch)));
        }

        if state.probe_finished {
            return Poll::Ready(None);
        }

        state.pending_pull = Some(cx.waker().clone());
        Poll::Pending
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
    Local(Vec<DataBatch>),

    /// All batches across all partitions.
    Global(Arc<Vec<DataBatch>>),
}

impl BatchState {
    fn push_for_build(&mut self, batch: DataBatch) -> Result<()> {
        match self {
            Self::Local(batches) => batches.push(batch),
            _ => return Err(RayexecError::new("Expected batch state to be local")),
        }
        Ok(())
    }

    fn take_local(&mut self) -> Result<Vec<DataBatch>> {
        match self {
            Self::Local(batches) => Ok(std::mem::take(batches)),
            _ => Err(RayexecError::new("Expected batch state to be local")),
        }
    }

    fn swap_to_global(&mut self, global: Arc<Vec<DataBatch>>) -> Result<()> {
        match self {
            Self::Local(_) => {
                *self = BatchState::Global(global);
                Ok(())
            }
            _ => Err(RayexecError::new("Expected batch state to be local")),
        }
    }

    fn get_global_batches(&self) -> Result<Arc<Vec<DataBatch>>> {
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
    computed: VecDeque<DataBatch>,
}

#[derive(Debug)]
pub struct PhysicalNestedLoopJoinBuildSink {
    /// Partition-local states.
    states: Arc<Vec<Mutex<LocalState>>>,

    /// Number of partitions we're still waiting on to complete.
    remaining: AtomicUsize,
}

impl Sink for PhysicalNestedLoopJoinBuildSink {
    fn input_partitions(&self) -> usize {
        self.states.len()
    }

    fn poll_ready(&self, _cx: &mut Context, _partition: usize) -> Poll<()> {
        // Always need to collect build side.
        Poll::Ready(())
    }

    fn push(&self, input: DataBatch, partition: usize) -> Result<()> {
        let mut state = self.states[partition].lock();
        assert!(!state.build_finished);
        state.left_batches.push_for_build(input)?;
        Ok(())
    }

    fn finish(&self, partition: usize) -> Result<()> {
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

impl Sink for PhysicalNestedLoopJoinProbeSink {
    fn input_partitions(&self) -> usize {
        self.states.len()
    }

    fn poll_ready(&self, cx: &mut Context, partition: usize) -> Poll<()> {
        let mut state = self.states[partition].lock();
        if !state.build_finished {
            // We're still building, register for a wakeup.
            state.pending_push = Some(cx.waker().clone());
            return Poll::Pending;
        }
        assert!(!state.probe_finished);
        Poll::Ready(())
    }

    fn push(&self, input: DataBatch, partition: usize) -> Result<()> {
        let left_batches = {
            // TODO: Maybe split input/output states to allow holding this lock
            // for the entire function call.
            let state = self.states[partition].lock();
            assert!(state.build_finished);
            assert!(!state.probe_finished);

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

        Ok(())
    }

    fn finish(&self, partition: usize) -> Result<()> {
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
    left: &DataBatch,
    right: &DataBatch,
    filter: Option<&PhysicalScalarExpression>,
) -> Result<Vec<DataBatch>> {
    let mut batches = Vec::with_capacity(left.num_rows() * right.num_rows());

    // For each row in the left batch, join the entirety of right.
    for left_idx in 0..left.num_rows() {
        let left_indices =
            UInt32Array::from_iter(std::iter::repeat(left_idx as u32).take(right.num_rows()));

        let mut cols = Vec::new();
        for col in left.columns() {
            // TODO: It'd be nice to have a logical repeated array type instead
            // of having to physically copy the same element `n` times into a
            // new array.
            let left_repeated = arrow::compute::take(&col, &left_indices, None)?;
            cols.push(left_repeated);
        }

        // Join all of right.
        cols.extend_from_slice(right.columns());
        let mut batch = DataBatch::try_new(cols)?;

        // If we have a filter, apply it to the intermediate batch.
        if let Some(filter) = &filter {
            let arr = filter.eval(&batch)?;
            let selection = arr.as_boolean(); // TODO: Need to check that this returns a boolean somewhere.

            let filtered = batch
                .columns()
                .iter()
                .map(|col| arrow::compute::filter(col, selection))
                .collect::<Result<Vec<_>, _>>()?;

            batch = DataBatch::try_new(filtered)?;
        }

        batches.push(batch);
    }

    Ok(batches)
}
