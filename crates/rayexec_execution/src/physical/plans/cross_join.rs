use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::types::batch::DataBatch;
use arrow::compute::filter_record_batch;
use arrow_array::{RecordBatch, UInt32Array};
use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use super::{Sink, Source};

#[derive(Debug)]
pub struct PhysicalCrossJoin {
    /// Shared states across the sinks and source for all partitions.
    states: Arc<Vec<Mutex<LocalState>>>,

    /// The configured build sink. Expected to be taken during pipeline
    /// building.
    build_sink: Option<PhysicalCrossJoinBuildSink>,

    /// The configured probe sink. Expected to be taken during pipeline
    /// building.
    probe_sink: Option<PhysicalCrossJoinProbeSink>,
}

impl PhysicalCrossJoin {
    pub fn new(partitions: usize) -> Self {
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
        let build_sink = PhysicalCrossJoinBuildSink {
            states: states.clone(),
            remaining: states.len().into(),
        };
        let probe_sink = PhysicalCrossJoinProbeSink {
            states: states.clone(),
        };

        PhysicalCrossJoin {
            states,
            build_sink: Some(build_sink),
            probe_sink: Some(probe_sink),
        }
    }

    pub fn take_build_sink(&mut self) -> Option<PhysicalCrossJoinBuildSink> {
        self.build_sink.take()
    }

    pub fn take_probe_sink(&mut self) -> Option<PhysicalCrossJoinProbeSink> {
        self.probe_sink.take()
    }
}

impl Source for PhysicalCrossJoin {
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
pub struct PhysicalCrossJoinBuildSink {
    /// Partition-local states.
    states: Arc<Vec<Mutex<LocalState>>>,

    /// Number of partitions we're still waiting on to complete.
    remaining: AtomicUsize,
}

impl Sink for PhysicalCrossJoinBuildSink {
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
            let mut state = self.states[partition].lock();
            assert!(!state.build_finished);
            state.build_finished = true;
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

#[derive(Debug)]
pub struct PhysicalCrossJoinProbeSink {
    /// Partition-local states.
    states: Arc<Vec<Mutex<LocalState>>>,
}

impl Sink for PhysicalCrossJoinProbeSink {
    fn input_partitions(&self) -> usize {
        self.states.len()
    }

    fn poll_ready(&self, cx: &mut Context, partition: usize) -> Poll<()> {
        let mut state = self.states[partition].lock();
        if !state.build_finished {
            // We're still building, register for a wakeup.
            state.pending_push = Some(cx.waker().clone());
        }
        assert!(!state.probe_finished);
        Poll::Ready(())
    }

    fn push(&self, input: DataBatch, partition: usize) -> Result<()> {
        let left_batches = {
            let state = self.states[partition].lock();
            assert!(state.build_finished);
            assert!(!state.probe_finished);

            state.left_batches.get_global_batches()?
        };

        let mut joined = Vec::new();
        for left in left_batches.iter() {
            let mut batch_joined = cross_join(left, &input)?;
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

fn cross_join(left: &DataBatch, right: &DataBatch) -> Result<Vec<DataBatch>> {
    let mut batches = Vec::with_capacity(left.num_rows() * right.num_rows());

    // For each row in the left batch, join the entirety of right.
    for left_idx in 0..left.num_rows() {
        let left_indices =
            UInt32Array::from_iter(std::iter::repeat(left_idx as u32).take(right.num_rows()));

        let mut cols = Vec::new();
        for col in left.columns() {
            let left_repeated = arrow::compute::take(&col, &left_indices, None)?;
            cols.push(left_repeated);
        }

        // Join all of right.
        cols.extend_from_slice(right.columns());

        batches.push(DataBatch::try_new(cols)?);
    }

    // Do same thing for all rows in the right batch.
    for right_idx in 0..left.num_rows() {
        let right_indices =
            UInt32Array::from_iter(std::iter::repeat(right_idx as u32).take(left.num_rows()));

        let mut cols = left.columns().to_vec();
        for col in right.columns() {
            let right_repeated = arrow::compute::take(&col, &right_indices, None)?;
            cols.push(right_repeated);
        }

        batches.push(DataBatch::try_new(cols)?);
    }

    Ok(batches)
}
