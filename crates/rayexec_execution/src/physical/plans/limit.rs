use crate::expr::PhysicalScalarExpression;
use crate::physical::TaskContext;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::types::batch::DataBatch;
use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};
use std::collections::VecDeque;
use std::sync::Arc;
use std::task::{Context, Waker};

use super::{PollPull, PollPush, Sink, Source};

const LIMIT_BUFFER_CAP: usize = 4;

/// Limit the number of rows output with an optional offset.
///
/// This applies a limit per-partition. If a global limit is desired, ensure
/// that the number of inputs partitions to this is one.
#[derive(Debug)]
pub struct PhysicalLimit {
    /// Partitions states.
    states: Arc<Vec<Mutex<LocalState>>>,

    /// Sink for the limit. Expected to be take during planning.
    sink: Option<PhysicalLimitSink>,
}

/// Per-partition state.
#[derive(Debug)]
struct LocalState {
    /// Waker on the pull side.
    pull_waker: Option<Waker>,
    /// Waker on the push side.
    push_waker: Option<Waker>,

    /// Remaining offset before we can actually start sending batches.
    remaining_offset: usize,
    /// Remaining limit before we stop sending batches.
    remaining_limit: usize,

    /// Buffer of computed batches from input.
    buf: VecDeque<DataBatch>,

    /// If we're finished.
    finished: bool,
}

impl PhysicalLimit {
    pub fn new(offset: Option<usize>, limit: usize, partitions: usize) -> Self {
        let states: Vec<_> = (0..partitions)
            .map(|_| {
                Mutex::new(LocalState {
                    pull_waker: None,
                    push_waker: None,
                    remaining_offset: offset.unwrap_or(0),
                    remaining_limit: limit,
                    buf: VecDeque::with_capacity(LIMIT_BUFFER_CAP),
                    finished: false,
                })
            })
            .collect();

        let states = Arc::new(states);
        let sink = PhysicalLimitSink {
            states: states.clone(),
        };

        PhysicalLimit {
            states,
            sink: Some(sink),
        }
    }

    pub fn take_sink(&mut self) -> Option<PhysicalLimitSink> {
        self.sink.take()
    }
}

impl Source for PhysicalLimit {
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
        match state.buf.pop_front() {
            Some(batch) => Ok(PollPull::Batch(batch)),
            None => {
                if state.finished {
                    Ok(PollPull::Exhausted)
                } else {
                    state.pull_waker = Some(cx.waker().clone());
                    if let Some(waker) = state.push_waker.take() {
                        waker.wake();
                    }
                    Ok(PollPull::Pending)
                }
            }
        }
    }
}

impl Explainable for PhysicalLimit {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("PhysicalLimit")
    }
}

#[derive(Debug)]
pub struct PhysicalLimitSink {
    states: Arc<Vec<Mutex<LocalState>>>,
}

impl Sink for PhysicalLimitSink {
    fn input_partitions(&self) -> usize {
        self.states.len()
    }

    fn poll_push(
        &self,
        _task_cx: &TaskContext,
        cx: &mut Context,
        input: DataBatch,
        partition: usize,
    ) -> Result<PollPush> {
        let mut state = self.states[partition].lock();

        // Need to wait for upstream to pull some batches.
        if state.buf.len() == LIMIT_BUFFER_CAP {
            state.push_waker = Some(cx.waker().clone());
            return Ok(PollPush::Pending(input));
        }

        // We're done.
        if state.remaining_limit == 0 {
            return Ok(PollPush::Break);
        }

        if input.num_rows() <= state.remaining_offset {
            state.remaining_offset -= input.num_rows();
            return Ok(PollPush::Pushed);
        }

        let batch = if state.remaining_offset > 0 {
            let len = std::cmp::min(
                input.num_rows(),
                state.remaining_limit + state.remaining_offset,
            );
            let batch = input.slice(state.remaining_offset, len);
            state.remaining_offset = 0;
            state.remaining_limit -= batch.num_rows();
            batch
        } else if state.remaining_limit < input.num_rows() {
            let batch = input.slice(0, state.remaining_limit);
            state.remaining_limit = 0;
            batch
        } else {
            state.remaining_limit -= input.num_rows();
            input
        };

        state.buf.push_back(batch);
        if let Some(waker) = state.pull_waker.take() {
            waker.wake();
        }

        Ok(PollPush::Pushed)
    }

    fn finish(&self, _task_cx: &TaskContext, partition: usize) -> Result<()> {
        let mut state = self.states[partition].lock();
        state.finished = true;
        if !state.buf.is_empty() {
            if let Some(waker) = state.pull_waker.take() {
                waker.wake()
            }
        }
        Ok(())
    }
}

impl Explainable for PhysicalLimitSink {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("PhysicalLimitSink")
    }
}
