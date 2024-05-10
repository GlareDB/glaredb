pub mod create_table_as;
pub mod empty_source;
pub mod filter;
pub mod hash_repartition;
pub mod limit;
pub mod nested_loop_join;
pub mod order;
pub mod projection;
pub mod set_var;
pub mod show_var;
pub mod table_function;
pub mod unbounded_repartition;
pub mod ungrouped_aggregate;
pub mod values;

mod util;

use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::fmt::Debug;
use std::task::{Context, Poll};

use crate::planner::explainable::Explainable;

use super::TaskContext;

#[derive(Debug)]
pub enum LocalSourceState {
    TableFunction(table_function::TableFunctionLocalState),
}

#[derive(Debug)]
pub enum GlobalSourceState {
    TableFunction(()),
}

#[derive(Debug)]
pub enum LocalSinkState {}

#[derive(Debug)]
pub enum GlobalSinkState {}

/// Result of a push to a Sink.
///
/// A sink may not be ready to accept input either because it's waiting on
/// something else to complete (e.g. the right side of a join needs to the left
/// side to complete first) or some internal buffer is full.
///
/// An additional variant containing a boxed future may be added to allow for
/// easily adapting to an async operation.
#[derive(Debug)]
pub enum PollPush {
    /// Batch was successfully pushed.
    Pushed,

    /// Batch could not be processed right now.
    ///
    /// A waker will be registered for a later wakeup. This same batch should be
    /// pushed at that time.
    Pending(Batch),

    /// This sink requires no more input.
    ///
    /// Upon receiving this, the operator chain should immediately call this
    /// sink's finish method.
    Break,
}

/// Result of a pull from a Source.
#[derive(Debug)]
pub enum PollPull {
    /// Successfully received a data batch.
    Batch(Batch),

    /// A batch could not be be retrieved right now.
    ///
    /// A waker will be registered for a later wakeup to try to pull the next
    /// batch.
    Pending,

    /// The source has been exhausted for this partition.
    Exhausted,
}

impl PollPull {
    /// Helper for convering a std library `Poll` into a `PollPull`.
    fn from_poll(poll: Poll<Option<Result<Batch>>>) -> Result<Self> {
        match poll {
            Poll::Ready(Some(Ok(batch))) => Ok(PollPull::Batch(batch)),
            Poll::Ready(Some(Err(e))) => Err(e),
            Poll::Ready(None) => Ok(PollPull::Exhausted),
            Poll::Pending => Ok(PollPull::Pending),
        }
    }
}

pub trait SinkOperator: Sync + Send + Explainable + Debug {
    /// Number of input partitions.
    fn input_partition(&self) -> usize;

    fn init_local_state(&self, partition: usize) -> Result<LocalSinkState>;

    fn init_global_state(&self) -> Result<GlobalSinkState>;

    fn poll_push(
        &self,
        cx: &mut Context,
        local: &mut LocalSinkState,
        global: &GlobalSinkState,
        partition: usize,
        input: Batch,
    ) -> Result<PollPull>;

    fn finish(
        &self,
        local: &mut LocalSinkState,
        global: &GlobalSinkState,
        partition: usize,
    ) -> Result<()>;
}

pub trait SourceOperator: Sync + Send + Explainable + Debug {
    fn output_partitions(&self) -> usize;

    fn init_local_state(&self, partition: usize) -> Result<LocalSourceState>;

    fn init_global_state(&self) -> Result<GlobalSourceState>;

    fn poll_pull(
        &self,
        cx: &mut Context,
        local: &mut LocalSourceState,
        global: &GlobalSourceState,
    ) -> Result<PollPull>;
}

pub trait SinkOperator2: Sync + Send + Explainable + Debug {
    /// Number of input partitions this sink can handle.
    fn input_partitions(&self) -> usize;

    fn poll_push(
        &self,
        task_cx: &TaskContext,
        cx: &mut Context,
        input: Batch,
        partition: usize,
    ) -> Result<PollPush>;

    fn finish(&self, task_cx: &TaskContext, partition: usize) -> Result<()>;
}

pub trait SourceOperator2: Sync + Send + Explainable + Debug {
    /// Number of output partitions this source can produce.
    fn output_partitions(&self) -> usize;

    fn poll_pull(
        &self,
        task_cx: &TaskContext,
        cx: &mut Context,
        partition: usize,
    ) -> Result<PollPull>;
}

pub trait StatelessOperator: Sync + Send + Explainable + Debug {
    /// Execute this operator on an input batch.
    fn execute(&self, task_cx: &TaskContext, input: Batch) -> Result<Batch>;
}
