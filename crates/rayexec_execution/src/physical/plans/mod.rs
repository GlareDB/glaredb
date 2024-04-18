pub mod create_table_as;
pub mod empty_source;
pub mod filter;
pub mod hash_aggregate;
pub mod hash_join;
pub mod hash_repartition;
pub mod limit;
pub mod nested_loop_join;
pub mod order;
pub mod projection;
pub mod set_var;
pub mod show_var;
pub mod unbounded_repartition;
pub mod ungrouped_aggregate;
pub mod values;

mod util;

use rayexec_error::{RayexecError, Result};
use std::fmt::Debug;
use std::task::{Context, Poll};

use crate::planner::explainable::Explainable;
use crate::types::batch::DataBatch;

use super::TaskContext;

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
    Pending(DataBatch),

    /// This sink requires no more input.
    ///
    /// Upon receiving this, the operator chain should immediately call this
    /// sink's finish method.
    Break,
}

/// Result of a pull from a Source.
///
/// An additional variant containing a boxed future may be added to allow for
/// easily adapting to an async operation.
#[derive(Debug)]
pub enum PollPull {
    /// Successfully received a data batch.
    Batch(DataBatch),

    /// A batch could not be be retrieved right now.
    ///
    /// A waker will be registered for a later wakeup to try to pull the next
    /// batch.
    Pending,

    /// The source has been exhausted for this partition.
    Exhausted,
}

pub trait Sink: Sync + Send + Explainable + Debug {
    /// Number of input partitions this sink can handle.
    fn input_partitions(&self) -> usize;

    fn poll_push(
        &self,
        task_cx: &TaskContext,
        cx: &mut Context,
        input: DataBatch,
        partition: usize,
    ) -> Result<PollPush>;

    fn finish(&self, task_cx: &TaskContext, partition: usize) -> Result<()>;
}

pub trait Source: Sync + Send + Explainable + Debug {
    /// Number of output partitions this source can produce.
    fn output_partitions(&self) -> usize;

    fn poll_pull(
        &self,
        task_cx: &TaskContext,
        cx: &mut Context,
        partition: usize,
    ) -> Result<PollPull>;
}

pub trait PhysicalOperator: Sync + Send + Explainable + Debug {
    /// Execute this operator on an input batch.
    fn execute(&self, task_cx: &TaskContext, input: DataBatch) -> Result<DataBatch>;
}
