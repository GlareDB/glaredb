pub mod physical_project;
pub mod physical_sort;

use std::fmt::Debug;
use std::task::Context;

use physical_project::ProjectPartitionState;
use physical_sort::partition_state::SortPartitionState;
use rayexec_error::Result;

use crate::arrays::batch::Batch;
use crate::explain::explainable::Explainable;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollExecute {
    /// Operator accepted input and wrote its output to the output batch.
    ///
    /// The next poll should be with a new input batch.
    Ready,
    /// Push pending. Waker stored, re-execute with the exact same state.
    Pending,
    /// Operator accepted as much input at can handle. Don't provide any
    /// additional input.
    Break,
    /// Operator needs more input before it'll produce any meaningful output.
    NeedsMore,
    /// No more output.
    Exhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollFinalize {
    /// Operator finalized, execution of this operator finished.
    ///
    /// `poll_execute` will not be called after this is returned.
    Finalized,
    /// This operator needs to be drained.
    ///
    /// `poll_execute` will be called with empty input batches until the
    /// opperator indicates it's been exhausted.
    NeedsDrain,
    /// Finalize pending, re-execute with the same state.
    Pending,
}

#[derive(Debug)]
pub enum PartitionState {
    Project(ProjectPartitionState),
    Sort(SortPartitionState),
    None,
}

#[derive(Debug)]
pub enum OperatorState {
    None,
}

#[derive(Debug)]
pub struct ExecuteInOutState<'a> {
    /// Input batch being pushed to the operator.
    ///
    /// May be None for operators that are only producing output.
    input: Option<&'a mut Batch>,
    /// Output batch the operator should write to.
    ///
    /// May be None for operators that only consume batches.
    output: Option<&'a mut Batch>,
}

pub trait ExecutableOperator: Sync + Send + Debug + Explainable {
    fn poll_execute(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        inout: ExecuteInOutState,
    ) -> Result<PollExecute>;

    fn poll_finalize(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize>;
}
