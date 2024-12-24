pub mod batch_collection;
pub mod physical_project;
pub mod physical_sort;

use std::fmt::Debug;
use std::task::Context;

use physical_project::ProjectPartitionState;
use physical_sort::{SortOperatorState, SortPartitionState};
use rayexec_error::Result;

use crate::arrays::batch::Batch;
use crate::arrays::buffer_manager::BufferManager;
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
    /// Operator has more output. Call again with the same input batch.
    HasMore,
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
pub enum PartitionAndOperatorStates {
    /// Operators that have a single input/output.
    Branchless {
        /// Global operator state.
        operator_state: OperatorState,
        /// State per-partition.
        partition_states: Vec<PartitionState>,
    },
    /// Operators that produce 1 or more output branches.
    ///
    /// Mostly for materializations.
    BranchingOutput {
        /// Global operator state.
        operator_state: OperatorState,
        /// Single set of input states.
        inputs_states: Vec<PartitionState>,
        /// Multiple sets of output states.
        output_states: Vec<Vec<PartitionState>>,
    },
    /// Operators that have two children, with this operator acting as the
    /// "sink" for one child.
    ///
    /// For joins, the build side is the terminating input, while the probe side
    /// is non-terminating.
    TerminatingInput {
        /// Global operator state.
        operator_state: OperatorState,
        /// States for the input that is non-terminating.
        nonterminating_states: Vec<PartitionState>,
        /// States for the input that is terminated by this operator.
        terminating_states: Vec<PartitionState>,
    },
}

#[derive(Debug)]
pub enum PartitionState {
    Project(ProjectPartitionState),
    Sort(SortPartitionState),
    None,
}

#[derive(Debug)]
pub enum OperatorState {
    Sort(SortOperatorState),
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
