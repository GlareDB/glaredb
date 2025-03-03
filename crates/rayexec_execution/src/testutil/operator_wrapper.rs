use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Wake, Waker};

use rayexec_error::Result;

use super::database_context::test_database_context;
use crate::arrays::batch::Batch;
use crate::database::system::new_system_catalog;
use crate::database::DatabaseContext;
use crate::datasource::DataSourceRegistry;
use crate::execution::operators::{
    BaseOperator,
    BinaryInputStates,
    ExecutableOperator,
    ExecuteInOut,
    ExecuteOperator,
    OperatorState,
    PartitionState,
    PollExecute,
    PollFinalize,
    PollPull,
    PollPush,
    PullOperator,
    PushOperator,
    UnaryInputStates,
};

/// Waker containing a count that gets incremented by one on every wake.
#[derive(Debug, Default)]
pub struct CountingWaker {
    count: AtomicUsize,
}

impl CountingWaker {
    pub fn wake_count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.count.fetch_add(1, Ordering::SeqCst);
    }
}

/// Wrapper around an operator that provides extra utilities to reduce boiler
/// plate.
///
/// All `poll_` helper method currently shared the same counting waker.
pub struct OperatorWrapper<O: BaseOperator> {
    pub waker: Arc<CountingWaker>,
    pub operator: O,
}

impl<O> OperatorWrapper<O>
where
    O: BaseOperator,
{
    pub fn new(operator: O) -> Self {
        OperatorWrapper {
            waker: Arc::new(CountingWaker::default()),
            operator,
        }
    }
}

impl<O> OperatorWrapper<O>
where
    O: ExecuteOperator,
{
    #[track_caller]
    pub fn poll_execute(
        &self,
        state: &mut O::PartitionExecuteState,
        operator_state: &O::OperatorState,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute> {
        let waker = Waker::from(self.waker.clone());
        let mut cx = Context::from_waker(&waker);
        self.operator
            .poll_execute(&mut cx, state, operator_state, input, output)
    }

    #[track_caller]
    pub fn poll_finalize_execute(
        &self,
        state: &mut O::PartitionExecuteState,
        operator_state: &O::OperatorState,
    ) -> Result<PollFinalize> {
        let waker = Waker::from(self.waker.clone());
        let mut cx = Context::from_waker(&waker);
        self.operator
            .poll_finalize_execute(&mut cx, state, operator_state)
    }
}

impl<O> OperatorWrapper<O>
where
    O: PullOperator,
{
    #[track_caller]
    pub fn poll_pull(
        &self,
        state: &mut O::PartitionPullState,
        operator_state: &O::OperatorState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        let waker = Waker::from(self.waker.clone());
        let mut cx = Context::from_waker(&waker);
        self.operator
            .poll_pull(&mut cx, state, operator_state, output)
    }
}

impl<O> OperatorWrapper<O>
where
    O: PushOperator,
{
    #[track_caller]
    pub fn poll_push(
        &self,
        state: &mut O::PartitionPushState,
        operator_state: &O::OperatorState,
        input: &mut Batch,
    ) -> Result<PollPush> {
        let waker = Waker::from(self.waker.clone());
        let mut cx = Context::from_waker(&waker);
        self.operator
            .poll_push(&mut cx, state, operator_state, input)
    }
}

#[derive(Debug)]
pub struct OperatorWrapper2<O: ExecutableOperator> {
    pub waker: Arc<CountingWaker>,
    pub operator: O,
}

impl<O> OperatorWrapper2<O>
where
    O: ExecutableOperator,
{
    pub fn new(operator: O) -> Self {
        OperatorWrapper2 {
            waker: Arc::new(CountingWaker::default()),
            operator,
        }
    }

    #[track_caller]
    pub fn poll_execute(
        &self,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        inout: ExecuteInOut,
    ) -> Result<PollExecute> {
        let waker = Waker::from(self.waker.clone());
        let mut cx = Context::from_waker(&waker);
        self.operator
            .poll_execute(&mut cx, partition_state, operator_state, inout)
    }

    #[track_caller]
    pub fn poll_finalize(
        &self,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        let waker = Waker::from(self.waker.clone());
        let mut cx = Context::from_waker(&waker);
        self.operator
            .poll_finalize(&mut cx, partition_state, operator_state)
    }
}

/// Methods for operators that accept a single input.
impl<O> OperatorWrapper2<O>
where
    O: ExecutableOperator<States = UnaryInputStates>,
{
    /// Creates unary input states for the operator using a test database
    /// context.
    #[track_caller]
    pub fn create_unary_states(
        &mut self,
        batch_size: usize,
        partitions: usize,
    ) -> UnaryInputStates {
        self.operator
            .create_states(&test_database_context(), batch_size, partitions)
            .unwrap()
    }

    #[track_caller]
    pub fn unary_execute_inout(
        &self,
        states: &mut UnaryInputStates,
        partition: usize,
        input: &mut Batch,
        output: &mut Batch,
    ) -> PollExecute {
        self.poll_execute(
            &mut states.partition_states[partition],
            &states.operator_state,
            ExecuteInOut {
                input: Some(input),
                output: Some(output),
            },
        )
        .unwrap()
    }

    #[track_caller]
    pub fn unary_execute_out(
        &self,
        states: &mut UnaryInputStates,
        partition: usize,
        output: &mut Batch,
    ) -> PollExecute {
        self.poll_execute(
            &mut states.partition_states[partition],
            &states.operator_state,
            ExecuteInOut {
                input: None,
                output: Some(output),
            },
        )
        .unwrap()
    }

    #[track_caller]
    pub fn unary_finalize(&self, states: &mut UnaryInputStates, partition: usize) -> PollFinalize {
        self.poll_finalize(
            &mut states.partition_states[partition],
            &states.operator_state,
        )
        .unwrap()
    }
}

/// Methods for operators that accept two inputs.
impl<O> OperatorWrapper2<O>
where
    O: ExecutableOperator<States = BinaryInputStates>,
{
    /// Creates binary input states for the operator using a test database
    /// context.
    #[track_caller]
    pub fn create_binary_states(
        &mut self,
        batch_size: usize,
        partitions: usize,
    ) -> BinaryInputStates {
        self.operator
            .create_states(&test_database_context(), batch_size, partitions)
            .unwrap()
    }

    /// Executes the sink side of the operator with the given batch input for a
    /// partition.
    #[track_caller]
    pub fn binary_execute_sink(
        &self,
        states: &mut BinaryInputStates,
        partition: usize,
        input: &mut Batch,
    ) -> PollExecute {
        self.poll_execute(
            &mut states.sink_states[partition],
            &states.operator_state,
            ExecuteInOut {
                input: Some(input),
                output: None,
            },
        )
        .unwrap()
    }

    /// Executes the "inout" side of the operator with the given batch input
    /// for a partition, writing the output to `output`.
    #[track_caller]
    pub fn binary_execute_inout(
        &self,
        states: &mut BinaryInputStates,
        partition: usize,
        input: &mut Batch,
        output: &mut Batch,
    ) -> PollExecute {
        self.poll_execute(
            &mut states.inout_states[partition],
            &states.operator_state,
            ExecuteInOut {
                input: Some(input),
                output: Some(output),
            },
        )
        .unwrap()
    }

    /// Finalizes the sink side.
    #[track_caller]
    pub fn binary_finalize_sink(
        &self,
        states: &mut BinaryInputStates,
        partition: usize,
    ) -> PollFinalize {
        self.poll_finalize(&mut states.sink_states[partition], &states.operator_state)
            .unwrap()
    }

    /// Finalizes the inout side.
    #[track_caller]
    pub fn binary_finalize_inout(
        &self,
        states: &mut BinaryInputStates,
        partition: usize,
    ) -> PollFinalize {
        self.poll_finalize(&mut states.inout_states[partition], &states.operator_state)
            .unwrap()
    }
}
