use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Wake, Waker};

use rayexec_error::Result;

use super::database_context::test_db_context;
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
        operator_state: &O::OperatorState,
        state: &mut O::PartitionExecuteState,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute> {
        let waker = Waker::from(self.waker.clone());
        let mut cx = Context::from_waker(&waker);
        self.operator
            .poll_execute(&mut cx, operator_state, state, input, output)
    }

    #[track_caller]
    pub fn poll_finalize_execute(
        &self,
        operator_state: &O::OperatorState,
        state: &mut O::PartitionExecuteState,
    ) -> Result<PollFinalize> {
        let waker = Waker::from(self.waker.clone());
        let mut cx = Context::from_waker(&waker);
        self.operator
            .poll_finalize_execute(&mut cx, operator_state, state)
    }
}

impl<O> OperatorWrapper<O>
where
    O: PullOperator,
{
    #[track_caller]
    pub fn poll_pull(
        &self,
        operator_state: &O::OperatorState,
        state: &mut O::PartitionPullState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        let waker = Waker::from(self.waker.clone());
        let mut cx = Context::from_waker(&waker);
        self.operator
            .poll_pull(&mut cx, operator_state, state, output)
    }
}

impl<O> OperatorWrapper<O>
where
    O: PushOperator,
{
    #[track_caller]
    pub fn poll_push(
        &self,
        operator_state: &O::OperatorState,
        state: &mut O::PartitionPushState,
        input: &mut Batch,
    ) -> Result<PollPush> {
        let waker = Waker::from(self.waker.clone());
        let mut cx = Context::from_waker(&waker);
        self.operator
            .poll_push(&mut cx, operator_state, state, input)
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
        operator_state: &OperatorState,
        partition_state: &mut PartitionState,
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
        operator_state: &OperatorState,
        partition_state: &mut PartitionState,
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
            .create_states(&test_db_context(), batch_size, partitions)
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
            &states.operator_state,
            &mut states.partition_states[partition],
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
            &states.operator_state,
            &mut states.partition_states[partition],
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
            &states.operator_state,
            &mut states.partition_states[partition],
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
            .create_states(&test_db_context(), batch_size, partitions)
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
            &states.operator_state,
            &mut states.sink_states[partition],
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
            &states.operator_state,
            &mut states.inout_states[partition],
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
        self.poll_finalize(&states.operator_state, &mut states.sink_states[partition])
            .unwrap()
    }

    /// Finalizes the inout side.
    #[track_caller]
    pub fn binary_finalize_inout(
        &self,
        states: &mut BinaryInputStates,
        partition: usize,
    ) -> PollFinalize {
        self.poll_finalize(&states.operator_state, &mut states.inout_states[partition])
            .unwrap()
    }
}
