use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Wake, Waker};

use rayexec_error::Result;

use crate::arrays::batch::Batch;
use crate::database::system::new_system_catalog;
use crate::database::DatabaseContext;
use crate::datasource::DataSourceRegistry;
use crate::execution::operators::{
    BinaryInputStates,
    ExecutableOperator,
    ExecuteInOutState,
    OperatorState,
    PartitionState,
    PollExecute,
    PollFinalize,
    UnaryInputStates,
};

pub fn test_database_context() -> DatabaseContext {
    DatabaseContext::new(Arc::new(
        new_system_catalog(&DataSourceRegistry::default()).unwrap(),
    ))
    .unwrap()
}

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

/// Wrapper around an operator that uses a stub waker that tracks how many times
/// it's woken.
#[derive(Debug)]
pub struct OperatorWrapper<O: ExecutableOperator> {
    pub waker: Arc<CountingWaker>,
    pub operator: O,
}

impl<O> OperatorWrapper<O>
where
    O: ExecutableOperator,
{
    pub fn new(operator: O) -> Self {
        OperatorWrapper {
            waker: Arc::new(CountingWaker::default()),
            operator,
        }
    }

    #[track_caller]
    pub fn poll_execute(
        &self,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        inout: ExecuteInOutState,
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
impl<O> OperatorWrapper<O>
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
            ExecuteInOutState {
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
            ExecuteInOutState {
                input: None,
                output: Some(output),
            },
        )
        .unwrap()
    }

    #[track_caller]
    pub fn unary_finalize_sink(
        &self,
        states: &mut UnaryInputStates,
        partition: usize,
    ) -> PollFinalize {
        self.poll_finalize(
            &mut states.partition_states[partition],
            &states.operator_state,
        )
        .unwrap()
    }
}

/// Methods for operators that accept two inputs.
impl<O> OperatorWrapper<O>
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
            ExecuteInOutState {
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
            ExecuteInOutState {
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
