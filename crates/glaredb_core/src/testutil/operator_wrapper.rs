use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Wake, Waker};

use glaredb_error::Result;

use super::database_context::test_db_context;
use crate::arrays::batch::Batch;
use crate::execution::operators::{
    BaseOperator,
    ExecuteOperator,
    PollExecute,
    PollFinalize,
    PollPull,
    PollPush,
    PullOperator,
    PushOperator,
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

    #[track_caller]
    pub fn poll_finalize_push(
        &self,
        operator_state: &O::OperatorState,
        state: &mut O::PartitionPushState,
    ) -> Result<PollFinalize> {
        let waker = Waker::from(self.waker.clone());
        let mut cx = Context::from_waker(&waker);
        self.operator
            .poll_finalize_push(&mut cx, operator_state, state)
    }
}
