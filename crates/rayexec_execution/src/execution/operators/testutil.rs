//! Utilities for testing operator implementations.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Wake, Waker};

use rayexec_error::Result;

use super::{
    ExecutableOperator,
    ExecuteInOutState,
    OperatorState,
    PartitionState,
    PollExecute,
    PollFinalize,
};
use crate::database::system::new_system_catalog;
use crate::database::DatabaseContext;
use crate::datasource::DataSourceRegistry;

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
