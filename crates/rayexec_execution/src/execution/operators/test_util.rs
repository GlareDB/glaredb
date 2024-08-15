//! Utilities for testing operator implementations.
use rayexec_error::Result;
use std::sync::Arc;
use std::task::Context;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    task::{Wake, Waker},
};

use rayexec_bullet::array::{Array, Int32Array};
use rayexec_bullet::batch::Batch;

use crate::database::system::new_system_catalog;
use crate::database::DatabaseContext;
use crate::datasource::DataSourceRegistry;

use super::{ExecutableOperator, OperatorState, PartitionState, PollPull, PollPush};

pub fn test_database_context() -> DatabaseContext {
    DatabaseContext::new(Arc::new(
        new_system_catalog(&DataSourceRegistry::default()).unwrap(),
    ))
    .unwrap()
}

/// Test context containg a waker implementation that counts the number of times
/// it's woken.
///
/// Also has utility methods for calling poll_push and poll_pull using self as
/// the context/waker.
///
/// Normal execution of the operator uses a `PartitionPipelineWaker` which
/// re-executes the pipeline once woken, but that operates on an entire
/// Partition Pipeline.
#[derive(Debug)]
pub struct TestWakerContext {
    waker: Waker,
    inner: Arc<TestWakerInner>,
}

#[derive(Debug)]
struct TestWakerInner {
    wake_count: AtomicUsize,
}

impl TestWakerContext {
    pub fn new() -> Self {
        let inner = Arc::new(TestWakerInner {
            wake_count: 0.into(),
        });
        let waker = Waker::from(inner.clone());

        TestWakerContext { waker, inner }
    }

    /// Get the number of times this waker was woken.
    pub fn wake_count(&self) -> usize {
        self.inner.wake_count.load(Ordering::SeqCst)
    }

    /// Create a context that's holding this waker.
    pub fn context(&self) -> Context {
        Context::from_waker(&self.waker)
    }

    pub fn poll_push<Operator: ExecutableOperator>(
        &self,
        operator: impl AsRef<Operator>,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        operator
            .as_ref()
            .poll_push(&mut self.context(), partition_state, operator_state, batch)
    }

    pub fn poll_pull<Operator: ExecutableOperator>(
        &self,
        operator: impl AsRef<Operator>,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollPull> {
        operator
            .as_ref()
            .poll_pull(&mut self.context(), partition_state, operator_state)
    }
}

impl Wake for TestWakerInner {
    fn wake(self: Arc<Self>) {
        self.wake_count.fetch_add(1, Ordering::SeqCst);
    }
}

/// Unwraps a batch from the PollPull::Batch variant.
pub fn unwrap_poll_pull_batch(poll: PollPull) -> Batch {
    match poll {
        PollPull::Batch(batch) => batch,
        other => panic!("unexpected poll pull: {other:?}"),
    }
}

/// Makes a batch with a single column i32 values provided by the iterator.
pub fn make_i32_batch(iter: impl IntoIterator<Item = i32>) -> Batch {
    Batch::try_new(vec![Array::Int32(Int32Array::from_iter(iter.into_iter()))]).unwrap()
}
