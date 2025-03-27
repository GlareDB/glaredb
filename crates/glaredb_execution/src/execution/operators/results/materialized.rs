use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use glaredb_error::{DbError, Result};
use parking_lot::Mutex;

use crate::arrays::batch::Batch;
use crate::arrays::collection::concurrent::{
    ColumnCollectionAppendState,
    ConcurrentColumnCollection,
};
use crate::arrays::datatype::DataType;
use crate::execution::operators::util::delayed_count::DelayedPartitionCount;
use crate::execution::operators::{
    BaseOperator,
    ExecutionProperties,
    PollFinalize,
    PollPush,
    PushOperator,
};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

/// Notifies when all partitions have been materialized into the column
/// collection.
#[derive(Debug)]
pub struct NotifyMaterialized {
    inner: Arc<Mutex<NotifyMaterializedInner>>,
}

#[derive(Debug)]
struct NotifyMaterializedInner {
    registered: bool,
    finished: bool,
    waker: Option<Waker>,
}

impl NotifyMaterialized {
    pub fn new() -> NotifyMaterialized {
        NotifyMaterialized {
            inner: Arc::new(Mutex::new(NotifyMaterializedInner {
                registered: false,
                finished: false,
                waker: None,
            })),
        }
    }
}

impl Future for NotifyMaterialized {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut inner = self.inner.lock();
        if inner.finished {
            return Poll::Ready(Ok(()));
        }

        if !inner.registered {
            return Poll::Ready(Err(DbError::new("Notify not registered")));
        }

        inner.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

#[derive(Debug)]
pub struct MaterializedResultsOperatorState {
    remaining_partitions: Mutex<DelayedPartitionCount>,
}

#[derive(Debug)]
pub struct MaterializedResultsPartitionState {
    append_state: ColumnCollectionAppendState,
}

/// Materializes results into a column collection.
#[derive(Debug)]
pub struct PhysicalMaterializedResults {
    notify: Arc<Mutex<NotifyMaterializedInner>>,
    collection: Arc<ConcurrentColumnCollection>,
}

impl PhysicalMaterializedResults {
    /// Creates a new sink which will send all batches to the column collection.
    ///
    /// `notify` will notify when the last partition is finalized, indicating
    /// all rows have been written to the collection.
    pub fn new(
        notify: &NotifyMaterialized,
        collection: Arc<ConcurrentColumnCollection>,
    ) -> Result<Self> {
        let notify = notify.inner.clone();
        {
            let mut inner = notify.lock();
            if inner.registered {
                return Err(DbError::new("Notify signal has already been registered"));
            }
            inner.registered = true;
        }

        Ok(PhysicalMaterializedResults { notify, collection })
    }
}

impl BaseOperator for PhysicalMaterializedResults {
    const OPERATOR_NAME: &str = "MaterializedResults";

    type OperatorState = MaterializedResultsOperatorState;

    fn create_operator_state(&self, _props: ExecutionProperties) -> Result<Self::OperatorState> {
        Ok(MaterializedResultsOperatorState {
            remaining_partitions: Mutex::new(DelayedPartitionCount::uninit()),
        })
    }

    fn output_types(&self) -> &[DataType] {
        &[]
    }
}

impl PushOperator for PhysicalMaterializedResults {
    type PartitionPushState = MaterializedResultsPartitionState;

    fn create_partition_push_states(
        &self,
        operator_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionPushState>> {
        operator_state.remaining_partitions.lock().set(partitions)?;

        let states = (0..partitions)
            .map(|_| MaterializedResultsPartitionState {
                append_state: self.collection.init_append_state(),
            })
            .collect();

        Ok(states)
    }

    fn poll_push(
        &self,
        _cx: &mut Context,
        _operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPushState,
        input: &mut Batch,
    ) -> Result<PollPush> {
        self.collection
            .append_batch(&mut state.append_state, input)?;

        Ok(PollPush::NeedsMore)
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPushState,
    ) -> Result<PollFinalize> {
        self.collection.flush(&mut state.append_state)?;

        let current = operator_state.remaining_partitions.lock().dec_by_one()?;
        if current == 0 {
            let mut inner = self.notify.lock();
            inner.finished = true;
            if let Some(waker) = inner.waker.take() {
                waker.wake()
            }
        }

        Ok(PollFinalize::Finalized)
    }
}

impl Explainable for PhysicalMaterializedResults {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new(Self::OPERATOR_NAME)
    }
}
