use rayexec_error::Result;
use std::task::{Context, Poll};

use crate::types::batch::DataBatch;

/// Unwraps a record batch from a poll.
pub fn unwrap_poll_partition(poll: Poll<Option<Result<DataBatch>>>) -> DataBatch {
    match poll {
        Poll::Ready(Some(Ok(batch))) => batch,
        other => panic!("did not get ready batch, got: {other:?}"),
    }
}

/// Returns a noop context for using with `poll_partition` in tests.
pub fn noop_context() -> Context<'static> {
    let waker = futures::task::noop_waker_ref();
    Context::from_waker(waker)
}
