use std::fmt::Debug;
use std::task::Context;

use rayexec_error::Result;

use crate::arrays::batch::Batch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollPull {
    /// Batch was pulled, keep pulling for more batches.
    HasMore,
    /// More work needs to be done to pull the batch.
    ///
    /// The same output batch will be provided on the next poll.
    Pending,
    /// Source is exhausted.
    ///
    /// Output batch will contain meaningful data.
    Exhausted,
}

#[derive(Debug)]
pub struct RawPartitionScanVTable {
    /// Pull function for the scan.
    poll_pull_fn: unsafe fn(*mut (), cx: &mut Context, output: &mut Batch) -> Result<PollPull>,
    /// Drop function for the scan.
    drop_fn: unsafe fn(*mut ()),
}

#[derive(Debug)]
pub struct RawPartitionScan {
    scan: *mut (),
    vtable: &'static RawPartitionScanVTable,
}

unsafe impl Sync for RawPartitionScan {}
unsafe impl Send for RawPartitionScan {}

impl RawPartitionScan {
    pub fn new<S>(mut scan: S) -> Self
    where
        S: PartitionScan,
    {
        let ptr = (&mut scan as *mut S).cast();
        std::mem::forget(scan); // We'll handle dropping manually.
        RawPartitionScan {
            scan: ptr,
            vtable: S::VTABLE,
        }
    }

    pub fn call_poll_pull(&mut self, cx: &mut Context, output: &mut Batch) -> Result<PollPull> {
        // SAFETY: We have a mut reference to the raw scan, so we can safely
        // dereference the pointer into a mut ref.
        unsafe { (self.vtable.poll_pull_fn)(self.scan, cx, output) }
    }
}

impl Drop for RawPartitionScan {
    fn drop(&mut self) {
        unsafe { (self.vtable.drop_fn)(self.scan) }
    }
}

pub trait PartitionScan: Debug + Sync + Send + Sized {
    // TODO: Move to private trait.
    const VTABLE: &'static RawPartitionScanVTable = &RawPartitionScanVTable {
        poll_pull_fn: |ptr: *mut (), cx: &mut Context, output: &mut Batch| -> Result<PollPull> {
            let scan = unsafe { ptr.cast::<Self>().as_mut().unwrap() };
            scan.poll_pull(cx, output)
        },

        drop_fn: |ptr: *mut ()| unsafe { ptr.drop_in_place() },
    };

    /// Pull batches from this source.
    ///
    /// `output` will already be reset for writing. The source should write its
    /// data to `output` which will be pushed to the upstream operators.
    fn poll_pull(&mut self, cx: &mut Context, output: &mut Batch) -> Result<PollPull>;
}
