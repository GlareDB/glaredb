use super::{SinkOperator2, SourceOperator2};
use crate::physical::TaskContext;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crossbeam::channel::{Receiver, Sender};
use parking_lot::Mutex;
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

//
// TODO: I don't know if this is good. I'm just adding it in to provide M:N
// repartitioning without the hashing overhead (which also unbounded right now).
//
#[derive(Debug)]
pub struct PhysicalUnboundedRepartition {
    /// Receiver for the Source side to produce batches.
    recv: Receiver<Batch>,

    /// The sink side of the repartition, expected to be take during planning.
    sink: Option<PhysicalUnboundedRepartitionSink>,

    /// Number of output partitions.
    output_partitions: usize,

    /// Number of remaining (unfinished) inputs.
    ///
    /// Until this drops to zero, all output partitions will continue producing
    /// batches.
    remaing_inputs: AtomicUsize,
}

impl PhysicalUnboundedRepartition {
    // pub fn new()
}

// impl Source for PhysicalUnboundedRepartition {
//     fn output_partitions(&self) -> usize {
//         self.output_partitions
//     }

//     fn poll_pull(
//         &self,
//         task_cx: &TaskContext,
//         cx: &mut Context,
//         partition: usize,
//     ) -> Poll<Option<Result<DataBatch>>> {
//         unimplemented!()
//     }
// }

impl Explainable for PhysicalUnboundedRepartition {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("PhysicalUnboundedRepartition")
    }
}

#[derive(Debug)]
pub struct PhysicalUnboundedRepartitionSink {
    /// Where to send data batches.
    ///
    /// There's no guarantees on which output partition the data batch will be
    /// sent to.
    send: Sender<Batch>,
}

impl Explainable for PhysicalUnboundedRepartitionSink {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("PhysicalUnboundedRepartitionSink")
    }
}
