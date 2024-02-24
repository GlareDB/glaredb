use arrow_array::RecordBatch;
use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};
use std::collections::VecDeque;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use crate::types::batch::DataBatch;

use super::{Sink, Source};

#[derive(Debug)]
pub struct BatchBuffer {
    /// Per-partiton buffers.
    buffers: Arc<Vec<Mutex<PartitionBuffer>>>,
}

impl BatchBuffer {
    pub fn new(num_partitons: usize) -> Self {
        BatchBuffer {
            buffers: Arc::new(
                (0..num_partitons)
                    .map(|_i| Mutex::new(PartitionBuffer::default()))
                    .collect(),
            ),
        }
    }
}

impl Source for BatchBuffer {
    fn output_partitions(&self) -> usize {
        self.buffers.len()
    }

    fn poll_partition(
        &self,
        cx: &mut Context<'_>,
        partition: usize,
    ) -> Poll<Option<Result<DataBatch>>> {
        let mut buffer = self.buffers.get(partition).unwrap().lock();

        // Always check if we have stuff in the buffer first before checking if
        // finished.
        if let Some(batch) = buffer.batches.pop_front() {
            return Poll::Ready(Some(Ok(batch)));
        }

        if buffer.finished {
            return Poll::Ready(None);
        }

        buffer.waker = Some(cx.waker().clone());

        Poll::Pending
    }
}

impl Sink for BatchBuffer {
    fn push(&self, input: DataBatch, child: usize, partition: usize) -> Result<()> {
        if child != 0 {
            return Err(RayexecError::new(format!(
                "non-zero child, push, batch buffer: {child}"
            )));
        }

        let mut buffer = self.buffers.get(partition).unwrap().lock();
        buffer.batches.push_back(input);
        if let Some(waker) = buffer.waker.take() {
            waker.wake();
        }

        Ok(())
    }

    fn finish(&self, child: usize, partition: usize) -> Result<()> {
        if child != 0 {
            return Err(RayexecError::new(format!(
                "non-zero child, finish, batch buffer: {child}"
            )));
        }

        let mut buffer = self.buffers.get(partition).unwrap().lock();
        buffer.finished = true;
        if let Some(waker) = buffer.waker.take() {
            waker.wake();
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
struct PartitionBuffer {
    finished: bool,
    waker: Option<Waker>,
    // TODO: Bounded, will require push to be a "future".
    batches: VecDeque<DataBatch>,
}
