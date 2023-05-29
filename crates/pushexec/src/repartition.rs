use crate::errors::{PushExecError, Result};
use crate::partition::BufferedPartition;
use crate::pipeline::{Pipeline, PushPartitionId, Sink, Source};
use datafusion::{
    arrow::record_batch::RecordBatch,
    physical_plan::{metrics, repartition::BatchPartitioner, Partitioning},
};
use parking_lot::Mutex;
use std::fmt;
use std::task::{Context, Poll, Waker};

/// A repartitioning operator meant to replace Datafusion's repartition/coalesce
/// operators.
pub struct Repartitioner {
    inner: Mutex<Inner>,
    outputs: usize,
    inputs: usize,
}

struct Inner {
    partitioner: BatchPartitioner,

    /// Output partition buffers.
    partitions: Vec<BufferedPartition>,

    /// Number of still open inputs.
    open_inputs: usize,
}

impl Repartitioner {
    pub fn new(input: Partitioning, output: Partitioning) -> Result<Repartitioner> {
        let inputs = input.partition_count();
        let outputs = output.partition_count();

        let partitions: Vec<_> = (0..outputs).map(|_| BufferedPartition::default()).collect();

        let partitioner = BatchPartitioner::try_new(output, metrics::Time::new())?;

        Ok(Repartitioner {
            inner: Mutex::new(Inner {
                partitioner,
                partitions,
                open_inputs: inputs,
            }),
            outputs,
            inputs,
        })
    }
}

impl fmt::Debug for Repartitioner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Repartitioner")
            .field("inputs", &self.inputs)
            .field("outputs", &self.outputs)
            .finish()
    }
}

impl Pipeline for Repartitioner {}

impl Sink for Repartitioner {
    fn push_partition(&self, input: RecordBatch, partition: PushPartitionId) -> Result<()> {
        if partition.child != 0 {
            return Err(PushExecError::Static(
                "Repartitioner only accepts input from a single child",
            ));
        }

        let mut inner = self.inner.lock();
        let inner = &mut *inner;
        inner.partitioner.partition(input, |idx, batch| {
            inner.partitions[idx].push_batch(batch);
            Ok(())
        })?;

        Ok(())
    }

    fn finish(&self, partition: PushPartitionId) -> Result<()> {
        if partition.child != 0 {
            return Err(PushExecError::Static(
                "Repartitioner only accepts input from a single child",
            ));
        }

        let mut inner = self.inner.lock();
        let part = &mut inner.partitions[partition.idx];
        part.finish();

        inner.open_inputs -= 1;

        Ok(())
    }
}

impl Source for Repartitioner {
    fn output_partitions(&self) -> usize {
        self.outputs
    }

    fn poll_partition(
        &self,
        cx: &mut Context,
        partition: usize,
    ) -> Poll<Option<Result<RecordBatch>>> {
        let mut inner = self.inner.lock();
        let finished = inner.open_inputs == 0;
        let part = &mut inner.partitions[partition];
        let batch = part.pop_batch();

        match batch {
            Some(batch) => Poll::Ready(Some(Ok(batch))),
            None if finished => Poll::Ready(None),
            None => {
                part.register_waker(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}
