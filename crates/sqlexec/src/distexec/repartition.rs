use super::pipeline::{Sink, Source};
use super::Result;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::Partitioning;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::task::{Context, Poll, Waker};

#[derive(Debug)]
pub struct RepartitionPipeline {
    output_count: usize,
    state: Mutex<RepartitionState>,
}

impl RepartitionPipeline {
    pub fn try_new(input: Partitioning, output: Partitioning) -> Result<Self> {
        let input_count = input.partition_count();
        let output_count = output.partition_count();
        assert_ne!(input_count, 0);
        assert_ne!(output_count, 0);

        // TODO: metrics support
        let partitioner = BatchPartitioner::try_new(output, Default::default())?;

        let state = Mutex::new(RepartitionState {
            partitioner,
            partition_closed: vec![false; input_count],
            input_closed: false,
            output_buffers: (0..output_count).map(|_| Default::default()).collect(),
        });

        Ok(Self {
            state,
            output_count,
        })
    }
}

struct RepartitionState {
    partitioner: BatchPartitioner,
    partition_closed: Vec<bool>,
    input_closed: bool,
    output_buffers: Vec<OutputBuffer>,
}

impl std::fmt::Debug for RepartitionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RepartitionState")
            .field("partition_closed", &self.partition_closed)
            .field("input_closed", &self.input_closed)
            .finish()
    }
}

impl Sink for RepartitionPipeline {
    fn push(&self, input: RecordBatch, child: usize, partition: usize) -> Result<()> {
        assert_eq!(child, 0);

        let mut state = self.state.lock();
        assert!(
            !state.partition_closed[partition],
            "attempt to push to closed partition {} of RepartitionPipeline({:?})",
            partition, state
        );

        let state = &mut *state;
        state.partitioner.partition(input, |partition, batch| {
            state.output_buffers[partition].push_batch(batch);
            Ok(())
        })?;

        Ok(())
    }

    fn finish(&self, child: usize, partition: usize) -> Result<()> {
        assert_eq!(child, 0);

        let mut state = self.state.lock();
        assert!(
            !state.partition_closed[partition],
            "attempt to close already closed partition {} of RepartitionPipeline({:?})",
            partition, state
        );

        state.partition_closed[partition] = true;

        // If all input streams exhausted, wake outputs
        if state.partition_closed.iter().all(|x| *x) {
            state.input_closed = true;
            for buffer in &mut state.output_buffers {
                if let Some(waker) = buffer.waker.take() {
                    waker.wake();
                }
            }
        }

        Ok(())
    }
}

impl Source for RepartitionPipeline {
    fn output_partitions(&self) -> usize {
        self.output_count
    }

    fn poll_partition(
        &self,
        cx: &mut Context<'_>,
        partition: usize,
    ) -> Poll<Option<Result<RecordBatch>>> {
        let mut state = self.state.lock();
        let input_closed = state.input_closed;
        let buffer = &mut state.output_buffers[partition];

        match buffer.batches.pop_front() {
            Some(batch) => Poll::Ready(Some(Ok(batch))),
            None if input_closed => Poll::Ready(None),
            _ => {
                buffer.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

#[derive(Debug, Default)]
struct OutputBuffer {
    batches: VecDeque<RecordBatch>,
    waker: Option<Waker>,
}

impl OutputBuffer {
    fn push_batch(&mut self, batch: RecordBatch) {
        self.batches.push_back(batch);

        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}
