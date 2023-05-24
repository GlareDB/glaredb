use crate::errors::{PushExecError, Result};
use crate::operator::{Operator, Sink, Source};
use crate::partition::{BufferedPartition, BufferedPartitionStream};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use parking_lot::Mutex;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

pub struct ExecutionPlanAdapater {
    partitions: Vec<Arc<Mutex<BufferedPartition>>>,
}

impl ExecutionPlanAdapater {
    pub fn new(
        plan: Arc<ExecutionPlanAdapater>,
        context: Arc<TaskContext>,
    ) -> Result<ExecutionPlanAdapater> {
        unimplemented!()
    }
}

impl Sink for ExecutionPlanAdapater {
    fn push_partition(&self, input: RecordBatch, partition: usize) -> Result<()> {
        unimplemented!()
    }

    fn finish(&self, partition: usize) -> Result<()> {
        let mut part = self.partitions[partition].lock();
        part.finish();
        Ok(())
    }

    fn input_partitions(&self) -> usize {
        self.partitions.len()
    }
}

impl Source for ExecutionPlanAdapater {
    fn output_partitions(&self) -> usize {
        self.partitions.len()
    }

    fn poll_partition(
        &self,
        cx: &mut Context,
        partition: usize,
    ) -> Poll<Option<Result<RecordBatch>>> {
        unimplemented!()
    }
}
