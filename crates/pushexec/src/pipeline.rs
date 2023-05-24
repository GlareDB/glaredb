use crate::errors::{PushExecError, Result};
use datafusion::arrow::ipc::RecordBatch;

pub trait Operator: Send {
    fn push_partition(&self, input: RecordBatch, partition: usize) -> Result<()>;

    fn finish(&self, partition: usize) -> Result<()>;
}
