use crate::errors::{err, Result};
use crate::expr::PhysicalExpr;
use arrow::compute::filter_record_batch;
use arrow_array::cast::AsArray;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema};
use std::task::{Context, Poll};

use super::{buffer::BatchBuffer, Sink, Source};

#[derive(Debug)]
pub struct Filter {
    predicate: Box<dyn PhysicalExpr>,
    buffer: BatchBuffer,
}

impl Filter {
    pub fn try_new(predicate: Box<dyn PhysicalExpr>, input_schema: &Schema) -> Result<Self> {
        let ret_type = predicate.data_type(input_schema)?;
        if ret_type != DataType::Boolean {
            return Err(err("expr {expr} does not return a boolean"));
        }

        Ok(Filter {
            predicate,
            buffer: BatchBuffer::new(1),
        })
    }
}

impl Source for Filter {
    fn output_partitions(&self) -> usize {
        self.buffer.output_partitions()
    }

    fn poll_partition(
        &self,
        cx: &mut Context<'_>,
        partition: usize,
    ) -> Poll<Option<Result<RecordBatch>>> {
        self.buffer.poll_partition(cx, partition)
    }
}

impl Sink for Filter {
    fn push(&self, input: RecordBatch, child: usize, partition: usize) -> Result<()> {
        if child != 0 {
            return Err(err(format!("non-zero child, push, filter: {child}")));
        }

        let selection = self.predicate.eval(&input)?;
        // Safe since we should have checked the data type during filter construction.
        let selection = selection.as_boolean();
        let filtered = filter_record_batch(&input, selection)?;

        self.buffer.push(filtered, 0, partition)
    }

    fn finish(&self, child: usize, partition: usize) -> Result<()> {
        self.buffer.finish(child, partition)
    }
}
