use crate::errors::{err, Result};
use crate::expr::PhysicalExpr;
use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use super::{buffer::BatchBuffer, Sink, Source};

#[derive(Debug)]
pub struct Projection {
    exprs: Vec<Box<dyn PhysicalExpr>>,
    schema: Arc<Schema>,
    buffer: BatchBuffer,
}

impl Projection {
    pub fn try_new(
        exprs: Vec<Box<dyn PhysicalExpr>>,
        output_names: Vec<String>,
        input_schema: &Schema,
    ) -> Result<Self> {
        if exprs.len() != output_names.len() {
            return Err(err(format!("numbers of expressions and output names don't match, exprs: {exprs:?}, names: {output_names:?}")));
        }

        let fields = exprs
            .iter()
            .zip(output_names.into_iter())
            .map(|(expr, name)| {
                let dt = expr.data_type(input_schema)?;
                let nullable = expr.nullable(input_schema)?;
                Ok(Field::new(name, dt, nullable))
            })
            .collect::<Result<Vec<_>>>()?;

        let schema = Schema::new(fields);

        Ok(Projection {
            exprs,
            schema: Arc::new(schema),
            buffer: BatchBuffer::new(1),
        })
    }
}

impl Source for Projection {
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

impl Sink for Projection {
    fn push(&self, input: RecordBatch, child: usize, partition: usize) -> Result<()> {
        if child != 0 {
            return Err(err(format!("non-zero child, push, projection: {child}")));
        }

        let arrs = self
            .exprs
            .iter()
            .map(|expr| expr.eval(&input))
            .collect::<Result<Vec<_>>>()?;

        let batch = RecordBatch::try_new(self.schema.clone(), arrs)?;
        self.buffer.push(batch, 0, partition)?;

        Ok(())
    }

    fn finish(&self, child: usize, partition: usize) -> Result<()> {
        self.buffer.finish(child, partition)
    }
}
