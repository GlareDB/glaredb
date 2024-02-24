use crate::expr::execute::ScalarExecutor;
use crate::expr::{Expression, PhysicalExpr};
use crate::physical::PhysicalOperator;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::types::batch::{DataBatch, DataBatchSchema};
use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};
use rayexec_error::{RayexecError, Result};
use std::sync::Arc;
use std::task::{Context, Poll};

use super::{buffer::BatchBuffer, Sink, Source};

#[derive(Debug)]
pub struct PhysicalProjection {
    exprs: Vec<ScalarExecutor>,
    buffer: BatchBuffer,
}

impl PhysicalProjection {
    pub fn try_new(exprs: Vec<Expression>) -> Result<Self> {
        let exprs = exprs
            .into_iter()
            .map(|expr| ScalarExecutor::try_new(expr))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(PhysicalProjection {
            exprs,
            buffer: BatchBuffer::new(1),
        })
    }
}

impl Source for PhysicalProjection {
    fn output_partitions(&self) -> usize {
        self.buffer.output_partitions()
    }

    fn poll_partition(
        &self,
        cx: &mut Context<'_>,
        partition: usize,
    ) -> Poll<Option<Result<DataBatch>>> {
        self.buffer.poll_partition(cx, partition)
    }
}

impl Sink for PhysicalProjection {
    fn push(&self, input: DataBatch, child: usize, partition: usize) -> Result<()> {
        if child != 0 {
            return Err(RayexecError::new(format!(
                "non-zero child, push, projection: {child}"
            )));
        }

        let arrs = self
            .exprs
            .iter()
            .map(|expr| expr.eval(&input))
            .collect::<Result<Vec<_>>>()?;

        let batch = DataBatch::try_new(arrs)?;
        self.buffer.push(batch, 0, partition)?;

        Ok(())
    }

    fn finish(&self, child: usize, partition: usize) -> Result<()> {
        self.buffer.finish(child, partition)
    }
}

impl PhysicalOperator for PhysicalProjection {}

impl Explainable for PhysicalProjection {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Projection")
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        expr::{
            binary::{BinaryOperator, PhysicalBinaryExpr},
            column::ColumnIndex,
            literal::LiteralExpr,
            scalar::ScalarValue,
        },
        physical::plans::test_util::{noop_context, unwrap_poll_partition},
    };

    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use std::sync::Arc;

    fn test_batch() -> DataBatch {
        DataBatch::try_new(vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["1", "2", "3", "4", "5"])),
        ])
        .unwrap()
    }

    // #[test]
    // fn basic() {
    //     let batch1 = test_batch();
    //     let batch2 = test_batch();

    //     // Mix of exprs.
    //     // Equivalent to something like `SELECT b, a+1, 'hello' FROM ...`
    //     let exprs: Vec<Box<dyn PhysicalExpr>> = vec![
    //         Box::new(ColumnIndex(1)),
    //         Box::new(PhysicalBinaryExpr {
    //             left: Box::new(ColumnIndex(0)),
    //             op: BinaryOperator::Plus,
    //             right: Box::new(LiteralExpr::new(ScalarValue::Int32(1))),
    //         }),
    //         Box::new(LiteralExpr::new(ScalarValue::Utf8("hello".to_string()))),
    //     ];

    //     let plan = PhysicalProjection::try_new(exprs).unwrap();

    //     plan.push(batch1, 0, 0).unwrap();
    //     plan.push(batch2, 0, 0).unwrap();

    //     let got1 = unwrap_poll_partition(plan.poll_partition(&mut noop_context(), 0));
    //     let got2 = unwrap_poll_partition(plan.poll_partition(&mut noop_context(), 0));

    //     // Both input batches contain the same data, so both outputs should
    //     // match this expected batch.
    //     let expected = DataBatch::try_new(vec![
    //         Arc::new(StringArray::from(vec!["1", "2", "3", "4", "5"])),
    //         Arc::new(Int32Array::from(vec![2, 3, 4, 5, 6])),
    //         Arc::new(StringArray::from(vec![
    //             "hello", "hello", "hello", "hello", "hello",
    //         ])),
    //     ])
    //     .unwrap();

    //     assert_eq!(expected, got1);
    //     assert_eq!(expected, got2);
    // }
}
