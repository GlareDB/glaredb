use crate::expr::PhysicalExpr;
use crate::logical::explainable::{ExplainConfig, ExplainEntry, Explainable};
use arrow::compute::filter_record_batch;
use arrow_array::cast::AsArray;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema};
use rayexec_error::{RayexecError, Result, ResultExt};
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
            return Err(RayexecError::new(format!(
                "expr {predicate} does not return a boolean"
            )));
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
            return Err(RayexecError::new(format!(
                "non-zero child, push, filter: {child}"
            )));
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

impl Explainable for Filter {
    fn explain_entry(_conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Filter")
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        expr::{
            binary::{BinaryOperator, PhysicalBinaryExpr},
            column::ColumnExpr,
            literal::LiteralExpr,
            scalar::ScalarValue,
        },
        physical::plans::test_util::{noop_context, unwrap_poll_partition},
    };

    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::Field;
    use std::sync::Arc;

    fn test_batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["1", "2", "3", "4", "5"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn basic() {
        let batch1 = test_batch();
        let batch2 = test_batch();
        let schema = batch1.schema();

        let pred = PhysicalBinaryExpr {
            left: Box::new(ColumnExpr::Index(0)),
            op: BinaryOperator::Gt,
            right: Box::new(LiteralExpr::new(ScalarValue::Int32(3))),
        };

        let plan = Filter::try_new(Box::new(pred), &schema).unwrap();

        plan.push(batch1, 0, 0).unwrap();
        plan.push(batch2, 0, 0).unwrap();

        let got1 = unwrap_poll_partition(plan.poll_partition(&mut noop_context(), 0));
        let got2 = unwrap_poll_partition(plan.poll_partition(&mut noop_context(), 0));

        // Both input batches contain the same data, so both outputs should
        // match this expected batch.
        let expected = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![4, 5])),
                Arc::new(StringArray::from(vec!["4", "5"])),
            ],
        )
        .unwrap();

        assert_eq!(expected, got1);
        assert_eq!(expected, got2);
    }
}
