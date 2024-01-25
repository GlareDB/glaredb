use crate::errors::{err, Result};
use crate::expr::PhysicalExpr;
use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};
use std::sync::Arc;
use std::task::{Context, Poll};

use super::{buffer::BatchBuffer, Sink, Source};

#[derive(Debug)]
pub struct Projection {
    exprs: Vec<Box<dyn PhysicalExpr>>,
    schema: Arc<Schema>,
    buffer: BatchBuffer,
}

impl Projection {
    pub fn try_new<S: Into<String>>(
        exprs: Vec<Box<dyn PhysicalExpr>>,
        output_names: Vec<S>,
        input_schema: &Schema,
    ) -> Result<Self> {
        if exprs.len() != output_names.len() {
            return Err(err(format!(
                "numbers of expressions and output names don't match, exprs: {}, names: {}",
                exprs.len(),
                output_names.len()
            )));
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

#[cfg(test)]
mod tests {
    use crate::{
        expr::{
            binary::{Operator, PhysicalBinaryExpr},
            column::ColumnExpr,
            literal::LiteralExpr,
            scalar::ScalarValue,
        },
        physical::plans::test_util::{noop_context, unwrap_poll_partition},
    };

    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field};
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

        // Mix of exprs.
        // Equivalent to something like `SELECT b, a+1, 'hello' FROM ...`
        let exprs: Vec<Box<dyn PhysicalExpr>> = vec![
            Box::new(ColumnExpr::Index(1)),
            Box::new(PhysicalBinaryExpr {
                left: Box::new(ColumnExpr::Index(0)),
                op: Operator::Plus,
                right: Box::new(LiteralExpr::new(ScalarValue::Int32(Some(1)))),
            }),
            Box::new(LiteralExpr::new(ScalarValue::Utf8(Some(
                "hello".to_string(),
            )))),
        ];

        let plan = Projection::try_new(exprs, vec!["col1", "col2", "col3"], &schema).unwrap();

        plan.push(batch1, 0, 0).unwrap();
        plan.push(batch2, 0, 0).unwrap();

        let got1 = unwrap_poll_partition(plan.poll_partition(&mut noop_context(), 0));
        let got2 = unwrap_poll_partition(plan.poll_partition(&mut noop_context(), 0));

        // Both input batches contain the same data, so both outputs should
        // match this expected batch.
        let expected = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("col1", DataType::Utf8, false),
                Field::new("col2", DataType::Int32, false),
                Field::new("col3", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["1", "2", "3", "4", "5"])),
                Arc::new(Int32Array::from(vec![2, 3, 4, 5, 6])),
                Arc::new(StringArray::from(vec![
                    "hello", "hello", "hello", "hello", "hello",
                ])),
            ],
        )
        .unwrap();

        assert_eq!(expected, got1);
        assert_eq!(expected, got2);
    }
}
