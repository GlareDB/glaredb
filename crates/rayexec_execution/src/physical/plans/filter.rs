use crate::expr::{Expression, PhysicalScalarExpression};
use crate::physical::PhysicalOperator;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::types::batch::{DataBatch, DataBatchSchema};
use arrow::compute::{filter, FilterBuilder};
use arrow_array::cast::AsArray;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema};
use rayexec_error::{RayexecError, Result, ResultExt};
use std::task::{Context, Poll};
use tracing::trace;

use super::{buffer::BatchBuffer, Sink, Source};

#[derive(Debug)]
pub struct PhysicalFilter {
    predicate: PhysicalScalarExpression,
    buffer: BatchBuffer,
}

impl PhysicalFilter {
    pub fn try_new(predicate: PhysicalScalarExpression) -> Result<Self> {
        trace!(?predicate, "creating physical filter");
        Ok(PhysicalFilter {
            predicate,
            buffer: BatchBuffer::new(1),
        })
    }
}

impl Source for PhysicalFilter {
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

impl Sink for PhysicalFilter {
    fn push(&self, input: DataBatch, child: usize, partition: usize) -> Result<()> {
        if child != 0 {
            return Err(RayexecError::new(format!(
                "non-zero child, push, filter: {child}"
            )));
        }

        let selection = self.predicate.eval(&input)?;
        // TODO: Need to check that this is actually a boolean somewhere.
        let selection = selection.as_boolean();

        let filtered_arrays = input
            .columns()
            .iter()
            .map(|a| filter(a, &selection))
            .collect::<Result<Vec<_>, _>>()?;

        let batch = if filtered_arrays.is_empty() {
            // If we're working on an empty input batch, just produce an new
            // empty batch with num rows equaling the number of trues in the
            // selection.
            DataBatch::empty_with_num_rows(selection.true_count())
        } else {
            // Otherwise use the actual filtered arrays.
            DataBatch::try_new(filtered_arrays)?
        };

        self.buffer.push(batch, 0, partition)
    }

    fn finish(&self, child: usize, partition: usize) -> Result<()> {
        self.buffer.finish(child, partition)
    }
}

impl PhysicalOperator for PhysicalFilter {}

impl Explainable for PhysicalFilter {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Filter")
    }
}

// #[cfg(test)]
// mod tests {
//     use crate::{
//         expr::{
//             binary::{BinaryOperator, PhysicalBinaryExpr},
//             column::ColumnIndex,
//             literal::LiteralExpr,
//             scalar::ScalarValue,
//         },
//         physical::plans::test_util::{noop_context, unwrap_poll_partition},
//     };

//     use super::*;
//     use arrow_array::{Int32Array, StringArray};
//     use std::sync::Arc;

//     fn test_batch() -> DataBatch {
//         DataBatch::try_new(vec![
//             Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
//             Arc::new(StringArray::from(vec!["1", "2", "3", "4", "5"])),
//         ])
//         .unwrap()
//     }

//     // #[test]
//     // fn basic() {
//     //     let batch1 = test_batch();
//     //     let batch2 = test_batch();
//     //     let schema = batch1.schema();

//     //     let pred = PhysicalBinaryExpr {
//     //         left: Box::new(ColumnIndex(0)),
//     //         op: BinaryOperator::Gt,
//     //         right: Box::new(LiteralExpr::new(ScalarValue::Int32(3))),
//     //     };

//     //     let plan = PhysicalFilter::try_new(Box::new(pred), &schema).unwrap();

//     //     plan.push(batch1, 0, 0).unwrap();
//     //     plan.push(batch2, 0, 0).unwrap();

//     //     let got1 = unwrap_poll_partition(plan.poll_partition(&mut noop_context(), 0));
//     //     let got2 = unwrap_poll_partition(plan.poll_partition(&mut noop_context(), 0));

//     //     // Both input batches contain the same data, so both outputs should
//     //     // match this expected batch.
//     //     let expected = DataBatch::try_new(vec![
//     //         Arc::new(Int32Array::from(vec![4, 5])),
//     //         Arc::new(StringArray::from(vec!["4", "5"])),
//     //     ])
//     //     .unwrap();

//     //     assert_eq!(expected, got1);
//     //     assert_eq!(expected, got2);
//     // }
// }
