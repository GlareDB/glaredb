use crate::expr::PhysicalScalarExpression;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::planner::operator::JoinType;
use crate::types::batch::DataBatch;
use arrow::compute::filter_record_batch;
use arrow_array::cast::AsArray;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema};
use rayexec_error::{RayexecError, Result};
use std::task::{Context, Poll, Waker};

/// Join implementation for executing a join between two tables with an
/// arbitrary expression.
///
/// Collects all input from the left side of the join before producing batches.
#[derive(Debug)]
pub struct PhysicalNestedLoopJoin {
    join_type: JoinType,
    on: PhysicalScalarExpression,
}

impl PhysicalNestedLoopJoin {}

// impl Source2 for PhysicalNestedLoopJoin {
//     fn output_partitions(&self) -> usize {
//         unimplemented!()
//     }

//     fn poll_partition(
//         &self,
//         cx: &mut Context<'_>,
//         partition: usize,
//     ) -> Poll<Option<Result<DataBatch>>> {
//         unimplemented!()
//     }
// }

// impl Sink2 for PhysicalNestedLoopJoin {
//     fn push(&self, input: DataBatch, child: usize, partition: usize) -> Result<()> {
//         unimplemented!()
//     }

//     fn finish(&self, child: usize, partition: usize) -> Result<()> {
//         unimplemented!()
//     }
// }

// impl PhysicalOperator2 for PhysicalNestedLoopJoin {}

impl Explainable for PhysicalNestedLoopJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("NestedLoopJoin")
    }
}

/// State dedicated to each partition.
#[derive(Debug)]
enum PartitionLocalState {
    /// Build phase.
    Building {
        /// Waker if there was an attempt to poll this partition prior to the
        /// build phases finishing.
        waker: Option<Waker>,
        /// Accumulated batches so far
        batches: Vec<DataBatch>,
    },
    /// Probe phase.
    Probing {
        // batches: Arc<Vec<DataBatch>>,
    },
}
