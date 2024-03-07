pub mod create_table_as;
pub mod empty_source;
pub mod filter;
pub mod hash_aggregate;
pub mod hash_join;
pub mod hash_repartition;
pub mod nested_loop_join;
pub mod order;
pub mod projection;
pub mod set_var;
pub mod show_var;
pub mod ungrouped_aggregate;
pub mod values;

mod util;

use rayexec_error::Result;
use std::fmt::Debug;
use std::task::{Context, Poll};

use crate::planner::explainable::Explainable;
use crate::types::batch::DataBatch;

use super::TaskContext;

pub trait Sink: Sync + Send + Explainable + Debug {
    /// Number of input partitions this sink can handle.
    fn input_partitions(&self) -> usize;

    fn poll_ready(&self, task_cx: &TaskContext, cx: &mut Context, partition: usize) -> Poll<()>;

    fn push(&self, task_cx: &TaskContext, input: DataBatch, partition: usize) -> Result<()>;

    fn finish(&self, task_cx: &TaskContext, partition: usize) -> Result<()>;
}

pub trait Source: Sync + Send + Explainable + Debug {
    /// Number of output partitions this source can produce.
    fn output_partitions(&self) -> usize;

    fn poll_next(
        &self,
        task_cx: &TaskContext,
        cx: &mut Context,
        partition: usize,
    ) -> Poll<Option<Result<DataBatch>>>;
}

pub trait PhysicalOperator: Sync + Send + Explainable + Debug {
    /// Execute this operator on an input batch.
    fn execute(&self, task_cx: &TaskContext, input: DataBatch) -> Result<DataBatch>;
}
