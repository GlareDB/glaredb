use std::fmt::Debug;
use std::task::Context;

use dyn_clone::DynClone;
use rayexec_error::Result;

use crate::arrays::batch::Batch2;
use crate::execution::operators::{PollFinalize2, PollPush2};

pub trait TableInOutFunction: Debug + Sync + Send + DynClone {
    fn create_states(
        &self,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn TableInOutPartitionState>>>;
}

#[derive(Debug)]
pub enum InOutPollPull {
    Batch { batch: Batch2, row_nums: Vec<usize> },
    Pending,
    Exhausted,
}

pub trait TableInOutPartitionState: Debug + Sync + Send {
    fn poll_push(&mut self, cx: &mut Context, inputs: Batch2) -> Result<PollPush2>;
    fn poll_finalize_push(&mut self, cx: &mut Context) -> Result<PollFinalize2>;
    fn poll_pull(&mut self, cx: &mut Context) -> Result<InOutPollPull>;
}

impl Clone for Box<dyn TableInOutFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}
