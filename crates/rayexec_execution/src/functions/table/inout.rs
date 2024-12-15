use std::fmt::Debug;
use std::task::Context;

use dyn_clone::DynClone;
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;

use crate::execution::operators::{PollFinalize, PollPull, PollPush};

pub trait TableInOutFunction: Debug + Sync + Send + DynClone {
    fn create_states(
        &self,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn TableInOutPartitionState>>>;
}

pub trait TableInOutPartitionState: Debug + Sync + Send {
    fn poll_push(&mut self, cx: &mut Context, inputs: Batch) -> Result<PollPush>;
    fn poll_finalize_push(&mut self, cx: &mut Context) -> Result<PollFinalize>;
    fn poll_pull(&mut self, cx: &mut Context) -> Result<PollPull>;
}

impl Clone for Box<dyn TableInOutFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}
