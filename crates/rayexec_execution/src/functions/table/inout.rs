use std::fmt::Debug;
use std::task::Context;

use rayexec_bullet::batch::Batch;
use rayexec_error::Result;

use crate::execution::operators::{PollFinalize, PollPull, PollPush};

pub trait TableInOutFunction: Debug + Sync + Send {
    fn create_states(&self, num_partitions: usize) -> Result<Vec<Box<dyn TableInOutState>>>;
}

pub trait TableInOutState: Debug + Sync + Send {
    fn poll_push(&mut self, cx: &mut Context, batch: Batch) -> Result<PollPush>;
    fn poll_finalize_push(&mut self, cx: &mut Context) -> Result<PollFinalize>;
    fn poll_pull(&mut self, cx: &mut Context) -> Result<PollPull>;
}
