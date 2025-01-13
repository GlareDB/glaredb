use std::fmt::Debug;
use std::task::Context;

use dyn_clone::DynClone;
use rayexec_error::Result;

use crate::arrays::batch::Batch;
use crate::execution::operators::{ExecuteInOutState, PollExecute, PollFinalize, PollPush};

pub trait TableInOutFunction: Debug + Sync + Send + DynClone {
    fn create_states(
        &self,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn TableInOutPartitionState>>>;
}

#[derive(Debug)]
pub enum InOutPollPull {
    Batch { batch: Batch, row_nums: Vec<usize> },
    Pending,
    Exhausted,
}

pub trait TableInOutPartitionState: Debug + Sync + Send {
    fn poll_execute(&mut self, cx: &mut Context, inout: ExecuteInOutState) -> Result<PollExecute> {
        unimplemented!()
    }

    fn poll_finalize(&mut self, cx: &mut Context) -> Result<PollFinalize> {
        unimplemented!()
    }

    fn poll_push(&mut self, cx: &mut Context, inputs: Batch) -> Result<PollPush> {
        unimplemented!()
    }

    fn poll_pull(&mut self, cx: &mut Context) -> Result<InOutPollPull> {
        unimplemented!()
    }
}

impl Clone for Box<dyn TableInOutFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

#[cfg(test)]
pub(crate) mod testutil {
    //! Utilities for testing table in/out function states.

    use std::sync::Arc;
    use std::task::{Context, Waker};

    use rayexec_error::Result;

    use super::TableInOutPartitionState;
    use crate::execution::operators::testutil::CountingWaker;
    use crate::execution::operators::{ExecuteInOutState, PollExecute, PollFinalize};

    #[derive(Debug)]
    pub struct StateWrapper {
        pub waker: Arc<CountingWaker>,
        pub state: Box<dyn TableInOutPartitionState>,
    }

    impl StateWrapper {
        pub fn new(state: Box<dyn TableInOutPartitionState>) -> Self {
            StateWrapper {
                waker: Arc::new(CountingWaker::default()),
                state,
            }
        }

        pub fn poll_execute(&mut self, inout: ExecuteInOutState) -> Result<PollExecute> {
            let waker = Waker::from(self.waker.clone());
            let mut cx = Context::from_waker(&waker);
            self.state.poll_execute(&mut cx, inout)
        }

        pub fn poll_finalize(&mut self) -> Result<PollFinalize> {
            let waker = Waker::from(self.waker.clone());
            let mut cx = Context::from_waker(&waker);
            self.state.poll_finalize(&mut cx)
        }
    }
}
