pub mod materialize;
pub mod modify;
pub mod session;
pub mod vars;

use rayexec_error::{RayexecError, Result};
use rayon::{ThreadPool, ThreadPoolBuilder};
use session::Session;
use std::sync::Arc;

use crate::physical::scheduler::Scheduler;

#[derive(Debug)]
pub struct Engine {
    thread_pool: Arc<ThreadPool>,
}

impl Engine {
    pub fn try_new() -> Result<Self> {
        let thread_pool = ThreadPoolBuilder::new()
            .build()
            .map_err(|e| RayexecError::with_source("Failed to build thread pool", Box::new(e)))?;

        Ok(Engine {
            thread_pool: Arc::new(thread_pool),
        })
    }

    pub fn new_session(&self) -> Result<Session> {
        let scheduler = Scheduler::new(self.thread_pool.clone());
        Ok(Session::new(scheduler))
    }
}
