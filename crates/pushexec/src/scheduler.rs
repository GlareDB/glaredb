use crate::errors::{PushExecError, Result};
use rayon::ThreadPool;
use std::sync::Arc;

pub struct Scheduler {
    sync_pool: Arc<ThreadPool>,
}

impl Scheduler {
    pub fn new(sync_pool: Arc<ThreadPool>) -> Scheduler {
        Scheduler { sync_pool }
    }
}
