//! Coordinating/scheduling plan execution.
use super::{DistExecError, Result};

pub struct CoordinatorClient {}

pub struct CoordinatorService {}

impl CoordinatorService {
    pub fn new() -> Self {
        unimplemented!()
    }

    pub fn client(&self) -> CoordinatorClient {
        unimplemented!()
    }

    pub async fn run(mut self) -> Result<()> {
        // Single input channel of execution plans, lockless

        // Route to worker that needs work, checking affinity, single lock

        unimplemented!()
    }
}
