//! Coordinating/scheduling plan execution.
use super::{DistExecError, Result};
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

const COORDINATOR_MSG_BUFFER: usize = 512;

/// Send messages to the coordinator.
#[derive(Debug, Clone)]
pub struct CoordinatorClient {
    send: mpsc::Sender<CoordinatorMessage>,
}

#[derive(Debug)]
pub struct CoordinatorService {
    send: mpsc::Sender<CoordinatorMessage>,
    recv: mpsc::Receiver<CoordinatorMessage>,
}

/// Messages that can be sent to the coordinator service.
#[derive(Debug)]
enum CoordinatorMessage {
    RegisterWorker {},
    DeregisterWorker {},
    HandlePlan {},
}

impl CoordinatorService {
    pub fn new() -> Self {
        let (send, recv) = mpsc::channel(COORDINATOR_MSG_BUFFER);
        CoordinatorService { send, recv }
    }

    pub fn client(&self) -> CoordinatorClient {
        CoordinatorClient {
            send: self.send.clone(),
        }
    }

    pub async fn run(mut self) -> Result<()> {
        // Single input channel of execution plans, lockless

        // Route to worker that needs work, checking affinity, single lock

        unimplemented!()
    }
}
