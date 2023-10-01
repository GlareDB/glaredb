//! Coordinating/scheduling plan execution.
use super::{CoordinatorError, Result};
use tokio::sync::mpsc;
use tracing::{debug, error, info};

const SCHEDULER_MSG_BUFFER: usize = 512;

/// Send messages to the scheduler.
#[derive(Debug, Clone)]
pub struct SchedulerClient {
    send: mpsc::Sender<CoordinatorMessage>,
}

#[derive(Debug)]
pub struct SchedulerService {
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

impl SchedulerService {
    pub fn new() -> Self {
        let (send, recv) = mpsc::channel(SCHEDULER_MSG_BUFFER);
        SchedulerService { send, recv }
    }

    pub fn client(&self) -> SchedulerClient {
        SchedulerClient {
            send: self.send.clone(),
        }
    }

    pub async fn run(mut self) -> Result<()> {
        // Single input channel of execution plans, lockless

        // Route to worker that needs work, checking affinity, single lock

        unimplemented!()
    }
}
