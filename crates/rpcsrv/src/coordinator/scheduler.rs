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

impl SchedulerClient {}

#[derive(Debug)]
pub struct Scheduler {
    send: mpsc::Sender<CoordinatorMessage>,
    recv: mpsc::Receiver<CoordinatorMessage>,
}

/// Messages that can be sent to the scheduler.
#[derive(Debug)]
enum CoordinatorMessage {
    RegisterWorker {},
    DeregisterWorker {},
    HandlePlan {},
}

impl Scheduler {
    pub fn new() -> Self {
        let (send, recv) = mpsc::channel(SCHEDULER_MSG_BUFFER);
        Scheduler { send, recv }
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
