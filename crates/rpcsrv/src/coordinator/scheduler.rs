//! Coordinating/scheduling plan execution.
use super::{CoordinatorError, Result};
use datafusion::physical_plan::ExecutionPlan;
use sqlexec::remote::exchange_stream::ExecutionBatchStream;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Configuration for running the scheduler.
#[derive(Debug, Clone, Copy)]
struct SchedulerConfig {
    pub chan_buffer: usize,
    pub work_buffer: usize,
    pub debug_tick_interval: Duration,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        SchedulerConfig {
            chan_buffer: 8,
            work_buffer: 512,
            debug_tick_interval: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StagedPlan {
    /// Unique identifier for this peice of work.
    pub work_id: Uuid,
    /// Plan to execute.
    pub plan: Arc<dyn ExecutionPlan>,
}

/// Send messages to the scheduler.
#[derive(Debug, Clone)]
pub struct SchedulerClient {
    send: mpsc::Sender<SchedulerMessage>,
}

impl SchedulerClient {
    /// Schedule a plan for execution.
    pub async fn schedule_plan(&self, plan: StagedPlan) -> Result<()> {
        self.send_msg(SchedulerMessage::SchedulePlan { plan }).await
    }

    /// Put a channel for receiving a batch stream once it's available.
    pub async fn put_batch_stream_channel(
        &self,
        work_id: Uuid,
        tx: oneshot::Sender<ExecutionBatchStream>,
    ) -> Result<()> {
        self.send_msg(SchedulerMessage::PutBatchStreamChannel { work_id, tx })
            .await
    }

    /// Put a channel for receiving work.
    pub async fn put_work_channel(&self, tx: oneshot::Sender<Option<StagedPlan>>) -> Result<()> {
        self.send_msg(SchedulerMessage::PutWorkChannel { tx }).await
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.send_msg(SchedulerMessage::Shutdown).await
    }

    async fn send_msg(&self, msg: SchedulerMessage) -> Result<()> {
        if self.send.send(msg).await.is_err() {
            return Err(CoordinatorError::String(
                "failed to send scheduled message".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug)]
enum PendingStream {
    StreamArrivedFirst(ExecutionBatchStream),
    WaitingForStream(oneshot::Sender<ExecutionBatchStream>),
}

#[derive(Debug)]
pub struct Scheduler {
    conf: SchedulerConfig,

    send: mpsc::Sender<SchedulerMessage>,
    recv: mpsc::Receiver<SchedulerMessage>,

    /// Buffer of work yet to do be done.
    ///
    /// Storing these in a separate vec rather than in the channel lets us
    /// implement prioritization and affinity tracking logic.
    // TODO: Work item instead of just staged plan.
    work_buf: VecDeque<StagedPlan>,

    /// Track streams that have been sent back by workers, _or_ streams that the
    /// client are waiting on.
    pending_streams: HashMap<Uuid, PendingStream>,
}

/// Messages that can be sent to the scheduler.
#[derive(Debug)]
enum SchedulerMessage {
    RegisterWorker {
        node_id: Uuid,
    },
    DeregisterWorker {
        node_id: Uuid,
    },
    SchedulePlan {
        plan: StagedPlan,
    },
    PutBatchStreamChannel {
        work_id: Uuid,
        tx: oneshot::Sender<ExecutionBatchStream>,
    },
    PutWorkChannel {
        tx: oneshot::Sender<Option<StagedPlan>>,
    },
    Shutdown,
}

impl Scheduler {
    pub fn new() -> Self {
        let conf = SchedulerConfig::default();
        let (send, recv) = mpsc::channel(conf.chan_buffer);
        Scheduler {
            conf,
            send,
            recv,
            work_buf: VecDeque::with_capacity(conf.work_buffer),
            pending_streams: HashMap::new(),
        }
    }

    pub fn client(&self) -> SchedulerClient {
        SchedulerClient {
            send: self.send.clone(),
        }
    }

    pub async fn run(mut self) -> Result<()> {
        debug!("starting scheduler");

        let mut interval = tokio::time::interval(self.conf.debug_tick_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    debug!("scheduler debug tick");
                }

                Some(msg) = self.recv.recv() => {
                    if let SchedulerMessage::Shutdown = msg {
                        // TODO: We could probably try draining pending work
                        // before shutting down.
                        debug!("scheduler shutting down");
                        return Ok(());
                    }
                    self.handle_msg(msg).await;
                }
            }
        }
    }

    async fn handle_msg(&mut self, msg: SchedulerMessage) {
        match msg {
            SchedulerMessage::RegisterWorker { node_id } => {
                debug!(%node_id, "register worker called");
            }
            SchedulerMessage::DeregisterWorker { node_id } => {
                debug!(%node_id, "deregister worker called");
            }
            SchedulerMessage::SchedulePlan { plan } => {
                if self.work_buf.len() >= self.conf.work_buffer {
                    // TODO: We probably want to get this back to the client...
                    error!(len = %self.work_buf.len(), "work buffer is at limit, plan being dropped");
                }
                self.work_buf.push_back(plan);
            }
            SchedulerMessage::PutWorkChannel { tx } => {
                let work = self.work_buf.pop_front();
                if tx.send(work).is_err() {
                    warn!("receiver for work channel dropped");
                }
            }
            SchedulerMessage::PutBatchStreamChannel { work_id, tx } => {
                if let Some(PendingStream::StreamArrivedFirst(stream)) =
                    self.pending_streams.remove(&work_id)
                {
                    if tx.send(stream).is_err() {
                        warn!("receiver for batch stream channel dropped");
                    }
                    return;
                }

                // TODO: Should we allow putting a oneshot channel twice?

                self.pending_streams
                    .insert(work_id, PendingStream::WaitingForStream(tx));
            }
            SchedulerMessage::Shutdown => {
                unreachable!("shutdown should have been handled already")
            }
        }
    }
}
