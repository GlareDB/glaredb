use datafusion::physical_plan::ExecutionPlan;
use once_cell::sync::OnceCell;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::debug;
use uuid::Uuid;

use super::executor::{LocalTaskExecutor, Task, TaskExecutor};
use super::pipeline::{ErrorSink, PipelineBuilder, QueryPipeline, Sink, Source};
use super::{DistExecError, Result};

#[derive(Debug)]
pub struct OutputSink {
    pub batches: Arc<dyn Sink>,
    pub errors: Arc<dyn ErrorSink>,
}

/// Configuration for running the scheduler.
#[derive(Debug, Clone, Copy)]
pub struct SchedulerConfig {
    pub chan_buffer: usize,
    pub debug_tick_interval: Duration,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        SchedulerConfig {
            chan_buffer: 512,
            debug_tick_interval: Duration::from_secs(60),
        }
    }
}

/// A scheduler for scheduling execution plans.
#[derive(Debug, Clone)]
pub struct Scheduler {
    send: mpsc::Sender<SchedulerMessage>,
}

impl Scheduler {
    /// Create a new scheduler.
    ///
    /// Panics if multiple schedulers are created in the same process.
    pub fn new(conf: SchedulerConfig) -> Self {
        let (send, recv) = mpsc::channel(conf.chan_buffer);
        SchedulerWorker::init_global(conf, recv);
        Self { send }
    }

    /// Schedule a plan for execution.
    pub fn schedule(&self, plan: Arc<dyn ExecutionPlan>, output: OutputSink) -> Result<()> {
        let pipeline = PipelineBuilder::new(plan).build(output.batches, output.errors)?;
        // TODO: take/split and schedule tasks
        unimplemented!()
    }

    pub fn schedule_task(&self, task: Task) {
        self.send
            .try_send(SchedulerMessage::ScheduleTask { task })
            .unwrap();
    }

    pub fn register_local_executor(&self, exec: LocalTaskExecutor) {
        self.send
            .try_send(SchedulerMessage::RegisterLocalExecutor { exec })
            .unwrap();
    }
}

static GLOBAL_WORKER: OnceCell<SchedulerWorker> = OnceCell::new();

/// Messages that can be sent to the scheduler.
#[derive(Debug)]
enum SchedulerMessage {
    RegisterLocalExecutor { exec: LocalTaskExecutor },
    SchedulePipeline { pipeline: QueryPipeline },
    ScheduleTask { task: Task },
    Shutdown,
}

#[derive(Debug)]
struct SchedulerWorker {
    _run_handle: JoinHandle<Result<()>>,
}

impl SchedulerWorker {
    fn init_global(conf: SchedulerConfig, recv: mpsc::Receiver<SchedulerMessage>) {
        let fut = Self::run(conf.clone(), recv);
        let handle = tokio::spawn(fut);
        let worker = Self {
            _run_handle: handle,
        };
        GLOBAL_WORKER
            .set(worker)
            .expect("init global should only be called once");
    }

    async fn run(conf: SchedulerConfig, mut recv: mpsc::Receiver<SchedulerMessage>) -> Result<()> {
        debug!(?conf, "starting scheduler worker");

        let mut registry = TaskRegistry::default();

        let mut interval = tokio::time::interval(conf.debug_tick_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    debug!("scheduler debug tick");
                }

                Some(msg) = recv.recv() => {
                    match msg {
                        SchedulerMessage::Shutdown => {
                            // TODO: We could probably try draining pending work
                            // before shutting down.
                            debug!("scheduler shutting down");
                            return Ok(());
                        }
                        SchedulerMessage::RegisterLocalExecutor { exec } => {
                            registry.push_local_executor(exec)
                        }
                        SchedulerMessage::ScheduleTask { task } => registry.push_task(task),
                        _ => unimplemented!(),
                    }
                }
            }
        }
    }
}

#[derive(Debug, Default)]
struct TaskRegistry {
    tasks: VecDeque<Task>,
    executors: VecDeque<LocalTaskExecutor>,
}

impl TaskRegistry {
    /// Push a task onto the queue.
    ///
    /// This may immediately start execution if an executor is available.
    fn push_task(&mut self, task: Task) {
        if let Some(exec) = self.executors.pop_front() {
            exec.execute(task);
            return;
        }
        self.tasks.push_back(task);
    }

    /// Push a local executor onto the queue.
    ///
    /// May immediately begin execution of a pending task.
    fn push_local_executor(&mut self, exec: LocalTaskExecutor) {
        if let Some(task) = self.tasks.pop_front() {
            exec.execute(task);
            return;
        }
        self.executors.push_back(exec);
    }
}
