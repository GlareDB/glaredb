use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;

use std::sync::Arc;

use super::executor::{Task, TaskExecutor};
use super::pipeline::{ErrorSink, PipelineBuilder, Sink};
use super::Result;

#[derive(Debug)]
pub struct OutputSink {
    pub batches: Arc<dyn Sink>,
    pub errors: Arc<dyn ErrorSink>,
}

/// A scheduler for scheduling execution plans.
#[derive(Debug, Clone)]
pub struct Scheduler {
    task_sender: async_channel::Sender<Task>,
}

impl Scheduler {
    /// Create a new scheduler.
    ///
    /// Returns an executor builder along-with to create different kinds of
    /// task executors.
    pub fn new() -> (Self, ExecutorBuilder) {
        let (task_sender, task_receiver) = async_channel::unbounded();
        (Self { task_sender }, ExecutorBuilder { task_receiver })
    }

    /// Schedule a plan for execution.
    pub fn schedule(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
        output: OutputSink,
    ) -> Result<()> {
        let pipeline = PipelineBuilder::new(plan, context)
            .build(output.batches.clone(), output.errors.clone())?;

        for stage in pipeline.stages.iter() {
            for partition in 0..stage.pipeline.output_partitions() {
                let sink = match stage.output {
                    Some(link) => pipeline.stages[link.pipeline].pipeline.clone().as_sink(),
                    None => output.batches.clone(),
                };

                let task = Task {
                    scheduler: self.clone(),
                    source: stage.pipeline.clone().as_source(),
                    output: sink,
                    errors: output.errors.clone(),
                    child: stage.output.map(|o| o.child).unwrap_or(0),
                    partition,
                };

                self.schedule_task(task);
            }
        }

        Ok(())
    }

    /// Schedules a new task. Can `await` on this if the buffer is full.
    pub fn schedule_task(&self, task: Task) {
        self.task_sender
            .try_send(task)
            .expect("should always be able to send task to unbounded queue");
    }
}

/// Helper to create executors.
#[derive(Debug)]
pub struct ExecutorBuilder {
    task_receiver: async_channel::Receiver<Task>,
}

impl ExecutorBuilder {
    /// Create an executor that only receives messages from a local scheduler.
    pub fn build_only_local(&self) -> TaskExecutor {
        TaskExecutor::new(self.task_receiver.clone(), None)
    }
}
