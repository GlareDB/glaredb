use crate::errors::Result;
use crate::plan::{PipelinePlan, PipelinePlanner, RoutablePipeline};
use crate::task::ExecutionResults;
use crate::task::{spawn_plan, Task};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::sync::Arc;

pub struct Scheduler {
    pool: Arc<ThreadPool>,
}

impl Scheduler {
    pub fn new(pool: Arc<ThreadPool>) -> Self {
        Scheduler { pool }
    }

    pub fn schedule(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> Result<ExecutionResults> {
        let plan = PipelinePlanner::new(plan, context).build()?;
        Ok(self.schedule_plan(plan))
    }

    pub(crate) fn schedule_plan(&self, plan: PipelinePlan) -> ExecutionResults {
        spawn_plan(plan, self.spawner())
    }

    fn spawner(&self) -> Spawner {
        Spawner {
            pool: self.pool.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Spawner {
    pool: Arc<ThreadPool>,
}

impl Spawner {
    pub fn spawn(&self, task: Task) {
        self.pool.spawn(move || task.do_work());
    }

    pub fn spawn_fifo(&self, task: Task) {
        self.pool.spawn_fifo(move || task.do_work());
    }
}
