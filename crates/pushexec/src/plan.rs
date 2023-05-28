use crate::{
    errors::{PushExecError, Result},
    pipeline::{Pipeline, Sink},
    repartition::Repartitioner,
};
use datafusion::{
    execution::context::TaskContext,
    physical_plan::{
        coalesce_partitions::CoalescePartitionsExec, repartition::RepartitionExec, ExecutionPlan,
        Partitioning,
    },
};
use std::sync::Arc;
use tracing::debug;

pub struct PipelineEdge {
    pipeline: Box<dyn Pipeline>,
    dest: usize,
}

pub struct MetaPipeline {}

/// A planner is able to produce a push based execution pipeline from a
/// datafusion physical plan.
pub struct Planner {}

impl Planner {
    pub fn new() -> Planner {
        Planner {}
    }

    pub fn plan_from_df_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> Result<MetaPipeline> {
        unimplemented!()
    }
}

struct LinkedPipeline {
    pipeline: Box<dyn Pipeline>,
    dest: Option<usize>,
}

struct PlanState {
    pipelines: Vec<LinkedPipeline>,
}

impl PlanState {
    /// Walk the plan depth first and transform datafusion execution plans into
    /// their equivalent pipelines.
    fn walk_plan(&mut self, dest: Option<usize>, plan: Arc<dyn ExecutionPlan>) -> Result<()> {
        if let Some(plan) = plan.as_any().downcast_ref::<RepartitionExec>() {
            // Replace repartition execs with the pipeline impl.
            debug!("replacing repartition exec");

            let pipeline = Repartitioner::new(
                plan.input().output_partitioning(),
                plan.output_partitioning(),
            )?;
            self.pipelines.push(LinkedPipeline {
                pipeline: Box::new(pipeline),
                dest,
            });
            let dest = Some(self.pipelines.len());

            for child in plan.children() {
                self.walk_plan(dest, child)?;
            }
        } else if let Some(plan) = plan.as_any().downcast_ref::<CoalescePartitionsExec>() {
            // Replace coalesce partition exec with the pipeline impl.
            debug!("replacing coalesce partitions exec");

            let pipeline = Repartitioner::new(
                plan.input().output_partitioning(),
                Partitioning::RoundRobinBatch(1),
            )?;
            self.pipelines.push(LinkedPipeline {
                pipeline: Box::new(pipeline),
                dest,
            });
            let dest = Some(self.pipelines.len());

            for child in plan.children() {
                self.walk_plan(dest, child)?;
            }
        } else {
            unimplemented!()
        }
        unimplemented!()
    }
}
