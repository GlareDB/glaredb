use crate::{
    adapter::{ExecutionPlanAdapter, SubPlan},
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

#[derive(Debug)]
pub struct MetaPipeline {
    /// All pipelines needed to execute a query.
    pub pipelines: Vec<LinkedPipeline>,

    /// An optional sink that output should go to.
    // TODO: Make this required?
    pub sink: Option<Box<dyn Sink>>,
}

/// Points to the destination to push a batch.
#[derive(Debug, Clone, Copy)]
pub struct DestinationLink {
    pub pipeline: usize,
    pub child: usize,
}

#[derive(Debug)]
pub struct LinkedPipeline {
    /// The source pipeline.
    pub pipeline: Box<dyn Pipeline>,
    /// Index of the pipeline to push its output to.
    pub dest: Option<DestinationLink>,
}

/// A planner is able to produce a push based execution pipeline from a
/// datafusion physical plan.
#[derive(Debug, Default)]
pub struct Planner {}

impl Planner {
    pub fn plan_from_df_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
        sink: Option<Box<dyn Sink>>,
    ) -> Result<MetaPipeline> {
        let mut state = PlanState {
            pipelines: Vec::new(),
            context,
        };

        state.walk_plan(None, plan)?;

        Ok(MetaPipeline {
            pipelines: state.pipelines,
            sink,
        })
    }
}

struct PlanState {
    pipelines: Vec<LinkedPipeline>,
    context: Arc<TaskContext>,
}

impl PlanState {
    /// Walk the plan depth first and transform datafusion execution plans into
    /// their equivalent pipelines.
    fn walk_plan(
        &mut self,
        dest: Option<DestinationLink>,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<()> {
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
            let pipeline_idx = self.pipelines.len();

            for (child_idx, child) in plan.children().into_iter().enumerate() {
                let dest = DestinationLink {
                    pipeline: pipeline_idx,
                    child: child_idx,
                };
                self.walk_plan(Some(dest), child)?;
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
            let pipeline_idx = self.pipelines.len();

            for (child_idx, child) in plan.children().into_iter().enumerate() {
                let dest = DestinationLink {
                    pipeline: pipeline_idx,
                    child: child_idx,
                };
                self.walk_plan(Some(dest), child)?;
            }
        } else {
            // Adapt execution plans.
            //
            // Splits the execution plan into one or more subplans depending on
            // the number of children it has.
            let mut subplan = SubPlan {
                root: plan.clone(),
                depth: 0,
            };

            // Keep adding plans with a single child to the subplan.
            let mut curr = plan;
            let mut children = curr.children();
            loop {
                match children.len() {
                    1 => {
                        // Add child to subplan, and keep going.
                        curr = children.pop().unwrap();
                        children = curr.children();
                        subplan.depth += 1;
                    }
                    _ => {
                        // At the end of the subplan. Need to either stop
                        // planning alltogether, or need to put children in
                        // their own subplans.
                        break;
                    }
                }
            }

            // Create adapter with subplan.
            let plan = ExecutionPlanAdapter::new(subplan, self.context.clone())?;
            self.pipelines.push(LinkedPipeline {
                pipeline: Box::new(plan),
                dest,
            });
            let pipeline_idx = self.pipelines.len();

            // Walk children of the plan we stopped at, pointing back up to the
            // adapter pipeline we just created.
            for (child_idx, child) in children.into_iter().enumerate() {
                let dest = DestinationLink {
                    pipeline: pipeline_idx,
                    child: child_idx,
                };

                self.walk_plan(Some(dest), child)?;
            }
        }

        Ok(())
    }
}
