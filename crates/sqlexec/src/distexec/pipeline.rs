use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use std::sync::Arc;
use std::task::Poll;
use std::{fmt::Debug, task::Context};

use super::adapter::{AdapterPipeline, SplicedPlan};
use super::repartition::RepartitionPipeline;
use super::{DistExecError, Result};

pub trait Pipeline: Source + Sink {
    fn as_sink(self: Arc<Self>) -> Arc<dyn Sink>;
    fn as_source(self: Arc<Self>) -> Arc<dyn Source>;
}

impl<P: Source + Sink + 'static> Pipeline for P {
    fn as_sink(self: Arc<Self>) -> Arc<dyn Sink> {
        self
    }

    fn as_source(self: Arc<Self>) -> Arc<dyn Source> {
        self
    }
}

pub trait Source: Send + Sync + Debug {
    fn output_partitions(&self) -> usize;
    fn poll_partition(
        &self,
        cx: &mut Context<'_>,
        partition: usize,
    ) -> Poll<Option<Result<RecordBatch>>>;
}

pub trait Sink: Send + Sync + Debug {
    fn push(&self, input: RecordBatch, child: usize, partition: usize) -> Result<()>;
    fn finish(&self, child: usize, partition: usize) -> Result<()>;
}

pub trait ErrorSink: Send + Sync + Debug {
    fn push_error(&self, err: DistExecError, partition: usize) -> Result<()>;
}

/// A complete query pipeline representing a single execution plan.
#[derive(Debug)]
pub struct QueryPipeline {
    /// Output schema.
    pub schema: Arc<Schema>,

    /// Number of output partitions.
    pub output_partitions: usize,

    /// Intermediate pipelines for this query.
    pub stages: Vec<PipelineStage>,

    /// Output sink for the final result stream.
    pub sink: Arc<dyn Sink>,

    /// Output errors.
    pub error_sink: Arc<dyn ErrorSink>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OutputLink {
    pub pipeline: usize,
    pub child: usize,
}

/// A part of a full query pipeline.
///
/// Represents a unit of work that should send its results to some output.
#[derive(Debug, Clone)]
pub struct PipelineStage {
    pub pipeline: Arc<dyn Pipeline>,
    pub output: Option<OutputLink>,
}

struct OperatorGroup {
    output: Option<OutputLink>,
    root: Arc<dyn ExecutionPlan>,
    depth: usize,
}

pub struct PipelineBuilder {
    schema: Arc<Schema>,
    output_partitions: usize,
    completed: Vec<PipelineStage>,
    to_visit: Vec<(Arc<dyn ExecutionPlan>, Option<OutputLink>)>,
    execution_operators: Option<OperatorGroup>,
    context: Arc<TaskContext>,
}

impl PipelineBuilder {
    pub fn new(plan: Arc<dyn ExecutionPlan>, context: Arc<TaskContext>) -> Self {
        let schema = plan.schema();
        let output_partitions = plan.output_partitioning().partition_count();
        Self {
            schema,
            output_partitions,
            completed: Vec::new(),
            to_visit: vec![(plan, None)],
            execution_operators: None,
            context,
        }
    }

    /// Flush the current group of operators stored in `execution_operators`.
    fn flush_exec(&mut self) -> Result<usize> {
        let group = self.execution_operators.take().unwrap();
        let node_idx = self.completed.len();
        let spliced = SplicedPlan::new_from_plan(group.root, group.depth, self.context.clone())?;
        self.completed.push(PipelineStage {
            pipeline: Arc::new(AdapterPipeline::new(spliced)),
            output: group.output,
        });
        Ok(node_idx)
    }

    /// Add the given list of children to the stack of `ExecutionPlan` to visit
    fn enqueue_children(&mut self, children: Vec<Arc<dyn ExecutionPlan>>, parent_node_idx: usize) {
        for (child_idx, child) in children.into_iter().enumerate() {
            self.to_visit.push((
                child,
                Some(OutputLink {
                    pipeline: parent_node_idx,
                    child: child_idx,
                }),
            ))
        }
    }

    /// Push a new pipeline and enqueue its children to be visited
    fn push_pipeline(&mut self, node: PipelineStage, children: Vec<Arc<dyn ExecutionPlan>>) {
        let node_idx = self.completed.len();
        self.completed.push(node);
        self.enqueue_children(children, node_idx)
    }

    /// Visit a non-special cased `ExecutionPlan`
    fn visit_exec(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
        parent: Option<OutputLink>,
    ) -> Result<()> {
        let children = plan.children();

        // Add the operator to the current group of operators to be combined
        // into a single `ExecutionPipeline`.
        //
        // TODO: More sophisticated policy, just because we can combine them doesn't mean we should
        match self.execution_operators.as_mut() {
            Some(buffer) => {
                assert_eq!(parent, buffer.output, "PipelineBuilder out of sync");
                buffer.depth += 1;
            }
            None => {
                self.execution_operators = Some(OperatorGroup {
                    output: parent,
                    root: plan,
                    depth: 0,
                })
            }
        }

        match children.len() {
            1 => {
                // Enqueue the children with the parent of the `OperatorGroup`
                self.to_visit
                    .push((children.into_iter().next().unwrap(), parent))
            }
            _ => {
                // We can only recursively group through nodes with a single child, therefore
                // if this node has multiple children, we now need to flush the buffer and
                // enqueue its children with this new pipeline as its parent
                let node = self.flush_exec()?;
                self.enqueue_children(children, node);
            }
        }

        Ok(())
    }

    /// Push a new `RepartitionPipeline` first flushing any buffered
    /// `OperatorGroup`.
    fn push_repartition(
        &mut self,
        input: Partitioning,
        output: Partitioning,
        parent: Option<OutputLink>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<()> {
        let parent = match &self.execution_operators {
            Some(buffer) => {
                assert_eq!(buffer.output, parent, "PipelineBuilder out of sync");
                Some(OutputLink {
                    pipeline: self.flush_exec()?,
                    child: 0, // Must be the only child
                })
            }
            None => parent,
        };

        let pipeline = Arc::new(RepartitionPipeline::try_new(input, output)?);
        self.push_pipeline(
            PipelineStage {
                pipeline,
                output: parent,
            },
            children,
        );
        Ok(())
    }

    /// Visit an `ExecutionPlan` operator and add it to the pipeline being built
    fn visit_operator(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
        parent: Option<OutputLink>,
    ) -> Result<()> {
        if let Some(repartition) = plan.as_any().downcast_ref::<RepartitionExec>() {
            self.push_repartition(
                repartition.input().output_partitioning(),
                repartition.output_partitioning(),
                parent,
                repartition.children(),
            )
        } else if let Some(coalesce) = plan.as_any().downcast_ref::<CoalescePartitionsExec>() {
            self.push_repartition(
                coalesce.input().output_partitioning(),
                Partitioning::RoundRobinBatch(1),
                parent,
                coalesce.children(),
            )
        } else {
            self.visit_exec(plan, parent)
        }
    }

    pub fn build(
        mut self,
        sink: Arc<dyn Sink>,
        error_sink: Arc<dyn ErrorSink>,
    ) -> Result<QueryPipeline> {
        while let Some((plan, parent)) = self.to_visit.pop() {
            self.visit_operator(plan, parent)?;
        }

        if self.execution_operators.is_some() {
            self.flush_exec()?;
        }

        Ok(QueryPipeline {
            schema: self.schema,
            output_partitions: self.output_partitions,
            stages: self.completed,
            sink,
            error_sink,
        })
    }
}
