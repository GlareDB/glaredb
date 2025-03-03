use rayexec_error::{RayexecError, Result};

use super::operators::{ExecutionProperties, PlannedOperator, RawOperatorState};
use super::partition_pipeline::ExecutablePartitionPipeline;
use super::planner::QueryGraph;
use crate::arrays::batch::Batch;
use crate::database::DatabaseContext;
use crate::execution::execution_stack::ExecutionStack;

#[derive(Debug)]
pub struct ExecutablePipelineGraph {
    pub(crate) pipelines: Vec<ExecutablePipeline>,
}

impl ExecutablePipelineGraph {
    pub fn plan_from_graph(
        db_context: &DatabaseContext,
        props: ExecutionProperties,
        query_graph: QueryGraph,
    ) -> Result<Self> {
        let mut pipeline_graph = ExecutablePipelineGraph {
            pipelines: Vec::new(),
        };

        let mut current = ExecutablePipeline::new();

        // TODO: Materializations.

        let root = query_graph.root;
        root.build_pipeline(db_context, props, &mut pipeline_graph, &mut current)?;

        Ok(pipeline_graph)
    }

    pub fn push_pipeline(&mut self, pipeline: ExecutablePipeline) {
        self.pipelines.push(pipeline);
    }

    pub fn create_partition_pipelines(
        &self,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<ExecutablePartitionPipeline>> {
        let mut final_pipelines = Vec::new();

        for pipeline in &self.pipelines {
            let partition_pipelines = pipeline.create_partition_pipelines(props, partitions)?;
            final_pipelines.extend(partition_pipelines);
        }

        Ok(final_pipelines)
    }
}

#[derive(Debug)]
pub struct ExecutablePipeline {
    /// Operators just in this pipeline.
    pub(crate) operators: Vec<PlannedOperator>,
    /// States associated with the operators.
    pub(crate) operator_states: Vec<RawOperatorState>,
}

impl ExecutablePipeline {
    pub fn new() -> Self {
        ExecutablePipeline {
            operators: Vec::new(),
            operator_states: Vec::new(),
        }
    }

    pub fn push_operator_and_state(&mut self, operator: PlannedOperator, state: RawOperatorState) {
        self.operators.push(operator);
        self.operator_states.push(state);
    }

    pub fn create_partition_pipelines(
        &self,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<ExecutablePartitionPipeline>> {
        debug_assert_eq!(self.operators.len(), self.operator_states.len());

        if self.operators.len() < 2 {
            return Err(RayexecError::new(
                "Cannot create partition pipeline for pipeline containing fewer than two operators (sink and source)",
            ));
        }

        let mut pipelines: Vec<_> = (0..partitions)
            .map(|_| ExecutablePartitionPipeline {
                operators: self.operators.clone(),
                operator_states: self.operator_states.clone(),
                partition_states: Vec::with_capacity(self.operators.len()),
                buffers: Vec::with_capacity(self.operators.len() - 1),
                stack: ExecutionStack::new(self.operators.len()),
            })
            .collect();

        let source = &self.operators[0];
        let source_state = &self.operator_states[0];

        let sink = &self.operators[self.operators.len() - 1];
        let sink_state = &self.operator_states[self.operators.len() - 1];

        let intermediate = &self.operators[1..self.operators.len() - 1];
        let intermediate_states = &self.operator_states[1..self.operators.len() - 1];

        let mut append_partition_states = |mut states: Vec<_>| -> Result<()> {
            if states.len() != pipelines.len() {
                return Err(RayexecError::new("Generated incorrect number of states")
                    .with_field("expected", pipelines.len())
                    .with_field("got", states.len()));
            }

            for pipeline in &mut pipelines {
                let state = states.pop().expect("enough states to exist");
                pipeline.partition_states.push(state);
            }

            Ok(())
        };

        // Create source states.
        let source_partition_states =
            source.call_create_partition_pull_states(source_state, props, partitions)?;
        append_partition_states(source_partition_states)?;

        // Create intermediate operator states.
        for (intermediate, intermediate_state) in intermediate.iter().zip(intermediate_states) {
            let partition_states = intermediate.call_create_partition_execute_states(
                intermediate_state,
                props,
                partitions,
            )?;
            append_partition_states(partition_states)?;
        }

        // Create sink states.
        let sink_partition_states =
            sink.call_create_partition_push_states(sink_state, props, partitions)?;
        append_partition_states(sink_partition_states)?;

        // Create batch buffers for all but the last operator.
        for pipeline in &mut pipelines {
            for operator in &self.operators[0..self.operators.len() - 1] {
                let batch = Batch::new(
                    operator.call_output_types().iter().cloned(),
                    props.batch_size,
                )?;
                pipeline.buffers.push(batch);
            }
        }

        Ok(pipelines)
    }
}
