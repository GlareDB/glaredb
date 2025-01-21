use std::sync::Arc;

use rayexec_error::{RayexecError, Result};

use super::pipeline::ExecutablePipeline;
use crate::arrays::batch::Batch;
use crate::config::execution::ExecutablePlanConfig;
use crate::database::DatabaseContext;
use crate::execution::executable::partition_pipeline::{
    ExecutablePartitionPipeline,
    OperatorWithState,
    PartitionPipelineInfo,
};
use crate::execution::executable::stack::ExecutionStack;
use crate::execution::intermediate::pipeline::{
    IntermediatePipeline,
    IntermediatePipelineId,
    PipelineSink,
    PipelineSource,
};
use crate::execution::operators::{
    ExecutableOperator,
    OperatorState,
    PartitionAndOperatorStates,
    PartitionState,
    PhysicalOperator,
};

#[derive(Debug)]
pub struct ExecutablePipelinePlanner<'a> {
    context: &'a DatabaseContext,
    config: ExecutablePlanConfig,
}

#[derive(Debug)]
pub(crate) struct PipelineStage1 {
    pub(crate) id: IntermediatePipelineId,
    pub(crate) source: PipelineSource,
    pub(crate) sink: PipelineSink,
    pub(crate) operators: Vec<OperatorStage1>,
}

#[derive(Debug)]
pub(crate) enum OperatorStage1 {
    UnaryInput(UnaryInputOperator),
    BinaryInput(BinaryInputOperator),
    MaterializedOuput(MaterializedOutputOperator),
}

#[derive(Debug)]
pub(crate) struct UnaryInputOperator {
    pub(crate) operator: Arc<PhysicalOperator>,
    pub(crate) operator_state: Arc<OperatorState>,
    pub(crate) partition_states: Vec<PartitionState>,
}

#[derive(Debug)]
pub(crate) struct BinaryInputOperator {
    pub(crate) operator: Arc<PhysicalOperator>,
    pub(crate) operator_state: Arc<OperatorState>,
    pub(crate) passthrough_states: Vec<PartitionState>,
    pub(crate) sink_states: Option<Vec<PartitionState>>,
}

#[derive(Debug)]
pub(crate) struct MaterializedOutputOperator {
    pub(crate) operator: Arc<PhysicalOperator>,
    pub(crate) operator_state: Arc<OperatorState>,
    pub(crate) input_states: Vec<PartitionState>,
    pub(crate) output_states: Vec<Vec<PartitionState>>,
}

#[derive(Debug)]
pub(crate) struct PipelineStage2 {
    pub(crate) id: IntermediatePipelineId,
    pub(crate) operators: Vec<OperatorStage1>,
    /// If we prepended a source operator to the operators vec.
    ///
    /// If we did, then all references to operators in this pipeline need to
    /// incremented by 1.
    pub(crate) did_prepend_source: bool,
}

impl<'a> ExecutablePipelinePlanner<'a> {
    pub fn new(context: &'a DatabaseContext, config: ExecutablePlanConfig) -> Self {
        ExecutablePipelinePlanner { context, config }
    }

    pub fn plan_pipelines(
        &mut self,
        pipelines: impl IntoIterator<Item = IntermediatePipeline>,
    ) -> Result<Vec<ExecutablePipeline>> {
        // Stage1: Plan all operators, do not handle interaction between
        // pipelines yet.
        let mut stage1 = pipelines
            .into_iter()
            .map(|pipeline| self.plan_stage1(pipeline))
            .collect::<Result<Vec<_>>>()?;

        // Stage2: Wire up inter-pipeline interactions.
        let mut stage2 = Vec::with_capacity(stage1.len());

        while let Some(pipeline) = stage1.pop() {
            let pipeline2 = self.plan_stage2(pipeline, &mut stage1, &mut stage2)?;
            stage2.push(pipeline2);
        }

        // Final stage: Plan all executable pipelines.
        let executables = stage2
            .into_iter()
            .map(|pipeline| self.plan_executable(pipeline))
            .collect::<Result<Vec<_>>>()?;

        Ok(executables)
    }

    /// Creates states for all operators in a single pipeline.
    ///
    /// This only concerns itself with operators in this pipeline. Handle
    /// sinks/sources with other pipelines happens in stage 2.
    pub(crate) fn plan_stage1(&mut self, pipeline: IntermediatePipeline) -> Result<PipelineStage1> {
        let mut operators = Vec::with_capacity(pipeline.operators.len());

        for mut operator in pipeline.operators {
            let states = operator.create_states(
                self.context,
                self.config.batch_size,
                self.config.partitions,
            )?;

            let stage1 = match states {
                PartitionAndOperatorStates::UnaryInput(states) => {
                    OperatorStage1::UnaryInput(UnaryInputOperator {
                        operator: Arc::new(operator),
                        operator_state: Arc::new(states.operator_state),
                        partition_states: states.partition_states,
                    })
                }
                PartitionAndOperatorStates::BinaryInput(states) => {
                    OperatorStage1::BinaryInput(BinaryInputOperator {
                        operator: Arc::new(operator),
                        operator_state: Arc::new(states.operator_state),
                        passthrough_states: states.passthrough_states,
                        sink_states: Some(states.sink_states),
                    })
                }
                PartitionAndOperatorStates::Materialization(states) => {
                    OperatorStage1::MaterializedOuput(MaterializedOutputOperator {
                        operator: Arc::new(operator),
                        operator_state: Arc::new(states.operator_state),
                        input_states: states.input_states,
                        output_states: states.output_states,
                    })
                }
            };

            operators.push(stage1);
        }

        Ok(PipelineStage1 {
            id: pipeline.id,
            source: pipeline.source,
            sink: pipeline.sink,
            operators,
        })
    }

    /// Handles planning sinks/sources between operators.
    ///
    /// The hashmaps for all stage1 and stage2 pipeline are provided to avoid
    /// needing to determine dependencies between pipelines, as they can be
    /// complex in the case of materializations.
    pub(crate) fn plan_stage2(
        &mut self,
        pipeline: PipelineStage1,
        stage1_pipelines: &mut [PipelineStage1],
        stage2_pipelines: &mut [PipelineStage2],
    ) -> Result<PipelineStage2> {
        let mut did_prepend_source = false;
        let mut operators = pipeline.operators;

        // Handle source.
        match pipeline.source {
            PipelineSource::InPipeline => {
                // Nothing to do. Pipeline already has source (e.g. table scan.)
            }
            PipelineSource::OtherPipeline { id, operator_idx } => {
                // Pipeline source is some other pipeline, find it and take the
                // appropriate states.
                //
                // This should only be materializations right now.
                if id == pipeline.id {
                    // TODO: This will be the trigger for recursive CTEs.
                    return Err(RayexecError::new(
                        "Recursive pipeline construction not yet supported",
                    ));
                }

                let create_source = |operator: &mut OperatorStage1| match operator {
                    OperatorStage1::MaterializedOuput(operator) => {
                        let partition_states = operator.output_states.pop().ok_or_else(|| {
                            RayexecError::new(
                                "Did not plan enough output states for materialization",
                            )
                        })?;

                        Ok(OperatorStage1::UnaryInput(UnaryInputOperator {
                            operator: operator.operator.clone(),
                            operator_state: operator.operator_state.clone(),
                            partition_states,
                        }))
                    }
                    _ => Err(RayexecError::new("Expected source to be materialization")),
                };

                if let Some(pipeline) = stage1_pipelines.iter_mut().find(|p| p.id == id) {
                    // Operator in stage1.
                    let op = pipeline
                        .operators
                        .get_mut(operator_idx)
                        .ok_or_else(|| RayexecError::new("Missing operator in stage1 pipeline"))?;

                    let source_op = create_source(op)?;
                    operators.insert(0, source_op);
                    did_prepend_source = true;
                } else {
                    let pipeline = stage2_pipelines
                        .iter_mut()
                        .find(|p| p.id == id)
                        .ok_or_else(|| {
                            RayexecError::new("Pipeline not found in stage 1 or stage 2")
                                .with_field("id", id)
                        })?;

                    // Operator in stage2, modify operator index if needed.
                    let operator_idx = if pipeline.did_prepend_source {
                        operator_idx + 1
                    } else {
                        operator_idx
                    };

                    let op = pipeline
                        .operators
                        .get_mut(operator_idx)
                        .ok_or_else(|| RayexecError::new("Missing operator in stage2 pipeline"))?;

                    let source_op = create_source(op)?;
                    operators.insert(0, source_op);
                    did_prepend_source = true;
                }
            }
        }

        // Handle sink.
        match pipeline.sink {
            PipelineSink::InPipeline => {
                // Nothing to do, sink already in pipeline.
            }
            PipelineSink::OtherPipeline { id, operator_idx } => {
                // Sink is to some other pipeline (either a join or union).
                if id == pipeline.id {
                    return Err(RayexecError::new("Cannot have pipeline sink be itself"));
                }

                let create_sink = |operator: &mut OperatorStage1| match operator {
                    OperatorStage1::BinaryInput(operator) => {
                        let partition_states = operator.sink_states.take().ok_or_else(|| {
                            RayexecError::new("Sink states for operator already taken")
                        })?;

                        Ok(OperatorStage1::UnaryInput(UnaryInputOperator {
                            operator: operator.operator.clone(),
                            operator_state: operator.operator_state.clone(),
                            partition_states,
                        }))
                    }
                    _ => Err(RayexecError::new("Expected source to be materialization")),
                };

                if let Some(pipeline) = stage1_pipelines.iter_mut().find(|p| p.id == id) {
                    // Operator in stage1.
                    let op = pipeline
                        .operators
                        .get_mut(operator_idx)
                        .ok_or_else(|| RayexecError::new("Missing operator in stage1 pipeline"))?;

                    let sink_op = create_sink(op)?;
                    operators.push(sink_op);
                } else {
                    let pipeline = stage2_pipelines
                        .iter_mut()
                        .find(|p| p.id == id)
                        .ok_or_else(|| {
                            RayexecError::new("Pipeline not found in stage 1 or stage 2")
                                .with_field("id", id)
                        })?;

                    // Operator in stage2, modify operator index if needed.
                    let operator_idx = if pipeline.did_prepend_source {
                        operator_idx + 1
                    } else {
                        operator_idx
                    };

                    let op = pipeline
                        .operators
                        .get_mut(operator_idx)
                        .ok_or_else(|| RayexecError::new("Missing operator in stage1 pipeline"))?;

                    let sink_op = create_sink(op)?;
                    operators.push(sink_op);
                }
            }
        }

        Ok(PipelineStage2 {
            id: pipeline.id,
            operators,
            did_prepend_source,
        })
    }

    /// Final planning stage for producing an executable pipeline.
    ///
    /// Every operator will have its states verified to ensure we planned the
    /// correct number of partition states.
    pub(crate) fn plan_executable(
        &mut self,
        pipeline: PipelineStage2,
    ) -> Result<ExecutablePipeline> {
        let mut partition_pipelines = (0..self.config.partitions)
            .map(|partition_idx| {
                Ok(ExecutablePartitionPipeline {
                    info: PartitionPipelineInfo {
                        pipeline: pipeline.id,
                        partition: partition_idx,
                    },
                    stack: ExecutionStack::try_new(pipeline.operators.len())?,
                    operators: Vec::with_capacity(pipeline.operators.len()),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let verify_num_states = |partition_pipelines: &[ExecutablePartitionPipeline],
                                 states: &[PartitionState]| {
            if states.len() != partition_pipelines.len() {
                return Err(RayexecError::new("Invalid number of partition states")
                    .with_field("expected", partition_pipelines.len())
                    .with_field("got", states.len()));
            }
            Ok(())
        };

        for operator in pipeline.operators {
            match operator {
                OperatorStage1::UnaryInput(op) => {
                    verify_num_states(&partition_pipelines, &op.partition_states)?;

                    for (partition_pipeline, state) in
                        partition_pipelines.iter_mut().zip(op.partition_states)
                    {
                        let output_buffer = Batch::try_new(
                            op.operator.output_types().to_vec(),
                            self.config.batch_size,
                        )?;

                        let op_with_state = OperatorWithState {
                            physical: op.operator.clone(),
                            operator_state: op.operator_state.clone(),
                            partition_states: state,
                            output_buffer: Some(output_buffer),
                        };

                        partition_pipeline.operators.push(op_with_state);
                    }
                }
                OperatorStage1::BinaryInput(op) => {
                    verify_num_states(&partition_pipelines, &op.passthrough_states)?;

                    if op.sink_states.is_some() {
                        // These states should have been taken during planning.
                        return Err(RayexecError::new(
                            "Failed to plan sink input to binary operator, still have sink states",
                        ));
                    }

                    for (partition_pipeline, state) in
                        partition_pipelines.iter_mut().zip(op.passthrough_states)
                    {
                        let output_buffer = Batch::try_new(
                            op.operator.output_types().to_vec(),
                            self.config.batch_size,
                        )?;

                        let op_with_state = OperatorWithState {
                            physical: op.operator.clone(),
                            operator_state: op.operator_state.clone(),
                            partition_states: state,
                            output_buffer: Some(output_buffer),
                        };

                        partition_pipeline.operators.push(op_with_state);
                    }
                }
                OperatorStage1::MaterializedOuput(op) => {
                    verify_num_states(&partition_pipelines, &op.input_states)?;

                    if !op.output_states.is_empty() {
                        // These should have all be popped during planning,
                        // means we compute the wrong number of scans for the
                        // materialization.
                        return Err(RayexecError::new(
                            "Failed to plan all materialization outputs, still have output states",
                        ));
                    }

                    for (partition_pipeline, state) in
                        partition_pipelines.iter_mut().zip(op.input_states)
                    {
                        let output_buffer = Batch::try_new(
                            op.operator.output_types().to_vec(),
                            self.config.batch_size,
                        )?;

                        let op_with_state = OperatorWithState {
                            physical: op.operator.clone(),
                            operator_state: op.operator_state.clone(),
                            partition_states: state,
                            output_buffer: Some(output_buffer),
                        };

                        partition_pipeline.operators.push(op_with_state);
                    }
                }
            }
        }

        Ok(ExecutablePipeline {
            pipeline_id: pipeline.id,
            partitions: partition_pipelines,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::datatype::DataType;
    use crate::execution::operators::project::PhysicalProject;
    use crate::execution::operators::testutil::test_database_context;
    use crate::execution::operators::union::PhysicalUnion;
    use crate::execution::operators::values::PhysicalValues;
    use crate::expr::physical::literal_expr::PhysicalLiteralExpr;

    #[test]
    fn plan_single_pipeline() {
        let pipeline = IntermediatePipeline {
            id: IntermediatePipelineId(0),
            source: PipelineSource::InPipeline,
            sink: PipelineSink::InPipeline,
            operators: vec![
                PhysicalOperator::Project(PhysicalProject::new([PhysicalLiteralExpr::new(1)])),
                PhysicalOperator::Project(PhysicalProject::new([PhysicalLiteralExpr::new("a")])),
            ],
        };

        let context = test_database_context();
        let mut planner = ExecutablePipelinePlanner::new(
            &context,
            ExecutablePlanConfig {
                partitions: 4,
                batch_size: 1024,
            },
        );

        let executable = planner.plan_pipelines([pipeline]).unwrap();
        assert_eq!(1, executable.len());

        assert_eq!(4, executable[0].partitions.len());
    }

    #[test]
    fn plan_union_two_pipelines() {
        let pipelines = [
            IntermediatePipeline {
                id: IntermediatePipelineId(0),
                source: PipelineSource::InPipeline,
                sink: PipelineSink::InPipeline,
                operators: vec![
                    PhysicalOperator::Values(PhysicalValues::new(vec![vec![
                        PhysicalLiteralExpr::new("cat").into(),
                    ]])),
                    PhysicalOperator::Union(PhysicalUnion::new([DataType::Utf8])),
                    PhysicalOperator::Project(PhysicalProject::new([PhysicalLiteralExpr::new(
                        "a",
                    )])),
                ],
            },
            IntermediatePipeline {
                id: IntermediatePipelineId(1),
                source: PipelineSource::InPipeline,
                sink: PipelineSink::OtherPipeline {
                    id: IntermediatePipelineId(0),
                    operator_idx: 1, // Union operator in pipeline 0
                },
                operators: vec![PhysicalOperator::Values(PhysicalValues::new(vec![vec![
                    PhysicalLiteralExpr::new("dog").into(),
                ]]))],
            },
        ];

        let context = test_database_context();
        let mut planner = ExecutablePipelinePlanner::new(
            &context,
            ExecutablePlanConfig {
                partitions: 4,
                batch_size: 1024,
            },
        );

        let executable = planner.plan_pipelines(pipelines).unwrap();
        assert_eq!(2, executable.len());

        // Pipeline 1 will now have the union as its last operator.
        let pipeline1 = executable
            .iter()
            .find(|ex| ex.pipeline_id == IntermediatePipelineId(1))
            .unwrap();
        assert_eq!(2, pipeline1.partitions[0].operators.len());
        match executable[1].partitions[0].operators[1].physical.as_ref() {
            PhysicalOperator::Union(_) => (),
            other => panic!("expected union operator, got other: {other:?}"),
        }
    }
}
