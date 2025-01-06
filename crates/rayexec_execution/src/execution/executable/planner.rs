use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use rayexec_error::{RayexecError, Result};
use rayexec_io::http::HttpClient;

use super::pipeline::{ExecutablePipeline, PipelineId};
use crate::config::execution::ExecutablePlanConfig;
use crate::database::DatabaseContext;
use crate::engine::result::ResultSink;
use crate::execution::intermediate::pipeline::{
    IntermediateMaterializationGroup,
    IntermediateOperator,
    IntermediatePipelineGroup,
    IntermediatePipelineId,
    PipelineSink,
    PipelineSource,
};
use crate::execution::operators::materialize::MaterializeOperation;
use crate::execution::operators::round_robin::PhysicalRoundRobinRepartition;
use crate::execution::operators::sink::{SinkOperation, SinkOperator};
use crate::execution::operators::source::{SourceOperation, SourceOperator};
use crate::execution::operators::{
    ExecutableOperator,
    InputOutputStates2,
    OperatorState,
    PartitionState,
    PhysicalOperator,
};
use crate::hybrid::buffer::ServerStreamBuffers;
use crate::hybrid::client::HybridClient;
use crate::hybrid::stream::{ClientToServerStream, ServerToClientStream};
use crate::logical::binder::bind_context::MaterializationRef;
use crate::runtime::Runtime;

/// Used for ensuring every pipeline in a query has a unique id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PipelineIdGen {
    gen: PipelineId,
}

impl PipelineIdGen {
    fn next(&mut self) -> PipelineId {
        let id = self.gen;
        self.gen.0 += 1;
        id
    }
}

#[derive(Debug)]
pub enum PlanLocationState<'a, C: HttpClient> {
    /// State when planning on the server.
    Server {
        /// Stream buffers used for buffering incoming and outgoing batches for
        /// distributed execution.
        stream_buffers: &'a ServerStreamBuffers,
    },
    /// State when planning on the client side.
    Client {
        /// Output sink for a query.
        ///
        /// Should only be used once per query. The option helps us enforce that
        /// (and also allows us to avoid needing to wrap in an Arc).
        output_sink: Option<ResultSink>,
        /// Optional hybrid client if we're executing in hybrid mode.
        ///
        /// When providing, appropriate query sinks and sources will be inserted
        /// to the plan which will work to move batches between the client an
        /// server.
        hybrid_client: Option<&'a Arc<HybridClient<C>>>,
    },
}

// TODO: Some refactoring in order.
//
// We don't need to create all operators up front, we can build them up
// incrementally.
#[derive(Debug)]
pub struct ExecutablePipelinePlanner<'a, R: Runtime> {
    context: &'a DatabaseContext,
    config: ExecutablePlanConfig,
    id_gen: PipelineIdGen,
    /// Location specific state used during planning.
    loc_state: PlanLocationState<'a, R::HttpClient>,
}

impl<'a, R: Runtime> ExecutablePipelinePlanner<'a, R> {
    pub fn new(
        context: &'a DatabaseContext,
        config: ExecutablePlanConfig,
        loc_state: PlanLocationState<'a, R::HttpClient>,
    ) -> Self {
        ExecutablePipelinePlanner {
            context,
            config,
            id_gen: PipelineIdGen { gen: PipelineId(0) },
            loc_state,
        }
    }

    pub fn plan_from_intermediate(
        &mut self,
        group: IntermediatePipelineGroup,
        materializations: IntermediateMaterializationGroup,
    ) -> Result<Vec<ExecutablePipeline>> {
        let mut pending = PendingQuery::try_from_operators_and_materializations(
            &self.config,
            self.context,
            group,
            materializations,
        )?;

        pending.plan_executable_pipelines(self.context, &mut self.loc_state, &mut self.id_gen)
    }
}

#[derive(Debug)]
struct PendingQuery {
    /// All pending operators in a query.
    operators: Vec<PendingOperatorWithState>,
    /// Pending pipelines in this query.
    ///
    /// This includes pipelines that make up a materialization.
    pipelines: HashMap<IntermediatePipelineId, PendingPipeline>,
    /// Pending materializations in the query.
    materializations: HashMap<MaterializationRef, PendingMaterialization>,
}

impl PendingQuery {
    fn try_from_operators_and_materializations(
        config: &ExecutablePlanConfig,
        context: &DatabaseContext,
        group: IntermediatePipelineGroup,
        materializations: IntermediateMaterializationGroup,
    ) -> Result<Self> {
        let mut pending_materializations = HashMap::new();
        let mut pending_pipelines = HashMap::new();
        let mut operators = Vec::new();

        // Handle materializations first.
        //
        // This will construct the appropriate sink/source operators, and then
        // converts the materialization body into a normal pending pipeline.
        for (mat_ref, materialization) in materializations.materializations {
            let mut operator_indexes = Vec::with_capacity(materialization.operators.len());

            for operator in materialization.operators {
                let idx = operators.len();
                let pending = PendingOperatorWithState::try_from_intermediate_operator(
                    config, context, operator,
                )?;

                operator_indexes.push(idx);
                operators.push(pending);
            }

            let mat_op =
                MaterializeOperation::new(mat_ref, config.partitions, materialization.scan_count);

            // Add materialization sink to pending operators.
            let idx = operators.len();
            let pending = PendingOperatorWithState::try_from_intermediate_operator(
                config,
                context,
                IntermediateOperator {
                    operator: Arc::new(PhysicalOperator::DynSink(SinkOperator::new(Box::new(
                        mat_op.sink,
                    )))),
                    partitioning_requirement: Some(config.partitions),
                },
            )?;
            operators.push(pending);
            operator_indexes.push(idx);

            // Add scan sources to pending operators.
            let mut scan_sources = Vec::new();
            for source in mat_op.sources {
                let idx = operators.len();
                let pending = PendingOperatorWithState::try_from_intermediate_operator(
                    config,
                    context,
                    IntermediateOperator {
                        operator: Arc::new(PhysicalOperator::MaterializedSource(
                            SourceOperator::new(source),
                        )),
                        partitioning_requirement: Some(config.partitions),
                    },
                )?;

                // Note these indexes aren't being stored on the pending
                // pipeline.
                scan_sources.push(idx);
                operators.push(pending);
            }

            // Push materialization as pipeline.
            pending_pipelines.insert(
                materialization.id,
                PendingPipeline {
                    operators: operator_indexes,
                    sink: PipelineSink::InPipeline, //  We added the sink above.
                    source: materialization.source,
                },
            );

            // Store indices for the scans for the output of the
            // materialization.
            pending_materializations.insert(mat_ref, PendingMaterialization { scan_sources });
        }

        // Handle the other operators.
        for (id, pipeline) in group.pipelines {
            let mut operator_indexes = Vec::with_capacity(pipeline.operators.len());

            for (op_idx, mut operator) in pipeline.operators.into_iter().enumerate() {
                // Check to see if this pipeline has a source with a
                // partitioning requriment. If it does, go ahead and just put
                // that on the operator.
                //
                // This avoids needing to introduce repartitions if we don't
                // want/need it.
                if op_idx == 0 && operator.partitioning_requirement.is_none() {
                    // TODO: Possibly for `OtherGroup` as well?
                    if let PipelineSource::OtherPipeline {
                        partitioning_requirement,
                        ..
                    } = &pipeline.source
                    {
                        operator.partitioning_requirement = *partitioning_requirement;
                    }
                }

                let idx = operators.len();
                let pending = PendingOperatorWithState::try_from_intermediate_operator(
                    config, context, operator,
                )?;

                operator_indexes.push(idx);
                operators.push(pending);
            }

            pending_pipelines.insert(
                id,
                PendingPipeline {
                    operators: operator_indexes,
                    sink: pipeline.sink,
                    source: pipeline.source,
                },
            );
        }

        Ok(PendingQuery {
            operators,
            pipelines: pending_pipelines,
            materializations: pending_materializations,
        })
    }

    fn plan_executable_pipelines<C: HttpClient + 'static>(
        &mut self,
        context: &DatabaseContext,
        loc_state: &mut PlanLocationState<'_, C>,
        id_gen: &mut PipelineIdGen,
    ) -> Result<Vec<ExecutablePipeline>> {
        let mut pipelines = Vec::with_capacity(self.pipelines.len() + self.materializations.len());

        // TODO: Would be cool not cloning here. Needed to avoid mut reference
        // issues (borrow a pending pipeline but need mut self).
        let pending_pipelines: Vec<_> = self.pipelines.values().cloned().collect();

        for pending in pending_pipelines {
            self.plan_executable_pipeline(context, loc_state, id_gen, &pending, &mut pipelines)?;
        }

        Ok(pipelines)
    }

    /// Plan an executable pipeline for a pending pipline, placing the resuling
    /// pipeline(s) in `executables`.
    ///
    /// A single pending pipeline may result in multiple executable pipelines
    /// due repartitioning.
    fn plan_executable_pipeline<C: HttpClient + 'static>(
        &mut self,
        context: &DatabaseContext,
        loc_state: &mut PlanLocationState<'_, C>,
        id_gen: &mut PipelineIdGen,
        pending: &PendingPipeline,
        executables: &mut Vec<ExecutablePipeline>,
    ) -> Result<()> {
        // Plan source.
        let mut pipeline = self.plan_from_source(context, loc_state, id_gen, pending)?;

        let mut operator_indices = pending.operators.iter();
        // Skip first index if the source was in the list of operators.
        if pending.source == PipelineSource::InPipeline {
            operator_indices.next();
        }

        // Wire up the rest.
        for operator_idx in operator_indices {
            let operator = self.get_operator_mut(*operator_idx)?;
            let partition_states = operator.input_states[operator.trunk_idx].take().unwrap();

            // If partition doesn't match, push a round robin and start new
            // pipeline.
            if partition_states.len() != pipeline.num_partitions() {
                pipeline = Self::push_repartition(
                    context,
                    id_gen,
                    pipeline,
                    partition_states.len(),
                    executables,
                )?;
            }

            pipeline.push_operator(
                operator.operator.clone(),
                operator.operator_state.clone(),
                partition_states,
            )?;
        }

        // Finish it, pushes to executables for us.
        self.finish_pipeline(context, loc_state, id_gen, pending, pipeline, executables)?;

        Ok(())
    }

    /// Finishes planning an executable pipeline by ensuring it has an
    /// appropriate sink.
    ///
    /// The resulting pipeline(s) will be placed in `executables`.
    fn finish_pipeline<C: HttpClient + 'static>(
        &mut self,
        context: &DatabaseContext,
        loc_state: &mut PlanLocationState<'_, C>,
        id_gen: &mut PipelineIdGen,
        pending: &PendingPipeline,
        mut pipeline: ExecutablePipeline,
        executables: &mut Vec<ExecutablePipeline>,
    ) -> Result<()> {
        match pending.sink {
            PipelineSink::QueryOutput => {
                let sink = match loc_state {
                    PlanLocationState::Client { output_sink, .. } => match output_sink.take() {
                        Some(sink) => sink,
                        None => return Err(RayexecError::new("Missing output sink")),
                    },
                    PlanLocationState::Server { .. } => {
                        return Err(RayexecError::new("Query output needs to happen on client"))
                    }
                };

                let partitions = match sink.partition_requirement() {
                    Some(n) => n,
                    None => pipeline.num_partitions(),
                };

                if partitions != pipeline.num_partitions() {
                    pipeline =
                        Self::push_repartition(context, id_gen, pipeline, partitions, executables)?;
                }

                let operator = Arc::new(PhysicalOperator::ResultSink(SinkOperator::new(sink)));
                let states = operator.create_states2(context, vec![partitions])?;
                let partition_states = match states.partition_states {
                    InputOutputStates2::OneToOne { partition_states } => partition_states,
                    _ => return Err(RayexecError::new("invalid partition states for query sink")),
                };

                pipeline.push_operator(operator, states.operator_state, partition_states)?;

                executables.push(pipeline);
            }
            PipelineSink::InPipeline => {
                // The pipeline's final operator is the query sink. Nothing left
                // for us to do.
                executables.push(pipeline);
            }
            PipelineSink::InGroup {
                pipeline_id,
                operator_idx,
                input_idx,
            } => {
                // We have the sink pipeline with us, wire up directly.

                let pending = self.pipelines.get(&pipeline_id).unwrap();
                let operator = self.get_operator_mut(pending.operators[operator_idx])?;
                let partition_states = operator.take_input_states(input_idx)?;

                if partition_states.len() != pipeline.num_partitions() {
                    pipeline = Self::push_repartition(
                        context,
                        id_gen,
                        pipeline,
                        partition_states.len(),
                        executables,
                    )?;
                }

                pipeline.push_operator(
                    operator.operator.clone(),
                    operator.operator_state.clone(),
                    partition_states,
                )?;

                executables.push(pipeline);
            }
            PipelineSink::OtherGroup {
                stream_id,
                partitions,
            } => {
                // Sink is pipeline executing somewhere else.
                let operator: SinkOperator<Box<dyn SinkOperation>> = match loc_state {
                    PlanLocationState::Server { stream_buffers } => {
                        let sink = stream_buffers.create_outgoing_stream(stream_id)?;
                        SinkOperator::new(Box::new(sink))
                    }
                    PlanLocationState::Client { hybrid_client, .. } => {
                        // Missing hybrid client shouldn't happen. Means we've
                        // incorrectly planned a hybrid query when we shouldn't
                        // have.
                        let hybrid_client = hybrid_client.ok_or_else(|| {
                            RayexecError::new("Hybrid client missing, cannot create sink pipeline")
                        })?;
                        let sink = ClientToServerStream::new(stream_id, hybrid_client.clone());
                        SinkOperator::new(Box::new(sink))
                    }
                };

                let states = operator.create_states2(context, vec![partitions])?;
                let partition_states = match states.partition_states {
                    InputOutputStates2::OneToOne { partition_states } => partition_states,
                    _ => return Err(RayexecError::new("invalid partition states")),
                };

                if partition_states.len() != pipeline.num_partitions() {
                    pipeline = Self::push_repartition(
                        context,
                        id_gen,
                        pipeline,
                        partition_states.len(),
                        executables,
                    )?;
                }

                pipeline.push_operator(
                    Arc::new(PhysicalOperator::DynSink(operator)),
                    states.operator_state,
                    partition_states,
                )?;

                executables.push(pipeline);
            }
            PipelineSink::Materialization { .. } => {
                // TODO: This is never constructed as pipelines that make up a
                // materialization are constructed slightly differently in that
                // we don't need to specify a sink.
                //
                // This does cause a slight split in logic, and we may at some
                // point use this variant if we want to try to unify them.
                unreachable!("Materialization variant never constructed")
            }
        }

        Ok(())
    }

    /// Construct the initial executable pipeline with the pipelines source.
    fn plan_from_source<C: HttpClient + 'static>(
        &mut self,
        context: &DatabaseContext,
        loc_state: &PlanLocationState<'_, C>,
        id_gen: &mut PipelineIdGen,
        pending: &PendingPipeline,
    ) -> Result<ExecutablePipeline> {
        match pending.source {
            PipelineSource::InPipeline => {
                // Source is the first operator in the pipeline.
                let idx = *pending
                    .operators
                    .first()
                    .ok_or_else(|| RayexecError::new("Missing first operator in pipeline"))?;

                let source = self.get_operator_mut(idx)?;
                debug_assert_eq!(1, source.input_states.len());
                let partition_states = source.take_input_states(0)?;

                let mut pipeline = ExecutablePipeline::new(id_gen.next(), partition_states.len());
                pipeline.push_operator(
                    source.operator.clone(),
                    source.operator_state.clone(),
                    partition_states,
                )?;

                Ok(pipeline)
            }
            PipelineSource::OtherPipeline { pipeline, .. } => {
                // Source is the last operation for some other pipeline
                // executing on this node.
                let operator_idx = *self
                    .pipelines
                    .get(&pipeline)
                    .ok_or_else(|| {
                        RayexecError::new(format!("Missing pipeline for id {pipeline:?}"))
                    })?
                    .operators
                    .last()
                    .ok_or_else(|| {
                        RayexecError::new(format!("Pipeline has no operators: {pipeline:?}"))
                    })?;

                let source = self.get_operator_mut(operator_idx)?;

                // TODO: Definitely needs comments.
                let pull_states = source.pull_states.pop_front().ok_or_else(|| {
                    RayexecError::new(format!("Pipeline has missing pull states: {pipeline:?}"))
                })?;

                let mut pipeline = ExecutablePipeline::new(id_gen.next(), pull_states.len());
                pipeline.push_operator(
                    source.operator.clone(),
                    source.operator_state.clone(),
                    pull_states,
                )?;

                Ok(pipeline)
            }
            PipelineSource::OtherGroup {
                stream_id,
                partitions,
            } => {
                // Source is pipeline that's executing somewhere else.
                //
                // Set up hybrid operator.
                let operator = match &loc_state {
                    PlanLocationState::Server { stream_buffers } => {
                        let source = stream_buffers.create_incoming_stream(stream_id)?;
                        SourceOperator::new(Box::new(source) as Box<dyn SourceOperation>)
                    }
                    PlanLocationState::Client { hybrid_client, .. } => {
                        // Missing hybrid client shouldn't happen.
                        let hybrid_client = hybrid_client.ok_or_else(|| {
                            RayexecError::new("Hybrid client missing, cannot create sink pipeline")
                        })?;
                        let source = ServerToClientStream::new(stream_id, hybrid_client.clone());
                        SourceOperator::new(Box::new(source) as Box<dyn SourceOperation>)
                    }
                };

                let states = operator.create_states2(context, vec![partitions])?;
                let partition_states = match states.partition_states {
                    InputOutputStates2::OneToOne { partition_states } => partition_states,
                    _ => {
                        return Err(RayexecError::new(
                            "Invalid partition states for query source",
                        ))
                    }
                };

                let mut pipeline = ExecutablePipeline::new(id_gen.next(), partitions);
                pipeline.push_operator(
                    Arc::new(PhysicalOperator::DynSource(operator)),
                    states.operator_state,
                    partition_states,
                )?;

                Ok(pipeline)
            }
            PipelineSource::Materialization { mat_ref } => {
                let mat = self.materializations.get_mut(&mat_ref).ok_or_else(|| {
                    RayexecError::new(format!("Missing pending materialization: {mat_ref}"))
                })?;

                let operator_idx = match mat.scan_sources.pop() {
                    Some(idx) => idx,
                    None => {
                        // Since we're popping these, would only happen if the
                        // scan count for the materialization is out of sync.
                        return Err(RayexecError::new(format!(
                            "Missing source operator index: {mat_ref}"
                        )));
                    }
                };

                let operator = self.get_operator_mut(operator_idx)?;
                let partition_states = operator.take_input_states(0)?;

                let mut pipeline = ExecutablePipeline::new(id_gen.next(), partition_states.len());
                pipeline.push_operator(
                    operator.operator.clone(),
                    operator.operator_state.clone(),
                    partition_states,
                )?;

                Ok(pipeline)
            }
        }
    }

    /// Push a repartition operator on the pipline, and return a new pipeline
    /// with the repartition as the source.
    fn push_repartition(
        context: &DatabaseContext,
        id_gen: &mut PipelineIdGen,
        mut pipeline: ExecutablePipeline,
        output_partitions: usize,
        pipelines: &mut Vec<ExecutablePipeline>,
    ) -> Result<ExecutablePipeline> {
        let rr_operator = Arc::new(PhysicalOperator::RoundRobin(PhysicalRoundRobinRepartition));
        let states = rr_operator
            .create_states2(context, vec![pipeline.num_partitions(), output_partitions])?;

        let (push_states, pull_states) = match states.partition_states {
            InputOutputStates2::SeparateInputOutput {
                push_states,
                pull_states,
            } => (push_states, pull_states),
            other => panic!("invalid states for round robin: {other:?}"),
        };

        pipeline.push_operator(
            rr_operator.clone(),
            states.operator_state.clone(),
            push_states,
        )?;

        pipelines.push(pipeline);

        // New pipeline with round robin as source.
        let mut pipeline = ExecutablePipeline::new(id_gen.next(), pull_states.len());
        pipeline.push_operator(rr_operator, states.operator_state, pull_states)?;

        Ok(pipeline)
    }

    fn get_operator_mut(&mut self, idx: usize) -> Result<&mut PendingOperatorWithState> {
        self.operators
            .get_mut(idx)
            .ok_or_else(|| RayexecError::new(format!("Missing pending operation at idx {idx}")))
    }
}

#[derive(Debug)]
struct PendingMaterialization {
    /// Indexes for operators that scan the materialization.
    ///
    /// Length of the vector corresponds to the computed scan count for the
    /// materialization. Indexes should be popped when the operator is used as a
    /// source to a pipeline.
    ///
    /// An error should be returned if this is non-zero at the end of planning,
    /// or if there are more dependent pipelines than there are sources.
    scan_sources: Vec<usize>,
}

#[derive(Debug, Clone)]
struct PendingPipeline {
    /// Indices that index into the `operators` vec in pending query.
    operators: Vec<usize>,
    /// Sink for this pipeline.
    sink: PipelineSink,
    /// Source for this pipeline.
    source: PipelineSource,
}

/// An operator with initialized state.
#[derive(Debug)]
struct PendingOperatorWithState {
    /// The physical operator.
    operator: Arc<PhysicalOperator>,
    /// Global operator state.
    operator_state: Arc<OperatorState>,
    /// Input states that get taken when building up the final execution
    /// pipeline.
    input_states: Vec<Option<Vec<PartitionState>>>,
    /// Output states that get popped when building the final pipeline.
    ///
    /// May be empty if the operator uses the same partition state for pushing
    /// and pulling.
    pull_states: VecDeque<Vec<PartitionState>>,
    /// Index of the input state to use for the pull state. This corresponds to
    /// the "trunk" of the pipeline.
    trunk_idx: usize,
}

impl PendingOperatorWithState {
    fn try_from_intermediate_operator(
        config: &ExecutablePlanConfig,
        context: &DatabaseContext,
        operator: IntermediateOperator,
    ) -> Result<Self> {
        let partitions = operator
            .partitioning_requirement
            .unwrap_or(config.partitions);

        // TODO: How to get other input partitions.
        let states = operator
            .operator
            .create_states2(context, vec![partitions])?;

        Ok(match states.partition_states {
            InputOutputStates2::OneToOne { partition_states } => PendingOperatorWithState {
                operator: operator.operator,
                operator_state: states.operator_state,
                input_states: vec![Some(partition_states)],
                pull_states: VecDeque::new(),
                trunk_idx: 0,
            },
            InputOutputStates2::NaryInputSingleOutput {
                partition_states,
                pull_states,
            } => {
                let input_states: Vec<_> = partition_states.into_iter().map(Some).collect();
                PendingOperatorWithState {
                    operator: operator.operator,
                    operator_state: states.operator_state,
                    input_states,
                    pull_states: VecDeque::new(),
                    trunk_idx: pull_states,
                }
            }
            InputOutputStates2::SeparateInputOutput {
                push_states,
                pull_states,
            } => PendingOperatorWithState {
                operator: operator.operator,
                operator_state: states.operator_state,
                input_states: vec![Some(push_states)],
                pull_states: [pull_states].into_iter().collect(),
                trunk_idx: 0,
            },
        })
    }

    fn take_input_states(&mut self, idx: usize) -> Result<Vec<PartitionState>> {
        self.input_states
            .get_mut(idx)
            .ok_or_else(|| RayexecError::new(format!("Missing input states at idx {idx}")))?
            .take()
            .ok_or_else(|| RayexecError::new(format!("Input states already taken at idx {idx}")))
    }
}
