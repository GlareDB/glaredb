use std::sync::Arc;

use rayexec_error::{not_implemented, RayexecError, Result, ResultExt};

use super::{IntermediatePipelineBuildState, Materializations, PipelineIdGen};
use crate::execution::intermediate::pipeline::IntermediateOperator;
use crate::execution::operators::hash_join::PhysicalHashJoin;
use crate::execution::operators::nl_join::PhysicalNestedLoopJoin;
use crate::execution::operators::PhysicalOperator;
use crate::expr::comparison_expr::ComparisonOperator;
use crate::expr::physical::scalar_function_expr::PhysicalScalarFunctionExpr;
use crate::expr::physical::PhysicalScalarExpression;
use crate::functions::scalar::builtin::boolean::AndImpl;
use crate::logical::logical_join::{
    JoinType,
    LogicalArbitraryJoin,
    LogicalComparisonJoin,
    LogicalCrossJoin,
    LogicalMagicJoin,
};
use crate::logical::operator::{self, LocationRequirement, LogicalNode, Node};

impl IntermediatePipelineBuildState<'_> {
    pub fn plan_magic_join(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        join: Node<LogicalMagicJoin>,
    ) -> Result<()> {
        // Planning is no different from a comparison join. Materialization
        // scans will be planned appropriately as we get there.
        self.plan_comparison_join(
            id_gen,
            materializations,
            Node {
                node: LogicalComparisonJoin {
                    join_type: join.node.join_type,
                    conditions: join.node.conditions,
                },
                location: join.location,
                children: join.children,
                estimated_cardinality: join.estimated_cardinality,
            },
        )
    }

    pub fn plan_comparison_join(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        mut join: Node<LogicalComparisonJoin>,
    ) -> Result<()> {
        let location = join.location;

        let equality_indices: Vec<_> = join
            .node
            .conditions
            .iter()
            .enumerate()
            .filter_map(|(idx, cond)| {
                if cond.op == ComparisonOperator::Eq {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect();

        if !equality_indices.is_empty() {
            // Use hash join

            let [left, right] = join.take_two_children_exact()?;
            let left_refs = left.get_output_table_refs(self.bind_context);
            let right_refs = right.get_output_table_refs(self.bind_context);

            let mut left_types = Vec::new();
            for &table_ref in &left_refs {
                let table = self.bind_context.get_table(table_ref)?;
                left_types.extend(table.column_types.iter().cloned());
            }

            let mut right_types = Vec::new();
            for &table_ref in &right_refs {
                let table = self.bind_context.get_table(table_ref)?;
                right_types.extend(table.column_types.iter().cloned());
            }

            // Build up all inputs on the right (probe) side. This is going to
            // continue with the the current pipeline.
            self.walk(materializations, id_gen, right)?;

            // Build up the left (build) side in a separate pipeline. This will feed
            // into the currently pipeline at the join operator.
            let mut left_state =
                IntermediatePipelineBuildState::new(self.config, self.bind_context);
            left_state.walk(materializations, id_gen, left)?;

            // Add batch resizer to left (build) side.
            left_state.push_batch_resizer(id_gen)?;

            // Take any completed pipelines from the left side and put them in our
            // list.
            self.local_group
                .merge_from_other(&mut left_state.local_group);
            self.remote_group
                .merge_from_other(&mut left_state.remote_group);

            // Get the left pipeline.
            let left_pipeline = left_state.in_progress.take().ok_or_else(|| {
                RayexecError::new("expected in-progress pipeline from left side of join")
            })?;

            // Resize probe inputs too.
            //
            // TODO: There's some experimentation to be done on if this is
            // beneficial to do on the output of a join too.
            self.push_batch_resizer(id_gen)?;

            let conditions = join
                .node
                .conditions
                .iter()
                .map(|condition| {
                    self.expr_planner
                        .plan_join_condition_as_hash_join_condition(
                            &left_refs,
                            &right_refs,
                            condition,
                        )
                        .context_fn(|| format!("Failed to plan condition: {condition}"))
                })
                .collect::<Result<Vec<_>>>()?;

            let operator = IntermediateOperator {
                operator: Arc::new(PhysicalOperator::HashJoin(PhysicalHashJoin::new(
                    join.node.join_type,
                    &equality_indices,
                    conditions,
                    left_types,
                    right_types,
                ))),
                partitioning_requirement: None,
            };
            self.push_intermediate_operator(operator, location, id_gen)?;

            // Left pipeline will be child this this pipeline at the current
            // operator.
            self.push_as_child_pipeline(left_pipeline, PhysicalHashJoin::BUILD_SIDE_INPUT_INDEX)?;

            // Resize output of join too.
            self.push_batch_resizer(id_gen)?;

            Ok(())
        } else {
            // Need to fall back to nested loop join.

            if join.node.join_type != JoinType::Inner {
                not_implemented!("join type with nl join: {}", join.node.join_type);
            }

            let table_refs = join.get_output_table_refs(self.bind_context);
            let conditions = self
                .expr_planner
                .plan_join_conditions_as_expression(&table_refs, &join.node.conditions)?;

            let condition = PhysicalScalarExpression::ScalarFunction(PhysicalScalarFunctionExpr {
                function: Box::new(AndImpl),
                inputs: conditions,
            });

            let [left, right] = join.take_two_children_exact()?;

            self.push_nl_join(
                id_gen,
                materializations,
                location,
                left,
                right,
                Some(condition),
                join.node.join_type,
            )?;

            Ok(())
        }
    }

    pub fn plan_arbitrary_join(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        mut join: Node<LogicalArbitraryJoin>,
    ) -> Result<()> {
        let location = join.location;
        let filter = self
            .expr_planner
            .plan_scalar(
                &join.get_children_table_refs(self.bind_context),
                &join.node.condition,
            )
            .context("Failed to plan expressions arbitrary join filter")?;

        // Modify the filter as to match the join type.
        let filter = match join.node.join_type {
            JoinType::Inner => filter,
            other => {
                // TODO: Other join types.
                return Err(RayexecError::new(format!(
                    "Unhandled join type for arbitrary join: {other:?}"
                )));
            }
        };

        let [left, right] = join.take_two_children_exact()?;

        self.push_nl_join(
            id_gen,
            materializations,
            location,
            left,
            right,
            Some(filter),
            join.node.join_type,
        )
    }

    pub fn plan_cross_join(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        mut join: Node<LogicalCrossJoin>,
    ) -> Result<()> {
        let location = join.location;
        let [left, right] = join.take_two_children_exact()?;

        self.push_nl_join(
            id_gen,
            materializations,
            location,
            left,
            right,
            None,
            JoinType::Inner,
        )
    }

    /// Push a nest loop join.
    ///
    /// This will create a complete pipeline for the left side of the join
    /// (build), right right side (probe) will be pushed onto the current
    /// pipeline.
    #[allow(clippy::too_many_arguments)]
    fn push_nl_join(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        location: LocationRequirement,
        left: operator::LogicalOperator,
        right: operator::LogicalOperator,
        filter: Option<PhysicalScalarExpression>,
        join_type: JoinType,
    ) -> Result<()> {
        self.config.check_nested_loop_join_allowed()?;

        // Continue to build up all the inputs into the right side.
        self.walk(materializations, id_gen, right)?;

        // Create a completely independent pipeline (or pipelines) for left
        // side.
        let mut left_state = IntermediatePipelineBuildState::new(self.config, self.bind_context);
        left_state.walk(materializations, id_gen, left)?;

        // Take completed pipelines from the left and merge them into this
        // state's completed set of pipelines.
        self.local_group
            .merge_from_other(&mut left_state.local_group);
        self.remote_group
            .merge_from_other(&mut left_state.remote_group);

        // Get the left in-progress pipeline. This will be one of the inputs
        // into the current in-progress pipeline.
        let left_pipeline = left_state.in_progress.take().ok_or_else(|| {
            RayexecError::new("expected in-progress pipeline from left side of join")
        })?;

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::NestedLoopJoin(
                PhysicalNestedLoopJoin::new(filter, join_type),
            )),
            partitioning_requirement: None,
        };
        self.push_intermediate_operator(operator, location, id_gen)?;

        // Left pipeline will be input to this pipeline.
        self.push_as_child_pipeline(
            left_pipeline,
            PhysicalNestedLoopJoin::BUILD_SIDE_INPUT_INDEX,
        )?;

        Ok(())
    }
}
