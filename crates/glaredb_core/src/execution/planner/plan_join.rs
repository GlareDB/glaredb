use glaredb_error::{Result, ResultExt};

use super::OperatorPlanState;
use crate::execution::operators::hash_join::{HashJoinCondition, PhysicalHashJoin};
use crate::execution::operators::nested_loop_join::PhysicalNestedLoopJoin;
use crate::execution::operators::{PlannedOperator, PlannedOperatorWithChildren};
use crate::explain::context_display::ContextDisplayWrapper;
use crate::expr::comparison_expr::ComparisonOperator;
use crate::expr::physical::PhysicalScalarExpression;
use crate::expr::{self, Expression};
use crate::logical::logical_join::{
    JoinType,
    LogicalArbitraryJoin,
    LogicalComparisonJoin,
    LogicalCrossJoin,
    LogicalMagicJoin,
};
use crate::logical::operator::{self, LocationRequirement, LogicalNode, Node};

impl OperatorPlanState<'_> {
    pub fn plan_magic_join(
        &mut self,
        join: Node<LogicalMagicJoin>,
    ) -> Result<PlannedOperatorWithChildren> {
        // Planning is no different from a comparison join. Materialization
        // scans will be planned appropriately as we get there.
        self.plan_comparison_join(Node {
            node: LogicalComparisonJoin {
                join_type: join.node.join_type,
                conditions: join.node.conditions,
            },
            location: join.location,
            children: join.children,
            estimated_cardinality: join.estimated_cardinality,
        })
    }

    pub fn plan_comparison_join(
        &mut self,
        mut join: Node<LogicalComparisonJoin>,
    ) -> Result<PlannedOperatorWithChildren> {
        let has_equality = join
            .node
            .conditions
            .iter()
            .any(|condition| condition.op == ComparisonOperator::Eq);

        if has_equality && self.config.enable_hash_joins {
            // Plan hash join.
            return self.plan_comparison_join_as_hash_join(join);
        }

        // Need to fall back to nested loop join.
        let location = join.location;

        let [left, right] = join.take_two_children_exact()?;
        let left_refs = left.get_output_table_refs(self.bind_context);
        let right_refs = right.get_output_table_refs(self.bind_context);

        let condition = if join.node.conditions.is_empty() {
            None
        } else {
            // Condition expression has access to both sides of the join.
            let table_refs: Vec<_> = left_refs
                .iter()
                .cloned()
                .chain(right_refs.iter().cloned())
                .collect();

            let condition: Expression = expr::and(
                join.node
                    .conditions
                    .into_iter()
                    .map(|cond| Expression::Comparison(cond.into())),
            )?
            .into();
            let condition = self
                .expr_planner
                .plan_scalar(&table_refs, &condition)
                .context_fn(|| {
                    let condition = ContextDisplayWrapper::with_mode(&condition, self.bind_context);
                    format!("Failed to plan condition for nested loop join: {condition}")
                })?;

            Some(condition)
        };

        let op =
            self.plan_nested_loop_join(location, left, right, condition, join.node.join_type)?;

        Ok(op)
    }

    fn plan_comparison_join_as_hash_join(
        &mut self,
        mut join: Node<LogicalComparisonJoin>,
    ) -> Result<PlannedOperatorWithChildren> {
        let [left, right] = join.take_two_children_exact()?;
        let left_refs = left.get_output_table_refs(self.bind_context);
        let right_refs = right.get_output_table_refs(self.bind_context);

        let join = join.node;

        let mut conditions = Vec::with_capacity(join.conditions.len());
        for condition in join.conditions {
            let left = self.expr_planner.plan_scalar(&left_refs, &condition.left)?;
            let right = self
                .expr_planner
                .plan_scalar(&right_refs, &condition.right)?;

            conditions.push(HashJoinCondition {
                left,
                right,
                op: condition.op,
            });
        }

        let left = self.plan(left)?;
        let right = self.plan(right)?;

        let left_types = left.operator.call_output_types();
        let right_types = right.operator.call_output_types();

        let hash_join = PhysicalHashJoin::new(join.join_type, left_types, right_types, conditions)?;

        Ok(PlannedOperatorWithChildren {
            operator: PlannedOperator::new_push_execute(self.id_gen.next_id(), hash_join),
            children: vec![left, right],
        })
    }

    pub fn plan_arbitrary_join(
        &mut self,
        mut join: Node<LogicalArbitraryJoin>,
    ) -> Result<PlannedOperatorWithChildren> {
        let location = join.location;
        let filter = self
            .expr_planner
            .plan_scalar(
                &join.get_children_table_refs(self.bind_context),
                &join.node.condition,
            )
            .context("Failed to plan expressions arbitrary join filter")?;

        let [left, right] = join.take_two_children_exact()?;

        self.plan_nested_loop_join(location, left, right, Some(filter), join.node.join_type)
    }

    pub fn plan_cross_join(
        &mut self,
        mut join: Node<LogicalCrossJoin>,
    ) -> Result<PlannedOperatorWithChildren> {
        let location = join.location;
        let [left, right] = join.take_two_children_exact()?;

        self.plan_nested_loop_join(location, left, right, None, JoinType::Inner)
    }

    fn plan_nested_loop_join(
        &mut self,
        _location: LocationRequirement,
        left: operator::LogicalOperator,
        right: operator::LogicalOperator,
        filter: Option<PhysicalScalarExpression>,
        join_type: JoinType,
    ) -> Result<PlannedOperatorWithChildren> {
        let left = self.plan(left)?;
        let right = self.plan(right)?;

        let join = PhysicalNestedLoopJoin::new(
            join_type,
            left.operator.call_output_types(),
            right.operator.call_output_types(),
            filter,
        )?;

        Ok(PlannedOperatorWithChildren {
            operator: PlannedOperator::new_push_execute(self.id_gen.next_id(), join),
            children: vec![left, right],
        })
    }
}
