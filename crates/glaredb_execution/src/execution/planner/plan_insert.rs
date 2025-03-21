use glaredb_error::Result;

use super::OperatorPlanState;
use crate::arrays::datatype::DataType;
use crate::execution::operators::ungrouped_aggregate::PhysicalUngroupedAggregate;
use crate::execution::operators::{PlannedOperator, PlannedOperatorWithChildren};
use crate::expr;
use crate::expr::physical::PhysicalAggregateExpression;
use crate::functions::aggregate::builtin::sum::FUNCTION_SET_SUM;
use crate::logical::logical_insert::LogicalInsert;
use crate::logical::operator::Node;

impl OperatorPlanState<'_> {
    pub fn plan_insert(
        &mut self,
        mut insert: Node<LogicalInsert>,
    ) -> Result<PlannedOperatorWithChildren> {
        let input = insert.take_one_child_exact()?;
        let child = self.plan(input)?;

        let db = self.db_context.require_get_database(&insert.node.catalog)?;
        let operator = db.plan_insert(&mut self.id_gen, insert.node.table)?;

        let mut planned = PlannedOperatorWithChildren {
            operator,
            children: vec![child],
        };

        if !self.config.per_partition_counts {
            // Sum counts across partitions.
            let sum = expr::bind_aggregate_function(
                &FUNCTION_SET_SUM,
                vec![expr::column((0, 0), DataType::Int64)],
            )?;

            let agg = PhysicalUngroupedAggregate::new([PhysicalAggregateExpression::new(
                sum,
                [(0, DataType::Int64)],
            )]);

            planned = PlannedOperatorWithChildren {
                operator: PlannedOperator::new_execute(self.id_gen.next(), agg),
                children: vec![planned],
            }
        }

        Ok(planned)
    }
}
