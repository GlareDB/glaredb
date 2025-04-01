use std::collections::BTreeSet;

use glaredb_error::Result;

use super::OperatorPlanState;
use crate::execution::operators::hash_aggregate::{Aggregates, PhysicalHashAggregate};
use crate::execution::operators::{PlannedOperator, PlannedOperatorWithChildren};
use crate::expr::physical::column_expr::PhysicalColumnExpr;
use crate::logical::logical_distinct::LogicalDistinct;
use crate::logical::operator::{LogicalNode, Node};

impl OperatorPlanState<'_> {
    pub fn plan_distinct(
        &mut self,
        mut distinct: Node<LogicalDistinct>,
    ) -> Result<PlannedOperatorWithChildren> {
        let input = distinct.take_one_child_exact()?;
        let input_refs = input.get_output_table_refs(self.bind_context);
        let child = self.plan(input)?;

        // GROUP BY all expressions in the input.
        let mut group_exprs = Vec::new();
        for input_ref in input_refs {
            let table = self.bind_context.get_table(input_ref)?;

            for col_type in &table.column_types {
                let idx = group_exprs.len();
                group_exprs.push(PhysicalColumnExpr::new(idx, col_type.clone()));
            }
        }

        let grouping_set: BTreeSet<_> = (0..group_exprs.len()).collect();

        let aggregates = Aggregates {
            groups: group_exprs,
            grouping_functions: Vec::new(),
            aggregates: Vec::new(), // TODO: Would need to change for ON
        };

        // GROUP_VALS is ordered first. An additional projection might be needed
        // when adding aggregates for ON (FIRST_VALUE, ARBITRARY).
        //
        // For now, we can just keep the groups vals as-is.
        let operator = PhysicalHashAggregate::new(aggregates, vec![grouping_set]);

        Ok(PlannedOperatorWithChildren {
            operator: PlannedOperator::new_execute(self.id_gen.next_id(), operator),
            children: vec![child],
        })
    }
}
