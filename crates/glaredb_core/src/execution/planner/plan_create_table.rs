use glaredb_error::Result;

use super::OperatorPlanState;
use crate::arrays::datatype::DataType;
use crate::catalog::create::CreateTableInfo;
use crate::execution::operators::ungrouped_aggregate::PhysicalUngroupedAggregate;
use crate::execution::operators::{PlannedOperator, PlannedOperatorWithChildren};
use crate::expr;
use crate::expr::physical::PhysicalAggregateExpression;
use crate::functions::aggregate::builtin::sum::FUNCTION_SET_SUM;
use crate::logical::logical_create::LogicalCreateTable;
use crate::logical::operator::Node;

impl OperatorPlanState<'_> {
    pub fn plan_create_table(
        &mut self,
        mut create: Node<LogicalCreateTable>,
    ) -> Result<PlannedOperatorWithChildren> {
        let db = self.db_context.require_get_database(&create.node.catalog)?;

        if create.children.is_empty() {
            // Normal create table.
            let info = CreateTableInfo {
                name: create.node.name,
                columns: create.node.columns,
                on_conflict: create.node.on_conflict,
            };

            let operator = db.plan_create_table(&mut self.id_gen, &create.node.schema, info)?;
            Ok(PlannedOperatorWithChildren {
                operator,
                children: Vec::new(),
            })
        } else {
            // CTAS.
            let input = create.take_one_child_exact()?;
            let child = self.plan(input)?;

            let info = CreateTableInfo {
                name: create.node.name,
                columns: create.node.columns,
                on_conflict: create.node.on_conflict,
            };

            let operator = db.plan_create_table_as(&mut self.id_gen, &create.node.schema, info)?;
            let mut planned = PlannedOperatorWithChildren {
                operator,
                children: vec![child],
            };

            if !self.config.per_partition_counts {
                // Sum counts across partitions.
                let sum = expr::bind_aggregate_function(
                    &FUNCTION_SET_SUM,
                    vec![expr::column((0, 0), DataType::int64())],
                )?;

                let agg = PhysicalUngroupedAggregate::try_new([PhysicalAggregateExpression::new(
                    sum,
                    [(0, DataType::int64())],
                )])?;

                planned = PlannedOperatorWithChildren {
                    operator: PlannedOperator::new_execute(self.id_gen.next_id(), agg),
                    children: vec![planned],
                }
            }

            Ok(planned)
        }
    }
}
