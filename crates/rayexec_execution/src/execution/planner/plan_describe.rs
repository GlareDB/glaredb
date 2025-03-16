use glaredb_error::Result;

use super::OperatorPlanState;
use crate::execution::operators::empty::PhysicalEmpty;
use crate::execution::operators::values::PhysicalValues;
use crate::execution::operators::{PlannedOperator, PlannedOperatorWithChildren};
use crate::expr::physical::literal_expr::PhysicalLiteralExpr;
use crate::logical::logical_describe::LogicalDescribe;
use crate::logical::operator::Node;

impl OperatorPlanState<'_> {
    pub fn plan_describe(
        &mut self,
        describe: Node<LogicalDescribe>,
    ) -> Result<PlannedOperatorWithChildren> {
        let _location = describe.location;

        let fields = &describe.node.schema.fields;
        let mut row_exprs = Vec::with_capacity(fields.len());

        for field in fields {
            // Push [name, datatype] expressions for each row.
            row_exprs.push(vec![
                PhysicalLiteralExpr::new(field.name.clone()).into(),
                PhysicalLiteralExpr::new(field.datatype.to_string()).into(),
            ]);
        }

        let operator = PhysicalValues::new(row_exprs);

        Ok(PlannedOperatorWithChildren {
            operator: PlannedOperator::new_execute(operator),
            children: vec![PlannedOperatorWithChildren {
                operator: PlannedOperator::new_pull(PhysicalEmpty),
                children: Vec::new(),
            }],
        })
    }
}
