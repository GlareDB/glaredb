use glaredb_error::{Result, not_implemented};

use super::OperatorPlanState;
use crate::execution::operators::empty::PhysicalEmpty;
use crate::execution::operators::values::PhysicalValues;
use crate::execution::operators::{PlannedOperator, PlannedOperatorWithChildren};
use crate::explain::formatter::ExplainFormatter;
use crate::explain::node::ExplainNode;
use crate::expr::physical::PhysicalScalarExpression;
use crate::expr::physical::literal_expr::PhysicalLiteralExpr;
use crate::logical::logical_explain::LogicalExplain;
use crate::logical::operator::Node;

impl OperatorPlanState<'_> {
    pub fn plan_explain(
        &mut self,
        mut explain: Node<LogicalExplain>,
    ) -> Result<PlannedOperatorWithChildren> {
        let _location = explain.location;

        if explain.node.analyze {
            not_implemented!("explain analyze")
        }

        let input = explain.take_one_child_exact()?;
        let plan = self.plan(input)?;
        let plan_explain_node =
            ExplainNode::new_from_planned_operators(explain.node.verbose, &plan);

        let formatter = ExplainFormatter::new(explain.node.format);

        // Row expressions.
        // [PLAN_TYPE, PLAN_STRING]
        let mut rows: Vec<Vec<PhysicalScalarExpression>> = Vec::new();

        // Unoptimized
        let unoptim = formatter.format(&explain.node.logical_unoptimized)?;
        rows.push(vec![
            PhysicalLiteralExpr::new("unoptimized").into(),
            PhysicalLiteralExpr::new(unoptim).into(),
        ]);

        // Optimized
        if let Some(optim) = &explain.node.logical_optimized {
            let optim = formatter.format(optim)?;
            rows.push(vec![
                PhysicalLiteralExpr::new("optimized").into(),
                PhysicalLiteralExpr::new(optim).into(),
            ]);
        }

        // Physical
        let phys = formatter.format(&plan_explain_node)?;
        rows.push(vec![
            PhysicalLiteralExpr::new("physical").into(),
            PhysicalLiteralExpr::new(phys).into(),
        ]);

        let values = PhysicalValues::new(rows);

        Ok(PlannedOperatorWithChildren {
            operator: PlannedOperator::new_execute(values),
            children: vec![PlannedOperatorWithChildren {
                operator: PlannedOperator::new_pull(PhysicalEmpty),
                children: Vec::new(),
            }],
        })
    }
}
