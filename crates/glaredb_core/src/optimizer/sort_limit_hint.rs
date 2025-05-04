use glaredb_error::Result;

use super::OptimizeRule;
use crate::logical::binder::bind_context::BindContext;
use crate::logical::logical_limit::LogicalLimit;
use crate::logical::operator::{LogicalOperator, Node};

/// For ORDER BY followed by LIMIT, provide a limit hint to the ORDER BY.
///
/// Note that this is just a hint, we still need the limit at the end to ensure
/// a hard limit.
///
/// This may be replaced with a dedicated TopK operator in the future.
#[derive(Debug, Clone, Copy)]
pub struct SortLimitHint;

impl OptimizeRule for SortLimitHint {
    fn optimize(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        // Optimize base plan.
        optimize_inner(&mut plan)?;

        // Optimize materializations.
        for mat in bind_context.iter_materializations_mut() {
            optimize_inner(&mut mat.plan)?;
        }

        Ok(plan)
    }
}

fn optimize_inner(plan: &mut LogicalOperator) -> Result<()> {
    if let LogicalOperator::Limit(limit) = plan {
        try_set_limit_hint(limit)?;
        // Fall through to children...
    }

    for child in plan.children_mut() {
        optimize_inner(child)?;
    }

    Ok(())
}

fn try_set_limit_hint(limit: &mut Node<LogicalLimit>) -> Result<()> {
    debug_assert_eq!(1, limit.children.len());

    if let LogicalOperator::Order(order) = &mut limit.children[0] {
        // Our limit hint also needs to include the offset.
        let offset = limit.node.offset.unwrap_or(0);
        let limit_hint = limit.node.limit + offset;

        order.node.limit_hint = Some(limit_hint);
    }

    Ok(())
}
