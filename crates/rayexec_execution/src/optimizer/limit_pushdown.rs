use rayexec_error::Result;

use crate::logical::{binder::bind_context::BindContext, operator::LogicalOperator};

use super::OptimizeRule;

/// Push down a limit below a project.
#[derive(Debug)]
pub struct LimitPushdown;

impl OptimizeRule for LimitPushdown {
    fn optimize(
        &mut self,
        _bind_context: &mut BindContext,
        mut plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        if let LogicalOperator::Limit(mut limit) = plan {
            if limit.children.len() == 1
                && matches!(&limit.children[0], LogicalOperator::Project(_))
            {
                let mut project = limit.children.pop().unwrap();
                limit.children = std::mem::take(project.children_mut());
                *project.children_mut() = vec![LogicalOperator::Limit(limit)];

                plan = project;
            } else {
                plan = LogicalOperator::Limit(limit);
            }
        }

        plan.modify_replace_children(&mut |child| self.optimize(_bind_context, child))?;

        Ok(plan)
    }
}
