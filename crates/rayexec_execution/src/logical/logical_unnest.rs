use rayexec_error::Result;

use super::binder::bind_context::{BindContext, TableRef};
use super::operator::{LogicalNode, Node};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalUnnest {
    /// Output table ref for expressions that won't be unnested, but still
    /// projected out of this node.
    pub projection_ref: TableRef,
    /// Output table ref expressions being unnested.
    pub unnest_ref: TableRef,
    /// All expressions being unnested.
    pub unnest_expressions: Vec<Expression>,
    /// All expressions being projected.
    pub project_expressions: Vec<Expression>,
}

impl Explainable for LogicalUnnest {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        let mut ent = ExplainEntry::new("Unnest")
            .with_values_context("unnest_expressions", conf, &self.unnest_expressions)
            .with_values_context("project_expressions", conf, &self.project_expressions);

        if conf.verbose {
            ent = ent
                .with_value("unnest_table_ref", self.unnest_ref)
                .with_value("project_table_ref", self.projection_ref);
        }

        ent
    }
}

impl LogicalNode for Node<LogicalUnnest> {
    fn get_output_table_refs(&self, _bind_context: &BindContext) -> Vec<TableRef> {
        vec![self.node.projection_ref, self.node.unnest_ref]
    }

    fn for_each_expr<F>(&self, func: &mut F) -> Result<()>
    where
        F: FnMut(&Expression) -> Result<()>,
    {
        for expr in &self.node.project_expressions {
            func(expr)?;
        }
        for expr in &self.node.unnest_expressions {
            func(expr)?;
        }
        Ok(())
    }

    fn for_each_expr_mut<F>(&mut self, func: &mut F) -> Result<()>
    where
        F: FnMut(&mut Expression) -> Result<()>,
    {
        for expr in &mut self.node.project_expressions {
            func(expr)?;
        }
        for expr in &mut self.node.unnest_expressions {
            func(expr)?;
        }
        Ok(())
    }
}
