use glaredb_error::Result;

use super::binder::bind_context::BindContext;
use super::binder::table_list::TableRef;
use super::operator::{LogicalNode, Node};
use crate::explain::explainable::{EntryBuilder, ExplainConfig, ExplainEntry, Explainable};
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
        EntryBuilder::new("Unnest", conf)
            .with_contextual_values("unnest_expressions", &self.unnest_expressions)
            .with_contextual_values("project_expressions", &self.project_expressions)
            .with_value_if_verbose("unnest_table_ref", self.unnest_ref)
            .with_value_if_verbose("project_table_ref", self.projection_ref)
            .build()
    }
}

impl LogicalNode for Node<LogicalUnnest> {
    fn name(&self) -> &'static str {
        "Unnest"
    }

    fn get_output_table_refs(&self, _bind_context: &BindContext) -> Vec<TableRef> {
        vec![self.node.projection_ref, self.node.unnest_ref]
    }

    fn for_each_expr<'a, F>(&'a self, mut func: F) -> Result<()>
    where
        F: FnMut(&'a Expression) -> Result<()>,
    {
        for expr in &self.node.project_expressions {
            func(expr)?;
        }
        for expr in &self.node.unnest_expressions {
            func(expr)?;
        }
        Ok(())
    }

    fn for_each_expr_mut<'a, F>(&'a mut self, mut func: F) -> Result<()>
    where
        F: FnMut(&'a mut Expression) -> Result<()>,
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
