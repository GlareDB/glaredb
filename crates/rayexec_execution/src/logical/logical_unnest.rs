use rayexec_error::Result;

use super::binder::bind_context::{BindContext, TableRef};
use super::operator::{LogicalNode, Node};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalUnnest {
    /// Output table ref for the unnest.
    pub table_ref: TableRef,
    /// All expressions being unnested.
    pub expressions: Vec<Expression>,
}

impl Explainable for LogicalUnnest {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        let mut ent =
            ExplainEntry::new("Unnest").with_values_context("expressions", conf, &self.expressions);

        if conf.verbose {
            ent = ent.with_value("table_ref", self.table_ref);
        }

        ent
    }
}

impl LogicalNode for Node<LogicalUnnest> {
    fn get_output_table_refs(&self, _bind_context: &BindContext) -> Vec<TableRef> {
        vec![self.node.table_ref]
    }

    fn for_each_expr<F>(&self, func: &mut F) -> Result<()>
    where
        F: FnMut(&Expression) -> Result<()>,
    {
        for expr in &self.node.expressions {
            func(expr)?;
        }
        Ok(())
    }

    fn for_each_expr_mut<F>(&mut self, func: &mut F) -> Result<()>
    where
        F: FnMut(&mut Expression) -> Result<()>,
    {
        for expr in &mut self.node.expressions {
            func(expr)?;
        }
        Ok(())
    }
}
