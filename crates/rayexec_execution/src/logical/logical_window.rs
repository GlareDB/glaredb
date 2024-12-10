use rayexec_error::Result;

use super::binder::bind_context::BindContext;
use super::binder::table_list::TableRef;
use super::operator::{LogicalNode, Node};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalWindow {
    /// All window expressions.
    pub windows: Vec<Expression>,
    /// Table ref for the output of the windows.
    pub windows_table: TableRef,
}

impl Explainable for LogicalWindow {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        let mut ent =
            ExplainEntry::new("Window").with_values_context("windows", conf, &self.windows);

        if conf.verbose {
            ent = ent.with_value("table_ref", self.windows_table);
        }

        ent
    }
}

impl LogicalNode for Node<LogicalWindow> {
    fn get_output_table_refs(&self, _bind_context: &BindContext) -> Vec<TableRef> {
        vec![self.node.windows_table]
    }

    fn for_each_expr<F>(&self, func: &mut F) -> Result<()>
    where
        F: FnMut(&Expression) -> Result<()>,
    {
        for expr in &self.node.windows {
            func(expr)?;
        }
        Ok(())
    }

    fn for_each_expr_mut<F>(&mut self, func: &mut F) -> Result<()>
    where
        F: FnMut(&mut Expression) -> Result<()>,
    {
        for expr in &mut self.node.windows {
            func(expr)?;
        }
        Ok(())
    }
}
