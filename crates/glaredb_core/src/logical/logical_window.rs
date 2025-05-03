use glaredb_error::Result;

use super::binder::bind_context::BindContext;
use super::binder::table_list::TableRef;
use super::operator::{LogicalNode, Node};
use crate::explain::explainable::{EntryBuilder, ExplainConfig, ExplainEntry, Explainable};
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
        EntryBuilder::new("Window", conf)
            .with_contextual_values("windows", &self.windows)
            .with_value_if_verbose("table_ref", self.windows_table)
            .build()
    }
}

impl LogicalNode for Node<LogicalWindow> {
    fn name(&self) -> &'static str {
        "Window"
    }

    fn get_output_table_refs(&self, _bind_context: &BindContext) -> Vec<TableRef> {
        vec![self.node.windows_table]
    }

    fn for_each_expr<'a, F>(&'a self, mut func: F) -> Result<()>
    where
        F: FnMut(&'a Expression) -> Result<()>,
    {
        for expr in &self.node.windows {
            func(expr)?;
        }
        Ok(())
    }

    fn for_each_expr_mut<'a, F>(&'a mut self, mut func: F) -> Result<()>
    where
        F: FnMut(&'a mut Expression) -> Result<()>,
    {
        for expr in &mut self.node.windows {
            func(expr)?;
        }
        Ok(())
    }
}
