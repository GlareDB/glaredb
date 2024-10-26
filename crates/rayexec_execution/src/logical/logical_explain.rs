use rayexec_error::Result;

use super::binder::bind_context::{BindContext, TableRef};
use super::operator::{LogicalNode, LogicalOperator, Node};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalExplain {
    pub analyze: bool,
    pub verbose: bool,
    pub format: ExplainFormat,
    pub logical_unoptimized: Box<LogicalOperator>,
    pub logical_optimized: Option<Box<LogicalOperator>>,
}

impl Explainable for LogicalExplain {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Explain")
    }
}

impl LogicalNode for Node<LogicalExplain> {
    fn get_output_table_refs(&self, _bind_context: &BindContext) -> Vec<TableRef> {
        Vec::new()
    }

    fn for_each_expr<F>(&self, _func: &mut F) -> Result<()>
    where
        F: FnMut(&Expression) -> Result<()>,
    {
        Ok(())
    }

    fn for_each_expr_mut<F>(&mut self, _func: &mut F) -> Result<()>
    where
        F: FnMut(&mut Expression) -> Result<()>,
    {
        Ok(())
    }
}
