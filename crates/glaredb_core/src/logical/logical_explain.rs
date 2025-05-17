use glaredb_error::Result;

use super::binder::bind_context::BindContext;
use super::binder::table_list::TableRef;
use super::operator::{LogicalNode, Node};
use crate::explain::explainable::{EntryBuilder, ExplainConfig, ExplainEntry, Explainable};
use crate::explain::node::ExplainedPlan;
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
    pub logical_unoptimized: ExplainedPlan,
    pub logical_optimized: Option<ExplainedPlan>,
}

impl Explainable for LogicalExplain {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        EntryBuilder::new("Explain", conf).build()
    }
}

impl LogicalNode for Node<LogicalExplain> {
    fn name(&self) -> &'static str {
        "Explain"
    }

    fn get_output_table_refs(&self, _bind_context: &BindContext) -> Vec<TableRef> {
        Vec::new()
    }

    fn for_each_expr<'a, F>(&'a self, _func: F) -> Result<()>
    where
        F: FnMut(&'a Expression) -> Result<()>,
    {
        Ok(())
    }

    fn for_each_expr_mut<'a, F>(&'a mut self, _func: F) -> Result<()>
    where
        F: FnMut(&'a mut Expression) -> Result<()>,
    {
        Ok(())
    }
}
