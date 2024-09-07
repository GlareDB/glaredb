use super::{
    binder::bind_context::TableRef,
    operator::{LogicalNode, LogicalOperator, Node},
};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

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
    fn get_output_table_refs(&self) -> Vec<TableRef> {
        Vec::new()
    }
}
