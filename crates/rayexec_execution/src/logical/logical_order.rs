use super::{
    binder::{bind_context::TableRef, bind_query::bind_modifier::BoundOrderByExpr},
    operator::{LogicalNode, Node},
};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalOrder {
    pub exprs: Vec<BoundOrderByExpr>,
}

impl Explainable for LogicalOrder {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Order").with_values("expressions", &self.exprs)
    }
}

impl LogicalNode for Node<LogicalOrder> {
    fn get_output_table_refs(&self) -> Vec<TableRef> {
        self.get_children_table_refs()
    }
}
