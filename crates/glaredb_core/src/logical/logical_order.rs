use glaredb_error::Result;

use super::binder::bind_context::BindContext;
use super::binder::bind_query::bind_modifier::BoundOrderByExpr;
use super::binder::table_list::TableRef;
use super::operator::{LogicalNode, Node};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;

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
    fn name(&self) -> &'static str {
        "Order"
    }

    fn get_output_table_refs(&self, bind_context: &BindContext) -> Vec<TableRef> {
        self.get_children_table_refs(bind_context)
    }

    fn for_each_expr<F>(&self, func: &mut F) -> Result<()>
    where
        F: FnMut(&Expression) -> Result<()>,
    {
        for order_expr in &self.node.exprs {
            func(&order_expr.expr)?;
        }
        Ok(())
    }

    fn for_each_expr_mut<F>(&mut self, func: &mut F) -> Result<()>
    where
        F: FnMut(&mut Expression) -> Result<()>,
    {
        for order_expr in &mut self.node.exprs {
            func(&mut order_expr.expr)?;
        }
        Ok(())
    }
}
