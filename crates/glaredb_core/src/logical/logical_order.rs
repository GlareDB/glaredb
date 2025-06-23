use glaredb_error::Result;

use super::binder::bind_context::BindContext;
use super::binder::bind_query::bind_modifier::BoundOrderByExpr;
use super::binder::table_list::TableRef;
use super::operator::{LogicalNode, Node};
use crate::explain::explainable::{EntryBuilder, ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalOrder {
    /// Sort expressions.
    pub exprs: Vec<BoundOrderByExpr>,
    /// Optional limit hint that can be provided to the physical operator to
    /// reduce the amount of work needed for the sort.
    pub limit_hint: Option<usize>,
}

impl Explainable for LogicalOrder {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        EntryBuilder::new("Order", conf)
            .with_values("expressions", &self.exprs)
            .with_value_opt("limit_hint", self.limit_hint)
            .build()
    }
}

impl LogicalNode for Node<LogicalOrder> {
    fn name(&self) -> &'static str {
        "Order"
    }

    fn get_output_table_refs(&self, bind_context: &BindContext) -> Vec<TableRef> {
        self.get_children_table_refs(bind_context)
    }

    fn for_each_expr<'a, F>(&'a self, mut func: F) -> Result<()>
    where
        F: FnMut(&'a Expression) -> Result<()>,
    {
        for order_expr in &self.node.exprs {
            func(&order_expr.expr)?;
        }
        Ok(())
    }

    fn for_each_expr_mut<'a, F>(&'a mut self, mut func: F) -> Result<()>
    where
        F: FnMut(&'a mut Expression) -> Result<()>,
    {
        for order_expr in &mut self.node.exprs {
            func(&mut order_expr.expr)?;
        }
        Ok(())
    }
}
