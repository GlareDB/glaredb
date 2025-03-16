use rayexec_error::Result;

use super::binder::bind_context::BindContext;
use super::binder::table_list::TableRef;
use super::operator::{LogicalNode, Node};
use crate::arrays::datatype::DataType;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalExpressionList {
    /// Table ref for the output of this list.
    pub table_ref: TableRef,
    /// Types for each "column" in the expression list.
    pub types: Vec<DataType>,
    /// "Rows" in the expression list.
    ///
    /// All rows should have the same column type.
    pub rows: Vec<Vec<Expression>>,
}

impl LogicalNode for Node<LogicalExpressionList> {
    fn get_output_table_refs(&self, _bind_context: &BindContext) -> Vec<TableRef> {
        vec![self.node.table_ref]
    }

    fn for_each_expr<F>(&self, func: &mut F) -> Result<()>
    where
        F: FnMut(&Expression) -> Result<()>,
    {
        for row in &self.node.rows {
            for expr in row {
                func(expr)?;
            }
        }
        Ok(())
    }

    fn for_each_expr_mut<F>(&mut self, func: &mut F) -> Result<()>
    where
        F: FnMut(&mut Expression) -> Result<()>,
    {
        for row in &mut self.node.rows {
            for expr in row {
                func(expr)?;
            }
        }
        Ok(())
    }
}

impl Explainable for LogicalExpressionList {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        let mut ent = ExplainEntry::new("ExpressionList")
            .with_value("num_rows", self.rows.len())
            .with_values("datatypes", &self.types);
        if conf.verbose {
            ent = ent.with_value("table_ref", self.table_ref)
        }
        ent
    }
}
