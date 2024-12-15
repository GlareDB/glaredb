use rayexec_error::Result;

use super::binder::bind_context::BindContext;
use super::binder::table_list::TableRef;
use super::operator::{LogicalNode, Node};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;
use crate::functions::table::PlannedTableFunction;

/// A table function that accepts inputs and produces outputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalInOut {
    /// Table ref for referencing the output of this function.
    pub table_ref: TableRef,
    /// The table function.
    pub function: PlannedTableFunction,
}

impl Explainable for LogicalInOut {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        let mut ent = ExplainEntry::new("TableInOut")
            .with_value("function", self.function.function.name())
            .with_values_context("inputs", conf, &self.function.positional_inputs);

        if conf.verbose {
            ent = ent.with_value("table_ref", self.table_ref);
        }

        ent
    }
}

impl LogicalNode for Node<LogicalInOut> {
    fn get_output_table_refs(&self, _bind_context: &BindContext) -> Vec<TableRef> {
        vec![self.node.table_ref]
    }

    fn for_each_expr<F>(&self, func: &mut F) -> Result<()>
    where
        F: FnMut(&Expression) -> Result<()>,
    {
        for expr in &self.node.function.positional_inputs {
            func(expr)?
        }
        Ok(())
    }

    fn for_each_expr_mut<F>(&mut self, func: &mut F) -> Result<()>
    where
        F: FnMut(&mut Expression) -> Result<()>,
    {
        for expr in &mut self.node.function.positional_inputs {
            func(expr)?
        }
        Ok(())
    }
}
