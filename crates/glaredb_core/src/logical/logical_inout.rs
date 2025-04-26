use glaredb_error::Result;

use super::binder::bind_context::BindContext;
use super::binder::table_list::TableRef;
use super::operator::{LogicalNode, Node};
use crate::explain::explainable::{EntryBuilder, ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;
use crate::functions::table::PlannedTableFunction;

/// A table function that accepts inputs and produces outputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalTableExecute {
    /// Table ref for referencing the output of this function.
    pub function_table_ref: TableRef,
    /// The table function.
    pub function: PlannedTableFunction,
    /// Table ref for referencing the projected expressions.
    ///
    /// This only gets set during subquery decorrelation to project the original
    /// inputs through the node.
    pub projected_table_ref: Option<TableRef>,
    /// Expressions that get projected out of this node alongside the results of
    /// the table function.
    pub projected_outputs: Vec<Expression>,
}

impl Explainable for LogicalTableExecute {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        EntryBuilder::new("TableExecute", conf)
            .with_value("function", self.function.name)
            .with_contextual_values("inputs", &self.function.bind_state.input.positional)
            .with_value_if_verbose("function_table_ref", self.function_table_ref)
            .with_value_opt_if_verbose("projected_table_ref", self.projected_table_ref)
            .with_contextual_values_opt_if_verbose(
                "projected_outputs",
                self.projected_table_ref.map(|_| &self.projected_outputs),
            )
            .build()
    }
}

impl LogicalNode for Node<LogicalTableExecute> {
    fn name(&self) -> &'static str {
        "TableExecute"
    }

    fn get_output_table_refs(&self, _bind_context: &BindContext) -> Vec<TableRef> {
        match self.node.projected_table_ref {
            Some(projected_table_ref) => {
                vec![self.node.function_table_ref, projected_table_ref]
            }
            _ => {
                vec![self.node.function_table_ref]
            }
        }
    }

    fn for_each_expr<F>(&self, func: &mut F) -> Result<()>
    where
        F: FnMut(&Expression) -> Result<()>,
    {
        // TODO: What about named arguments?
        for expr in &self.node.function.bind_state.input.positional {
            func(expr)?
        }
        for expr in &self.node.projected_outputs {
            func(expr)?
        }

        Ok(())
    }

    fn for_each_expr_mut<F>(&mut self, func: &mut F) -> Result<()>
    where
        F: FnMut(&mut Expression) -> Result<()>,
    {
        for expr in &mut self.node.function.bind_state.input.positional {
            func(expr)?
        }
        for expr in &mut self.node.projected_outputs {
            func(expr)?
        }

        Ok(())
    }
}
