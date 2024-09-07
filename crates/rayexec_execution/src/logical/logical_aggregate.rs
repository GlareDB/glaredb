use std::collections::BTreeSet;

use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;

use super::binder::bind_context::TableRef;
use super::operator::{LogicalNode, Node};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalAggregate {
    /// Table ref that represents output of aggregate expressions.
    pub aggregates_table: TableRef,
    /// Aggregate expressions.
    pub aggregates: Vec<Expression>,
    /// Table ref representing output of the group by expressions.
    ///
    /// Will be None if this aggregate does not have an associated GROUP BY.
    pub group_table: Option<TableRef>,
    /// Expressions to group on.
    pub group_exprs: Vec<Expression>,
    /// Grouping set referencing group exprs.
    pub grouping_sets: Option<Vec<BTreeSet<usize>>>,
}

impl Explainable for LogicalAggregate {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        let mut ent = ExplainEntry::new("Aggregate").with_values("aggregates", &self.aggregates);

        if conf.verbose {
            ent = ent.with_value("table_ref", self.aggregates_table);
        }

        if let Some(group_table) = &self.group_table {
            ent = ent.with_values("group_expressions", &self.group_exprs);

            if conf.verbose {
                ent = ent.with_value("group_table_ref", group_table);
            }
        }

        ent
    }
}

impl LogicalNode for Node<LogicalAggregate> {
    fn get_output_table_refs(&self) -> Vec<TableRef> {
        let mut refs = vec![self.node.aggregates_table];
        if let Some(group_table) = self.node.group_table {
            refs.push(group_table);
        }
        refs
    }
}
