use std::collections::BTreeSet;

use rayexec_error::Result;

use super::binder::bind_context::BindContext;
use super::binder::table_list::TableRef;
use super::operator::{LogicalNode, Node};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupingFunction {
    /// Indices pointing to expressions in the GROUP BY.
    pub group_exprs: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// Table reference for getting the GROUPING value for a group.
    ///
    /// This is Some if there's an explicit GROUPING function call in the query.
    /// Internally, the hash aggregate produces a group id based on null
    /// bitmaps, and that id is stored with the group. This let's us
    /// disambiguate NULL values from NULLs in the column vs NULLs produced by
    /// the null bitmap.
    ///
    /// Follows postgres semantics.
    /// See: <https://www.postgresql.org/docs/current/functions-aggregate.html#FUNCTIONS-GROUPING-TABLE>
    pub grouping_functions_table: Option<TableRef>,
    /// Grouping function calls.
    ///
    /// Empty if `grouping_set_table` is None.
    pub grouping_functions: Vec<GroupingFunction>,
}

impl Explainable for LogicalAggregate {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        let mut ent = ExplainEntry::new("Aggregate").with_values_context(
            "aggregates",
            conf,
            &self.aggregates,
        );

        if conf.verbose {
            ent = ent.with_value("table_ref", self.aggregates_table);

            if let Some(grouping_set_table) = self.grouping_functions_table {
                ent = ent.with_value("grouping_set_table_ref", grouping_set_table);
            }
        }

        if let Some(group_table) = &self.group_table {
            ent = ent.with_values_context("group_expressions", conf, &self.group_exprs);

            if conf.verbose {
                ent = ent.with_value("group_table_ref", group_table);
            }
        }

        ent
    }
}

impl LogicalNode for Node<LogicalAggregate> {
    fn get_output_table_refs(&self, _bind_context: &BindContext) -> Vec<TableRef> {
        // Order of refs here need to match physical output ordering of the
        // aggregate operators.
        //
        // Grouped aggregates: [GROUPS, AGG_RESULTS, GROUPING_FUNCS]
        let mut refs = Vec::new();
        if let Some(group_table) = self.node.group_table {
            refs.push(group_table);
        }
        refs.push(self.node.aggregates_table);
        if let Some(grouping_set_table) = self.node.grouping_functions_table {
            refs.push(grouping_set_table);
        }
        refs
    }

    fn for_each_expr<F>(&self, func: &mut F) -> Result<()>
    where
        F: FnMut(&Expression) -> Result<()>,
    {
        for expr in &self.node.aggregates {
            func(expr)?;
        }
        for expr in &self.node.group_exprs {
            func(expr)?;
        }
        Ok(())
    }

    fn for_each_expr_mut<F>(&mut self, func: &mut F) -> Result<()>
    where
        F: FnMut(&mut Expression) -> Result<()>,
    {
        for expr in &mut self.node.aggregates {
            func(expr)?;
        }
        for expr in &mut self.node.group_exprs {
            func(expr)?;
        }
        Ok(())
    }
}
