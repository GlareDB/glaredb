use rayexec_error::Result;

use super::binder::bind_context::TableRef;
use super::operator::{LogicalNode, Node};
use super::statistics::assumptions::DEFAULT_SELECTIVITY;
use super::statistics::{Statistics, StatisticsValue};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalFilter {
    pub filter: Expression,
}

impl Explainable for LogicalFilter {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Filter").with_value_context("predicate", conf, &self.filter)
    }
}

impl LogicalNode for Node<LogicalFilter> {
    fn get_output_table_refs(&self) -> Vec<TableRef> {
        self.get_children_table_refs()
    }

    fn cardinality(&self) -> StatisticsValue<usize> {
        let child_card = self
            .iter_child_cardinalities()
            .next()
            .expect("filter has child");

        match child_card.value() {
            Some(v) => StatisticsValue::Estimated(((*v as f64) * DEFAULT_SELECTIVITY) as usize),
            None => StatisticsValue::Unknown,
        }
    }

    fn get_statistics(&self) -> Statistics {
        let child_stats = self
            .iter_child_statistics()
            .next()
            .expect("filter has a child");

        if let Some(card) = child_stats.cardinality.value() {
            let estimated = (*card as f64) * DEFAULT_SELECTIVITY;
            return Statistics {
                cardinality: StatisticsValue::Estimated(estimated as usize),
                column_stats: None,
            };
        }

        Statistics::unknown()
    }

    fn for_each_expr<F>(&self, func: &mut F) -> Result<()>
    where
        F: FnMut(&Expression) -> Result<()>,
    {
        func(&self.node.filter)
    }

    fn for_each_expr_mut<F>(&mut self, func: &mut F) -> Result<()>
    where
        F: FnMut(&mut Expression) -> Result<()>,
    {
        func(&mut self.node.filter)
    }
}
