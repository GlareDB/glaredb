use glaredb_error::Result;

use crate::logical::logical_aggregate::LogicalAggregate;
use crate::logical::logical_filter::LogicalFilter;
use crate::logical::logical_project::LogicalProject;
use crate::logical::operator::{LogicalOperator, Node};
use crate::logical::statistics::assumptions::DEFAULT_SELECTIVITY;
use crate::logical::statistics::StatisticsValue;

/// Propagates estimated cardinalities from the bottom up.
pub fn propagate_estimated_cardinality(op: &mut LogicalOperator) -> Result<()> {
    match op {
        LogicalOperator::Project(op) => propagate_project(op)?,
        LogicalOperator::Filter(op) => propagate_filter(op)?,
        LogicalOperator::Aggregate(op) => propagate_aggregate(op)?,
        _ => (),
    }

    Ok(())
}

fn propagate_project(op: &mut Node<LogicalProject>) -> Result<()> {
    let child = op.get_nth_child_mut(0)?;
    propagate_estimated_cardinality(child)?;
    op.estimated_cardinality = child.estimated_cardinality();

    Ok(())
}

fn propagate_filter(op: &mut Node<LogicalFilter>) -> Result<()> {
    let child = op.get_nth_child_mut(0)?;
    propagate_estimated_cardinality(child)?;

    let estimated = match child.estimated_cardinality().value() {
        Some(v) => {
            // TODO: Use more specific selectivities if we can.
            let est = (*v as f64) * DEFAULT_SELECTIVITY;
            StatisticsValue::Estimated(est as usize)
        }
        None => StatisticsValue::Unknown,
    };

    op.estimated_cardinality = estimated;

    Ok(())
}

fn propagate_aggregate(op: &mut Node<LogicalAggregate>) -> Result<()> {
    let child = op.get_nth_child_mut(0)?;
    propagate_estimated_cardinality(child)?;

    let child_card = child.estimated_cardinality();

    if op.node.group_exprs.is_empty() {
        op.estimated_cardinality = StatisticsValue::Estimated(1);
    } else {
        let estimated = match child_card.value() {
            Some(v) => {
                // TODO: Do something a bit more reasonable. We should be
                // looking at the columns in the group expressions and checking
                // distinc values.
                let est = (*v as f64) * DEFAULT_SELECTIVITY;
                StatisticsValue::Estimated(est as usize)
            }
            None => StatisticsValue::Unknown,
        };

        op.estimated_cardinality = estimated;
    }

    Ok(())
}
