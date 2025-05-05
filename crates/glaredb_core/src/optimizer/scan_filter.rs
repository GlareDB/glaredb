use glaredb_error::Result;

use super::OptimizeRule;
use crate::expr::Expression;
use crate::expr::comparison_expr::ComparisonOperator;
use crate::expr::conjunction_expr::ConjunctionOperator;
use crate::logical::binder::bind_context::BindContext;
use crate::logical::operator::LogicalOperator;
use crate::storage::scan_filter::ScanFilter;

/// Pushes scan filters to scan.
///
/// Done separately from the normal filter push down since as we want to do the
/// scan filters after we prune columns.
#[derive(Debug, Clone, Copy)]
pub struct ScanFilterPushdown;

impl OptimizeRule for ScanFilterPushdown {
    fn optimize(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        // Base plan
        optimize_inner(&mut plan)?;

        // Materializations
        for mat in bind_context.iter_materializations_mut() {
            optimize_inner(&mut mat.plan)?;
        }

        Ok(plan)
    }
}

fn optimize_inner(plan: &mut LogicalOperator) -> Result<()> {
    if let LogicalOperator::Filter(filter) = plan {
        debug_assert_eq!(1, filter.children.len());
        if let LogicalOperator::Scan(scan) = &mut filter.children[0] {
            // Clone only epxressions that we know we can easily handle, at
            // least for now.
            //
            // At some point we'll have a way for the scan to signal
            // exact/inexact filters so that we could potentially remove from
            // the filter.
            let mut filters = match &filter.node.filter {
                Expression::Conjunction(conj) if conj.op == ConjunctionOperator::And => conj
                    .expressions
                    .iter()
                    .filter_map(|expr| {
                        let column_refs = expr.get_column_references();
                        if column_refs.len() != 1 {
                            return None;
                        }

                        match expr {
                            Expression::Comparison(cmp) if cmp.op == ComparisonOperator::Eq => {
                                match (cmp.left.as_ref(), cmp.right.as_ref()) {
                                    (Expression::Column(_), right) if right.is_const_foldable() => {
                                        Some(ScanFilter {
                                            expression: expr.clone(),
                                        })
                                    }
                                    (left, Expression::Column(_)) if left.is_const_foldable() => {
                                        Some(ScanFilter {
                                            expression: expr.clone(),
                                        })
                                    }
                                    _ => None,
                                }
                            }
                            _ => None,
                        }
                    })
                    .collect(),
                other => vec![ScanFilter {
                    expression: other.clone(),
                }],
            };

            scan.node.scan_filters.append(&mut filters);
        }
    }

    for child in plan.children_mut() {
        optimize_inner(child)?;
    }

    Ok(())
}
