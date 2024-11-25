use rayexec_bullet::datatype::DataType;
use rayexec_error::Result;

use crate::expr::column_expr::ColumnExpr;
use crate::expr::Expression;
use crate::logical::binder::bind_context::{BindContext, TableRef};
use crate::logical::logical_unnest::LogicalUnnest;
use crate::logical::operator::{LocationRequirement, LogicalNode, LogicalOperator, Node};
use crate::logical::statistics::StatisticsValue;

#[derive(Debug)]
pub struct UnnestPlanner;

impl UnnestPlanner {
    /// Takes an existing logical plan and replaces all UNNEST expressions with
    /// references to a dedicated logical node that will handle the unnesting.
    pub fn plan_unnests(
        &self,
        bind_context: &mut BindContext,
        mut plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        let mut expr_count = 0; // Determines if we need to introduce a cross join.
        let mut has_unnest = false;
        plan.for_each_expr(&mut |expr| {
            expr_count += 1;
            if expr.contains_unnest() {
                has_unnest = true;
            }
            Ok(())
        })?;

        if !has_unnest {
            return Ok(plan);
        }

        // We have one or more UNNESTs, extract them all into a separate logical
        // unnest.
        let unnest_ref = bind_context.new_ephemeral_table()?;
        let mut extracted_exprs = Vec::new(); // TODO: Extract all, UNNEST will just be project

        plan.for_each_expr_mut(&mut |expr| {
            // Generate replacement column expr based on number of extracted
            // expressions so far.
            extract_unnest(expr, unnest_ref, &mut extracted_exprs)
        })?;

        // Place extracted exprs in the bind context.
        for (idx, expr) in extracted_exprs.iter().enumerate() {
            // Need to store the type that's being produced from the unnest, so
            // unwrap the list data type.
            let datatype = match expr.datatype(bind_context)? {
                DataType::List(list) => list.datatype.as_ref().clone(),
                other => other,
            };

            bind_context.push_column_for_table(
                unnest_ref,
                format!("__generated_unnest{idx}"),
                datatype,
            )?;
        }

        let unnest_children = std::mem::take(plan.children_mut());
        let unnest = LogicalOperator::Unnest(Node {
            node: LogicalUnnest {
                table_ref: unnest_ref,
                expressions: extracted_exprs,
            },
            estimated_cardinality: StatisticsValue::Unknown,
            location: LocationRequirement::Any,
            children: unnest_children,
        });

        // Update plan to now have the unnest as its child.
        *plan.children_mut() = vec![unnest];

        Ok(plan)
    }
}

fn extract_unnest(
    expr: &mut Expression,
    unnest_ref: TableRef,
    extracted: &mut Vec<Expression>,
) -> Result<()> {
    match expr {
        Expression::Unnest(_) => {
            // Replace with the column expr that'll represent the output of the
            // UNNEST.
            let col_idx = extracted.len();
            let inner = std::mem::replace(
                expr,
                Expression::Column(ColumnExpr {
                    table_scope: unnest_ref,
                    column: col_idx,
                }),
            );

            // Note we don't support nested UNNESTs.
            match inner {
                Expression::Unnest(unnest) => {
                    extracted.push(*unnest.expr);
                }
                _ => unreachable!(),
            }

            Ok(())
        }
        other => {
            other.for_each_child_mut(&mut |child| extract_unnest(child, unnest_ref, extracted))
        }
    }
}
