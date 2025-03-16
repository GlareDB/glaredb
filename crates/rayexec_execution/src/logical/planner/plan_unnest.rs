use glaredb_error::Result;

use crate::arrays::datatype::DataType;
use crate::expr::column_expr::{ColumnExpr, ColumnReference};
use crate::expr::Expression;
use crate::logical::binder::bind_context::BindContext;
use crate::logical::binder::table_list::TableRef;
use crate::logical::logical_unnest::LogicalUnnest;
use crate::logical::operator::{LocationRequirement, LogicalNode, LogicalOperator, Node};
use crate::logical::statistics::StatisticsValue;

// TODO: This should be extended to support arbitrary table functions.
//
// - Left lateral join between function inputs, and the function itself.
// - Physical plan would need to handle multiple functions at once (similar to
//   the current unnest plan).
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

        // We have one or more UNNESTs.
        //
        // We create two new table refs to differentiate between expressions
        // that should be projected through the UNNEST, and expessions that
        // should actually be unnested.
        let unnest_ref = bind_context.new_ephemeral_table()?;
        let projection_ref = bind_context.new_ephemeral_table()?;

        let mut unnest_expressions = Vec::new();
        let mut project_expressions = Vec::new();

        plan.for_each_expr_mut(&mut |expr| {
            // Generate replacement column expr based on number of extracted
            // expressions so far.
            let did_extract = extract_unnest(expr, unnest_ref, &mut unnest_expressions)?;

            // If we didn't extract, we'll need to handle this expression as a
            // projection through the unnest. Just swap out the original
            // expression with a column ref.
            if !did_extract {
                let col_idx = project_expressions.len();
                let reference = ColumnReference {
                    table_scope: projection_ref,
                    column: col_idx,
                };
                let datatype = bind_context.get_column_type(reference)?;
                let replace = Expression::Column(ColumnExpr {
                    reference,
                    datatype,
                });

                let orig = std::mem::replace(expr, replace);

                project_expressions.push(orig);
            }

            Ok(())
        })?;

        // Update table refs with the proper columns/types
        for (idx, expr) in unnest_expressions.iter().enumerate() {
            // Need to store the type that's being produced from the unnest, so
            // unwrap the list data type.
            let datatype = match expr.datatype()? {
                DataType::List(list) => list.datatype.as_ref().clone(),
                other => other,
            };

            bind_context.push_column_for_table(
                unnest_ref,
                format!("__generated_unnest{idx}"),
                datatype,
            )?;
        }

        for (idx, expr) in project_expressions.iter().enumerate() {
            // Just plain projections, no need to modify types.
            let datatype = expr.datatype()?;
            bind_context.push_column_for_table(
                projection_ref,
                format!("__generated_project{idx}"),
                datatype,
            )?;
        }

        let unnest_children = std::mem::take(plan.children_mut());
        let unnest = LogicalOperator::Unnest(Node {
            node: LogicalUnnest {
                projection_ref,
                unnest_ref,
                unnest_expressions,
                project_expressions,
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

/// Try to extract any unnest expressions, replacing the original expression
/// with a column reference that points to the extracted expression.
///
/// Return true if at least one expression was extracted.
fn extract_unnest(
    expr: &mut Expression,
    unnest_ref: TableRef,
    extracted: &mut Vec<Expression>,
) -> Result<bool> {
    match expr {
        Expression::Unnest(unnest) => {
            // Replace with the column expr that'll represent the output of the
            // UNNEST.
            let col_idx = extracted.len();
            let datatype = unnest.datatype()?;
            let inner = std::mem::replace(
                expr,
                Expression::Column(ColumnExpr {
                    reference: ColumnReference {
                        table_scope: unnest_ref,
                        column: col_idx,
                    },
                    datatype,
                }),
            );

            // Note we don't support nested UNNESTs.
            match inner {
                Expression::Unnest(unnest) => {
                    extracted.push(*unnest.expr);
                }
                _ => unreachable!(),
            }

            Ok(true)
        }
        other => {
            let mut did_extract = false;
            other.for_each_child_mut(&mut |child| {
                let child_did_extract = extract_unnest(child, unnest_ref, extracted)?;
                did_extract |= child_did_extract;

                Ok(())
            })?;

            Ok(did_extract)
        }
    }
}
