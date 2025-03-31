use glaredb_error::{DbError, Result};

use super::PhysicalSortExpression;
use super::case_expr::PhysicalCaseExpr;
use super::cast_expr::PhysicalCastExpr;
use super::column_expr::PhysicalColumnExpr;
use super::literal_expr::PhysicalLiteralExpr;
use super::scalar_function_expr::PhysicalScalarFunctionExpr;
use crate::expr::physical::PhysicalScalarExpression;
use crate::expr::physical::case_expr::PhysicalWhenThen;
use crate::expr::{AsScalarFunctionSet, Expression};
use crate::functions::scalar::PlannedScalarFunction;
use crate::logical::binder::bind_query::bind_modifier::BoundOrderByExpr;
use crate::logical::binder::table_list::{TableList, TableRef};
use crate::util::fmt::displayable::IntoDisplayableSlice;

/// Plans logical expressions into their physical equivalents.
#[derive(Debug)]
pub struct PhysicalExpressionPlanner<'a> {
    pub table_list: &'a TableList,
}

impl<'a> PhysicalExpressionPlanner<'a> {
    /// Construct a physical expression planner with the given table list.
    ///
    /// When planning an expression, tables refs are provided to indicate the
    /// scope of that expression, and those refs are used to look up the tables
    /// in the list. This is done to map a "logical" column reference to a flat
    /// index in an input batch.
    pub fn new(table_list: &'a TableList) -> Self {
        PhysicalExpressionPlanner { table_list }
    }

    /// Plan more than one scalar expression.
    pub fn plan_scalars<'b>(
        &self,
        table_refs: &[TableRef],
        exprs: impl IntoIterator<Item = &'b Expression>,
    ) -> Result<Vec<PhysicalScalarExpression>> {
        exprs
            .into_iter()
            .map(|expr| self.plan_scalar(table_refs, expr))
            .collect::<Result<Vec<_>>>()
    }

    /// Plans a physical scalar expressions.
    ///
    /// Tables refs is a list of table references that represent valid
    /// expression inputs into some plan.
    ///
    /// If the expression does not reference a table, it indicates we didn't
    /// properly decorrelate the expression, and we error.
    ///
    /// The output expression list assumes that the input into an operator is a
    /// flat batch of columns. This means for a join, the batch will represent
    /// [left, right] table refs, and so column references on the right will
    /// take into account the number of columns on left.
    // TODO: Should probably take the owned expression.
    pub fn plan_scalar(
        &self,
        table_refs: &[TableRef],
        expr: &Expression,
    ) -> Result<PhysicalScalarExpression> {
        match expr {
            Expression::Column(col) => {
                // The optimizer should preserve columns in tables so we should
                // be able to look at the table list directly.
                //
                // If we get here and their's either a missing table for table
                // ref, or missing column for a table, then that should be
                // considered a bug.
                let mut offset = 0;
                for &table_ref in table_refs {
                    let table = self.table_list.get(table_ref)?;

                    if col.reference.table_scope == table_ref {
                        return Ok(PhysicalScalarExpression::Column(PhysicalColumnExpr {
                            idx: offset + col.reference.column,
                            datatype: col.datatype.clone(),
                        }));
                    }

                    offset += table.num_columns();
                }

                // Column not in any of our required tables, indicates
                // correlated column.
                Err(DbError::new(format!(
                    "Column expr not referencing a valid table ref, column: {col}, valid tables: {}",
                    table_refs.display_with_brackets(),
                )))
            }
            Expression::Literal(expr) => {
                Ok(PhysicalScalarExpression::Literal(PhysicalLiteralExpr {
                    literal: expr.literal.clone(),
                }))
            }
            Expression::ScalarFunction(expr) => {
                let physical_inputs = self.plan_scalars(table_refs, &expr.function.state.inputs)?;

                Ok(PhysicalScalarExpression::ScalarFunction(
                    PhysicalScalarFunctionExpr {
                        function: expr.function.clone(),
                        inputs: physical_inputs,
                    },
                ))
            }
            Expression::Cast(expr) => Ok(PhysicalScalarExpression::Cast(PhysicalCastExpr {
                to: expr.to.clone(),
                expr: Box::new(self.plan_scalar(table_refs, &expr.expr)?),
                cast_function: expr.cast_function.clone(),
            })),
            Expression::Comparison(expr) => self.plan_as_scalar_function(
                table_refs,
                expr.op,
                vec![expr.left.as_ref().clone(), expr.right.as_ref().clone()],
            ),
            Expression::Arith(expr) => self.plan_as_scalar_function(
                table_refs,
                expr.op,
                vec![expr.left.as_ref().clone(), expr.right.as_ref().clone()],
            ),
            Expression::Conjunction(expr) => {
                self.plan_as_scalar_function(table_refs, expr.op, expr.expressions.clone())
            }
            Expression::Negate(expr) => {
                self.plan_as_scalar_function(table_refs, expr.op, vec![expr.expr.as_ref().clone()])
            }
            Expression::Case(expr) => {
                let datatype = &expr.datatype;

                let cases = expr
                    .cases
                    .iter()
                    .map(|when_then| {
                        let when = self.plan_scalar(table_refs, &when_then.when)?;
                        let then = self.plan_scalar(table_refs, &when_then.then)?;
                        Ok(PhysicalWhenThen { when, then })
                    })
                    .collect::<Result<Vec<_>>>()?;

                let else_expr = self.plan_scalar(table_refs, &expr.else_expr)?;

                Ok(PhysicalScalarExpression::Case(PhysicalCaseExpr {
                    cases,
                    else_expr: Box::new(else_expr),
                    datatype: datatype.clone(),
                }))
            }
            other => Err(DbError::new(format!(
                "Unsupported scalar expression: {other}"
            ))),
        }
    }

    fn plan_as_scalar_function(
        &self,
        table_refs: &[TableRef],
        op: impl AsScalarFunctionSet,
        inputs: Vec<Expression>,
    ) -> Result<PhysicalScalarExpression> {
        let datatypes = inputs
            .iter()
            .map(|input| input.datatype())
            .collect::<Result<Vec<_>>>()?;
        let exact = op
            .as_scalar_function_set()
            .find_exact(&datatypes)
            .ok_or_else(|| DbError::new("Expected exact function signature match"))?;

        let bind_state = exact.call_bind(inputs)?;
        let planned = PlannedScalarFunction {
            name: op.as_scalar_function_set().name,
            raw: *exact,
            state: bind_state,
        };

        let physical_inputs = self.plan_scalars(table_refs, &planned.state.inputs)?;

        Ok(PhysicalScalarExpression::ScalarFunction(
            PhysicalScalarFunctionExpr {
                function: planned,
                inputs: physical_inputs,
            },
        ))
    }

    pub fn plan_sorts(
        &self,
        table_refs: &[TableRef],
        exprs: &[BoundOrderByExpr],
    ) -> Result<Vec<PhysicalSortExpression>> {
        exprs
            .iter()
            .map(|expr| self.plan_sort(table_refs, expr))
            .collect::<Result<Vec<_>>>()
    }

    /// Plan a sort expression.
    ///
    /// Sort expressions should be column expressions pointing to some
    /// pre-projection.
    pub fn plan_sort(
        &self,
        table_refs: &[TableRef],
        expr: &BoundOrderByExpr,
    ) -> Result<PhysicalSortExpression> {
        let scalar = self.plan_scalar(table_refs, &expr.expr)?;
        Ok(PhysicalSortExpression {
            column: scalar,
            desc: expr.desc,
            nulls_first: expr.nulls_first,
        })
    }
}
