use fmtutil::IntoDisplayableSlice;
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::scalar::interval::Interval;
use rayexec_bullet::scalar::ScalarValue;
use rayexec_bullet::{field::TypeSchema, scalar::OwnedScalarValue};
use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

use crate::expr::scalar::{BinaryOperator, UnaryOperator};
use crate::functions::aggregate::GenericAggregateFunction;
use crate::functions::scalar::GenericScalarFunction;
use crate::functions::CastType;
use crate::logical::operator::LogicalExpression;

use super::{
    binder::{Bound, BoundFunctionReference},
    planner::PlanContext,
    scope::{Scope, TableReference},
};

/// An expanded select expression.
#[derive(Debug, Clone, PartialEq)]
pub enum ExpandedSelectExpr {
    /// A typical expression. Can be a reference to a column, or a more complex
    /// expression.
    Expr {
        /// The original expression.
        expr: ast::Expr<Bound>,
        /// Either an alias provided by the user or a name we generate for the
        /// expression. If this references a column, then the name will just
        /// match that column.
        name: String,
    },
    /// An index of a column in the current scope. This is needed for wildcards
    /// since they're expanded to match some number of columns in the current
    /// scope.
    Column {
        /// Index of the column the current scope.
        idx: usize,
        /// Name of the column.
        name: String,
    },
}

/// Context for planning expressions.
#[derive(Debug, Clone)]
pub struct ExpressionContext<'a> {
    /// Plan context containing this expression.
    pub plan_context: &'a PlanContext<'a>,
    /// Scope for this expression.
    pub scope: &'a Scope,
    /// Schema of input that this expression will be executed on.
    pub input: &'a TypeSchema,
}

impl<'a> ExpressionContext<'a> {
    pub fn new(plan_context: &'a PlanContext, scope: &'a Scope, input: &'a TypeSchema) -> Self {
        ExpressionContext {
            plan_context,
            scope,
            input,
        }
    }

    pub fn expand_select_expr(
        &self,
        expr: ast::SelectExpr<Bound>,
    ) -> Result<Vec<ExpandedSelectExpr>> {
        Ok(match expr {
            ast::SelectExpr::Expr(expr) => match &expr {
                ast::Expr::Ident(ident) => vec![ExpandedSelectExpr::Expr {
                    name: ident.as_normalized_string(),
                    expr,
                }],
                ast::Expr::CompoundIdent(ident) => vec![ExpandedSelectExpr::Expr {
                    name: ident
                        .last()
                        .map(|n| n.as_normalized_string())
                        .unwrap_or_else(|| "?column?".to_string()),
                    expr,
                }],
                _ => vec![ExpandedSelectExpr::Expr {
                    expr,
                    name: "?column?".to_string(),
                }],
            },
            ast::SelectExpr::AliasedExpr(expr, alias) => vec![ExpandedSelectExpr::Expr {
                expr,
                name: alias.into_normalized_string(),
            }],
            ast::SelectExpr::Wildcard(_wildcard) => {
                // TODO: Exclude, replace
                // TODO: Need to omit "hidden" columns that may have been added to the scope.
                self.scope
                    .items
                    .iter()
                    .enumerate()
                    .map(|(idx, col)| ExpandedSelectExpr::Column {
                        idx,
                        name: col.column.clone(),
                    })
                    .collect()
            }
            ast::SelectExpr::QualifiedWildcard(reference, _wildcard) => {
                // TODO: Exclude, replace
                // TODO: Need to omit "hidden" columns that may have been added to the scope.
                self.scope
                    .items
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, col)| match &col.alias {
                        // TODO: I got lazy. Need to check the entire reference.
                        Some(alias)
                            if alias.table
                                == reference.base().unwrap().into_normalized_string() =>
                        {
                            Some(ExpandedSelectExpr::Column {
                                idx,
                                name: col.column.clone(),
                            })
                        }
                        _ => None,
                    })
                    .collect()
            }
        })
    }

    /// Converts an AST expression to a logical expression.
    pub fn plan_expression(&self, expr: ast::Expr<Bound>) -> Result<LogicalExpression> {
        match expr {
            ast::Expr::Ident(ident) => self.plan_ident(ident),
            ast::Expr::CompoundIdent(idents) => self.plan_idents(idents),
            ast::Expr::Literal(literal) => Self::plan_literal(literal),
            ast::Expr::UnaryExpr { op, expr } => {
                let expr = self.plan_expression(*expr)?;
                let op = match op {
                    ast::UnaryOperator::Plus => return Ok(expr), // Nothing to do.
                    ast::UnaryOperator::Minus => UnaryOperator::Negate,
                    ast::UnaryOperator::Not => unimplemented!(),
                };

                Ok(LogicalExpression::Unary {
                    op,
                    expr: Box::new(expr),
                })
            }
            ast::Expr::BinaryExpr { left, op, right } => {
                let op = BinaryOperator::try_from(op)?;
                let left = self.plan_expression(*left)?;
                let right = self.plan_expression(*right)?;

                let mut out =
                    self.apply_casts_for_scalar_function(op.scalar_function(), vec![left, right])?;
                let [right, left] = [out.pop().unwrap(), out.pop().unwrap()];

                Ok(LogicalExpression::Binary {
                    op,
                    left: Box::new(left),
                    right: Box::new(right),
                })
            }
            ast::Expr::Function(func) => {
                let inputs = func
                    .args
                    .into_iter()
                    .map(|arg| match arg {
                        ast::FunctionArg::Unnamed { arg } => match arg {
                            ast::FunctionArgExpr::Expr(expr) => Ok(self.plan_expression(expr)?),
                            ast::FunctionArgExpr::Wildcard => {
                                // Binder should have handled removing '*' from
                                // function calls.
                                Err(RayexecError::new(
                                    "Cannot plan a function with '*' as an argument",
                                ))
                            }
                        },
                        ast::FunctionArg::Named { .. } => Err(RayexecError::new(
                            "Named arguments to scalar functions not supported",
                        )),
                    })
                    .collect::<Result<Vec<_>>>()?;

                match func.reference {
                    BoundFunctionReference::Scalar(scalar) => {
                        let inputs =
                            self.apply_casts_for_scalar_function(scalar.as_ref(), inputs)?;

                        Ok(LogicalExpression::ScalarFunction {
                            function: scalar,
                            inputs,
                        })
                    }
                    BoundFunctionReference::Aggregate(agg) => {
                        let inputs =
                            self.apply_casts_for_aggregate_function(agg.as_ref(), inputs)?;

                        Ok(LogicalExpression::Aggregate {
                            agg,
                            inputs,
                            filter: None,
                        })
                    }
                }
            }
            ast::Expr::Subquery(subquery) => {
                let mut nested = self.plan_context.nested(self.scope.clone());
                let subquery = nested.plan_query(*subquery)?;
                // We can ignore scope, as it's only relevant to planning of the
                // subquery, which is complete.
                Ok(LogicalExpression::Subquery(Box::new(subquery.root)))
            }
            ast::Expr::Exists {
                subquery,
                not_exists,
            } => {
                let mut nested = self.plan_context.nested(self.scope.clone());
                let subquery = nested.plan_query(*subquery)?;
                Ok(LogicalExpression::Exists {
                    not_exists,
                    subquery: Box::new(subquery.root),
                })
            }
            ast::Expr::Nested(expr) => self.plan_expression(*expr),
            ast::Expr::TypedString { datatype, value } => {
                let scalar = OwnedScalarValue::Utf8(value.into());
                // TODO: Add this back. Currently doing this to avoid having to
                // update cast rules for arrays and scalars at the same time.
                //
                // let scalar = cast_scalar(scalar, &datatype)?;
                Ok(LogicalExpression::Cast {
                    to: datatype,
                    expr: Box::new(LogicalExpression::Literal(scalar)),
                })
            }
            ast::Expr::Cast { datatype, expr } => {
                let expr = self.plan_expression(*expr)?;
                Ok(LogicalExpression::Cast {
                    to: datatype,
                    expr: Box::new(expr),
                })
            }
            ast::Expr::Interval(ast::Interval {
                value,
                leading,
                trailing,
            }) => {
                if leading.is_some() {
                    return Err(RayexecError::new(
                        "Leading unit in interval not yet supported",
                    ));
                }
                let expr = self.plan_expression(*value)?;

                match trailing {
                    Some(trailing) => {
                        // If a user provides a unit like `INTERVAL 3 YEARS`, we
                        // go ahead an multiply 3 with the a constant interval
                        // representing 1 YEAR.
                        //
                        // This builds on top of our existing casting/function
                        // dispatch rules. It's assumed that we have a
                        // `mul(interval, int64)` function (and similar).

                        let const_interval = match trailing {
                            ast::IntervalUnit::Year => Interval::new(12, 0, 0),
                            ast::IntervalUnit::Month => Interval::new(1, 0, 0),
                            ast::IntervalUnit::Week => Interval::new(0, 7, 0),
                            ast::IntervalUnit::Day => Interval::new(0, 1, 0),
                            ast::IntervalUnit::Hour => {
                                Interval::new(0, 0, Interval::NANOSECONDS_IN_HOUR)
                            }
                            ast::IntervalUnit::Minute => {
                                Interval::new(0, 0, Interval::NANOSECONDS_IN_MINUTE)
                            }
                            ast::IntervalUnit::Second => {
                                Interval::new(0, 0, Interval::NANOSECONDS_IN_SECOND)
                            }
                            ast::IntervalUnit::Millisecond => {
                                Interval::new(0, 0, Interval::NANOSECONDS_IN_MILLISECOND)
                            }
                            other => {
                                // TODO: Got lazy, add the rest.
                                return Err(RayexecError::new(format!(
                                    "Missing interval constant for {other:?}"
                                )));
                            }
                        };

                        Ok(LogicalExpression::Binary {
                            op: BinaryOperator::Multiply,
                            left: Box::new(LogicalExpression::Literal(ScalarValue::Interval(
                                const_interval,
                            ))),
                            right: Box::new(expr),
                        })
                    }
                    None => Ok(LogicalExpression::Cast {
                        to: DataType::Interval,
                        expr: Box::new(expr),
                    }),
                }
            }

            other => unimplemented!("{other:?}"),
        }
    }

    /// Plan a sql literal
    pub(crate) fn plan_literal(literal: ast::Literal<Bound>) -> Result<LogicalExpression> {
        Ok(match literal {
            ast::Literal::Number(n) => {
                if let Ok(n) = n.parse::<i64>() {
                    LogicalExpression::Literal(OwnedScalarValue::Int64(n))
                } else if let Ok(n) = n.parse::<u64>() {
                    LogicalExpression::Literal(OwnedScalarValue::UInt64(n))
                } else if let Ok(n) = n.parse::<f64>() {
                    LogicalExpression::Literal(OwnedScalarValue::Float64(n))
                } else {
                    return Err(RayexecError::new(format!(
                        "Unable to parse {n} as a number"
                    )));
                }
            }
            ast::Literal::Boolean(b) => LogicalExpression::Literal(OwnedScalarValue::Boolean(b)),
            ast::Literal::Null => LogicalExpression::Literal(OwnedScalarValue::Null),
            ast::Literal::SingleQuotedString(s) => {
                LogicalExpression::Literal(OwnedScalarValue::Utf8(s.to_string().into()))
            }
            other => {
                return Err(RayexecError::new(format!(
                    "Unusupported SQL literal: {other:?}"
                )))
            }
        })
    }

    /// Plan a single identifier.
    ///
    /// Assumed to be a column name either in the current scope or one of the
    /// outer scopes.
    fn plan_ident(&self, ident: ast::Ident) -> Result<LogicalExpression> {
        let val = ident.into_normalized_string();
        match self
            .scope
            .resolve_column(&self.plan_context.outer_scopes, None, &val)?
        {
            Some(col) => Ok(LogicalExpression::ColumnRef(col)),
            None => Err(RayexecError::new(format!(
                "Missing column for reference: {}",
                &val
            ))),
        }
    }

    /// Plan a compound identifier.
    ///
    /// Assumed to be a reference to a column either in the current scope or one
    /// of the outer scopes.
    fn plan_idents(&self, mut idents: Vec<ast::Ident>) -> Result<LogicalExpression> {
        fn format_err(table_ref: &TableReference, col: &str) -> String {
            format!("Missing column for reference: {table_ref}.{col}")
        }

        match idents.len() {
            0 => Err(RayexecError::new("Empty identifier")),
            1 => {
                // Single column.
                let ident = idents.pop().unwrap();
                self.plan_ident(ident)
            }
            2..=4 => {
                // Qualified column.
                // 2 => 'table.column'
                // 3 => 'schema.table.column'
                // 4 => 'database.schema.table.column'
                // TODO: Struct fields.
                let col = idents.pop().unwrap().into_normalized_string();
                let table_ref = TableReference {
                    table: idents
                        .pop()
                        .map(|ident| ident.into_normalized_string())
                        .unwrap(), // Must exist
                    schema: idents.pop().map(|ident| ident.into_normalized_string()), // May exist
                    database: idents.pop().map(|ident| ident.into_normalized_string()), // May exist
                };
                match self.scope.resolve_column(
                    &self.plan_context.outer_scopes,
                    Some(&table_ref),
                    &col,
                )? {
                    Some(col) => Ok(LogicalExpression::ColumnRef(col)),
                    None => Err(RayexecError::new(format_err(&table_ref, &col))), // Struct fields here.
                }
            }
            _ => Err(RayexecError::new(format!(
                "Too many identifier parts in {}",
                ast::ObjectReference(idents),
            ))), // TODO: Struct fields.
        }
    }

    /// Applies casts to an input expression based on the signatures for a
    /// scalar function.
    fn apply_casts_for_scalar_function(
        &self,
        scalar: &dyn GenericScalarFunction,
        inputs: Vec<LogicalExpression>,
    ) -> Result<Vec<LogicalExpression>> {
        let input_datatypes = inputs
            .iter()
            .map(|expr| expr.datatype(self.input, &[])) // TODO: Outer schemas
            .collect::<Result<Vec<_>>>()?;

        if scalar.return_type_for_inputs(&input_datatypes).is_some() {
            // Exact
            Ok(inputs)
        } else {
            // Try to find candidates that we can cast to.
            let mut candidates = scalar.candidate(&input_datatypes);

            if candidates.is_empty() {
                // TODO: Do we want to fall through? Is it possible for a
                // scalar and aggregate function to have the same name?

                // TODO: Better error.
                return Err(RayexecError::new(format!(
                    "Invalid inputs to '{}': {}",
                    scalar.name(),
                    input_datatypes.displayable(),
                )));
            }

            // TODO: Maybe more sophisticated candidate selection.
            //
            // TODO: Sort by score
            //
            // We should do some lightweight const folding and prefer candidates
            // that cast the consts over ones that need array inputs to be
            // casted.
            let candidate = candidates.swap_remove(0);

            // Apply casts where needed.
            let inputs = inputs
                .into_iter()
                .zip(candidate.casts)
                .map(|(input, cast_to)| {
                    Ok(match cast_to {
                        CastType::Cast { to, .. } => LogicalExpression::Cast {
                            to: DataType::try_default_datatype(to)?,
                            expr: Box::new(input),
                        },
                        CastType::NoCastNeeded => input,
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(inputs)
        }
    }

    // TODO: Reduce dupliation with the scalar one.
    fn apply_casts_for_aggregate_function(
        &self,
        agg: &dyn GenericAggregateFunction,
        inputs: Vec<LogicalExpression>,
    ) -> Result<Vec<LogicalExpression>> {
        let input_datatypes = inputs
            .iter()
            .map(|expr| expr.datatype(self.input, &[])) // TODO: Outer schemas
            .collect::<Result<Vec<_>>>()?;

        if agg.return_type_for_inputs(&input_datatypes).is_some() {
            // Exact
            Ok(inputs)
        } else {
            // Try to find candidates that we can cast to.
            let mut candidates = agg.candidate(&input_datatypes);

            if candidates.is_empty() {
                return Err(RayexecError::new(format!(
                    "Invalid inputs to '{}': {}",
                    agg.name(),
                    input_datatypes.displayable(),
                )));
            }

            // TODO: Maybe more sophisticated candidate selection.
            let candidate = candidates.swap_remove(0);

            // Apply casts where needed.
            let inputs = inputs
                .into_iter()
                .zip(candidate.casts)
                .map(|(input, cast_to)| {
                    Ok(match cast_to {
                        CastType::Cast { to, .. } => LogicalExpression::Cast {
                            to: DataType::try_default_datatype(to)?,
                            expr: Box::new(input),
                        },
                        CastType::NoCastNeeded => input,
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(inputs)
        }
    }
}
