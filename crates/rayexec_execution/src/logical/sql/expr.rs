use rayexec_bullet::{field::TypeSchema, scalar::OwnedScalarValue};
use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

use crate::functions::scalar::GenericScalarFunction;
use crate::logical::operator::LogicalExpression;

use super::{
    binder::{Bound, BoundFunctionReference},
    planner::PlanContext,
    scope::{Scope, TableReference},
};

/// An expanded select expression.
// TODO: What does the below TODO mean?
// TODO: Expand wildcard.
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
            ast::SelectExpr::Expr(expr) => vec![ExpandedSelectExpr::Expr {
                expr,
                name: "?column?".to_string(),
            }],
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
            ast::Expr::Literal(literal) => self.plan_literal(literal),
            ast::Expr::BinaryExpr { left, op, right } => Ok(LogicalExpression::Binary {
                op: op.try_into()?,
                left: Box::new(self.plan_expression(*left)?),
                right: Box::new(self.plan_expression(*right)?),
            }),
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
                        if !self.scalar_function_can_handle_input(scalar.as_ref(), &inputs)? {
                            // TODO: Do we want to fall through? Is it possible for a
                            // scalar and aggregate function to have the same name?

                            return Err(RayexecError::new(format!(
                                "Invalid inputs to '{}'",
                                scalar.name(),
                            )));
                        }
                        Ok(LogicalExpression::ScalarFunction {
                            function: scalar,
                            inputs,
                        })
                    }
                    BoundFunctionReference::Aggregate(agg) => {
                        // TODO: Sig check

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

            _ => unimplemented!(),
        }
    }

    /// Plan a sql literal
    fn plan_literal(&self, literal: ast::Literal<Bound>) -> Result<LogicalExpression> {
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

    /// Check if a scalar function is able to handle the given inputs.
    ///
    /// Errors if the datatypes for the inputs cannot be determined.
    fn scalar_function_can_handle_input(
        &self,
        function: &dyn GenericScalarFunction,
        inputs: &[LogicalExpression],
    ) -> Result<bool> {
        let inputs = inputs
            .iter()
            .map(|expr| expr.datatype(self.input, &[])) // TODO: Outer schemas
            .collect::<Result<Vec<_>>>()?;

        if function.return_type_for_inputs(&inputs).is_some() {
            return Ok(true);
        }

        Ok(false)
    }
}
