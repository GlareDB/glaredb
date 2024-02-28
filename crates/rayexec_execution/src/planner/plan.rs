use crate::{
    functions::{self, table::TableFunctionArgs},
    planner::{
        operator::{ExpressionList, Filter, Scan, ScanItem},
        scope::TableReference,
    },
    types::batch::DataBatchSchema,
};

use super::{
    expr::ExpressionContext,
    operator::{LogicalExpression, LogicalOperator},
    scope::{ColumnRef, Scope, ScopeColumn},
    Resolver,
};
use rayexec_error::{RayexecError, Result};
use rayexec_parser::{ast, statement::Statement};

#[derive(Debug)]
pub struct LogicalQuery {
    /// Root of the query.
    pub root: LogicalOperator,

    /// The final scope of the query.
    pub scope: Scope,
}

#[derive(Debug, Clone)]
pub struct PlanContext<'a> {
    /// Resolver for resolving table and other table like items.
    pub resolver: &'a dyn Resolver,

    /// Scopes outside this context.
    pub outer_scopes: Vec<Scope>,
}

impl<'a> PlanContext<'a> {
    pub fn new(resolver: &'a dyn Resolver) -> Self {
        PlanContext {
            resolver,
            outer_scopes: Vec::new(),
        }
    }

    pub fn plan_statement(mut self, stmt: Statement) -> Result<LogicalQuery> {
        match stmt {
            Statement::Query(query) => self.plan_query(query),
            _ => unimplemented!(),
        }
    }

    fn plan_query(&mut self, query: ast::QueryNode) -> Result<LogicalQuery> {
        // TODO: CTEs

        let planned = match query.body {
            ast::QueryNodeBody::Select(select) => unimplemented!(),
            ast::QueryNodeBody::Set {
                left,
                right,
                operation,
            } => unimplemented!(),
            ast::QueryNodeBody::Values(values) => self.plan_values(values)?,
        };

        unimplemented!()
    }

    fn plan_select(&mut self, select: ast::SelectNode) -> Result<LogicalQuery> {
        // Handle FROM
        let mut plan = match select.from {
            Some(from) => self.plan_from_node(from)?,
            None => LogicalQuery {
                root: LogicalOperator::Empty,
                scope: Scope::empty(),
            },
        };

        // Handle WHERE
        if let Some(where_expr) = select.where_expr {
            let expr_ctx = ExpressionContext::new(self, &plan.scope, plan.root.schema()?);
            let expr = expr_ctx.plan_expression(where_expr)?;

            // Add filter to the plan, does not change the scope.
            plan.root = LogicalOperator::Filter(Filter {
                predicate: expr,
                input: Box::new(plan.root),
            });
        }

        // Expand projections.
        // TODO: Error on wildcards if no from.
        let expr_ctx = ExpressionContext::new(self, &plan.scope, plan.root.schema()?);
        let projections = select
            .projections
            .into_iter()
            .map(|item| expr_ctx.expand_select_expr(item))
            .collect::<Result<Vec<_>>>()?;

        // GROUP BY
        // Aggregates
        // HAVING

        // ORDER BY
        // DISTINCT

        unimplemented!()
    }

    fn plan_from_node(&self, from: ast::FromNode) -> Result<LogicalQuery> {
        match from.body {
            ast::FromNodeBody::BaseTable(_) => unimplemented!(),
            ast::FromNodeBody::Subquery(_) => unimplemented!(),
            ast::FromNodeBody::TableFunction(ast::FromTableFunction { reference, args }) => {
                let func = self.resolver.resolve_table_function(&reference)?;

                // Plan the arguments to the table function. Currently only
                // constant expressions are allowed.
                let expr_ctx =
                    ExpressionContext::new(self, &Scope::empty(), &DataBatchSchema::empty());
                let mut func_args = TableFunctionArgs::default();
                for arg in args {
                    match arg {
                        ast::FunctionArg::Named { name, arg } => {
                            match expr_ctx.plan_expression(arg)? {
                                LogicalExpression::Literal(v) => {
                                    func_args.named.insert(name.value, v);
                                }
                                other => {
                                    return Err(RayexecError::new(format!(
                                        "Argument to table funtion is not a constant: {other:?}"
                                    )))
                                }
                            }
                        }
                        ast::FunctionArg::Unnamed { arg } => {
                            match expr_ctx.plan_expression(arg)? {
                                LogicalExpression::Literal(v) => func_args.unnamed.push(v),
                                other => {
                                    return Err(RayexecError::new(format!(
                                        "Argument to table funtion is not a constant: {other:?}"
                                    )))
                                }
                            }
                        }
                    }
                }

                let name = func.name();
                let bound = func.bind(func_args)?; // The only thing that would benefit from async.
                let schema = bound.schema();
                let (orig_names, types) = schema.into_names_and_types();
                let schema = DataBatchSchema::new(types);

                let (reference, col_names) = match from.alias {
                    Some(ast::FromAlias { alias, columns }) => {
                        let reference = TableReference {
                            database: None,
                            schema: None,
                            table: alias.value,
                        };
                        let col_names = match columns {
                            Some(columns) => {
                                if columns.len() > orig_names.len() {
                                    return Err(RayexecError::new(format!(
                                        "Specified {} column aliases when only {} columns exist",
                                        columns.len(),
                                        orig_names.len()
                                    )));
                                }

                                // If user specifies less aliases than columns,
                                // extend out the aliases with the original
                                // names.
                                let mut aliases: Vec<_> =
                                    columns.into_iter().map(|ident| ident.value).collect();
                                if aliases.len() < orig_names.len() {
                                    aliases.extend_from_slice(&orig_names[aliases.len()..]);
                                }
                                aliases
                            }
                            None => orig_names,
                        };
                        (reference, col_names)
                    }
                    // TODO: We'll probably want to fully qualify this.
                    None => (
                        TableReference {
                            database: None,
                            schema: None,
                            table: name.to_string(),
                        },
                        orig_names,
                    ),
                };

                let scope = Scope::with_columns(Some(reference), col_names);
                let operator = LogicalOperator::Scan(Scan {
                    source: ScanItem::TableFunction(bound),
                    schema,
                });

                Ok(LogicalQuery {
                    root: operator,
                    scope,
                })
            }
            ast::FromNodeBody::Join(_) => unimplemented!(),
        }
    }

    fn plan_values(&self, values: ast::Values) -> Result<LogicalQuery> {
        if values.rows.is_empty() {
            return Err(RayexecError::new("Empty VALUES expression"));
        }

        // Convert AST expressions to logical expressions.
        let expr_ctx = ExpressionContext::new(self, &Scope::empty(), &DataBatchSchema::empty());
        let exprs = values
            .rows
            .into_iter()
            .map(|col_vals| {
                col_vals
                    .into_iter()
                    .map(|col_expr| expr_ctx.plan_expression(col_expr))
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<Vec<LogicalExpression>>>>()?;

        let operator = LogicalOperator::ExpressionList(ExpressionList { rows: exprs });

        // Generate output scope with appropriate column names.
        let num_cols = exprs[0].len();
        let mut scope = Scope::empty();
        scope.add_columns(None, (0..num_cols).map(|i| format!("column{}", i + 1)));

        Ok(LogicalQuery {
            root: operator,
            scope,
        })
    }
}
