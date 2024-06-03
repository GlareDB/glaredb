use super::{
    expr::{ExpandedSelectExpr, ExpressionContext},
    operator::{
        Aggregate, AnyJoin, CreateTable, CrossJoin, DropEntry, GroupingExpr, Insert, Limit,
        LogicalExpression, LogicalOperator, Order, OrderByExpr, Projection, Scan,
    },
    scope::{ColumnRef, Scope},
};
use crate::{
    database::{
        catalog::CatalogTx,
        create::OnConflict,
        drop::{DropInfo, DropObject},
        entry::TableEntry,
        DatabaseContext,
    },
    engine::vars::SessionVars,
    planner::{
        operator::{
            CreateSchema, Explain, ExplainFormat, ExpressionList, Filter, JoinType, ResetVar,
            SetVar, ShowVar, VariableOrAll,
        },
        scope::TableReference,
    },
};
use rayexec_bullet::field::{DataType, Field, TypeSchema};
use rayexec_error::{RayexecError, Result};
use rayexec_parser::{
    ast::{self, OrderByNulls, OrderByType},
    statement::Statement,
};
use tracing::trace;

const EMPTY_SCOPE: &Scope = &Scope::empty();
const EMPTY_TYPE_SCHEMA: &TypeSchema = &TypeSchema::empty();

#[derive(Debug)]
pub struct LogicalQuery {
    /// Root of the query.
    pub root: LogicalOperator,

    /// The final scope of the query.
    pub scope: Scope,
}

#[derive(Debug, Clone)]
pub struct PlanContext<'a> {
    pub tx: &'a CatalogTx,

    /// Resolver for resolving table and other table like items.
    pub resolver: &'a DatabaseContext,

    /// Session variables.
    pub vars: &'a SessionVars,

    /// Scopes outside this context.
    pub outer_scopes: Vec<Scope>,
}

impl<'a> PlanContext<'a> {
    pub fn new(tx: &'a CatalogTx, resolver: &'a DatabaseContext, vars: &'a SessionVars) -> Self {
        PlanContext {
            tx,
            resolver,
            vars,
            outer_scopes: Vec::new(),
        }
    }

    pub fn plan_statement(mut self, stmt: Statement) -> Result<LogicalQuery> {
        trace!("planning statement");
        match stmt {
            Statement::Explain(explain) => {
                let plan = match explain.body {
                    ast::ExplainBody::Query(query) => self.plan_query(query)?,
                };
                let format = match explain.output {
                    Some(ast::ExplainOutput::Text) => ExplainFormat::Text,
                    Some(ast::ExplainOutput::Json) => ExplainFormat::Json,
                    None => ExplainFormat::Text,
                };
                Ok(LogicalQuery {
                    root: LogicalOperator::Explain(Explain {
                        analyze: explain.analyze,
                        verbose: explain.verbose,
                        format,
                        input: Box::new(plan.root),
                    }),
                    scope: Scope::empty(),
                })
            }
            Statement::Query(query) => self.plan_query(query),
            Statement::CreateTable(create) => self.plan_create_table(create),
            Statement::CreateSchema(create) => self.plan_create_schema(create),
            Statement::Drop(drop) => self.plan_drop(drop),
            Statement::Insert(insert) => self.plan_insert(insert),
            Statement::SetVariable(ast::SetVariable { reference, value }) => {
                let expr_ctx = ExpressionContext::new(&self, EMPTY_SCOPE, EMPTY_TYPE_SCHEMA);
                let expr = expr_ctx.plan_expression(value)?;
                Ok(LogicalQuery {
                    root: LogicalOperator::SetVar(SetVar {
                        name: reference.0[0].as_normalized_string(), // TODO: Allow compound references?
                        value: expr.try_into_scalar()?,
                    }),
                    scope: Scope::empty(),
                })
            }
            Statement::ShowVariable(ast::ShowVariable { reference }) => {
                let name = reference.0[0].as_normalized_string(); // TODO: Allow compound references?
                let var = self.vars.get_var(&name)?;
                let scope = Scope::with_columns(None, [name.clone()]);
                Ok(LogicalQuery {
                    root: LogicalOperator::ShowVar(ShowVar { var: var.clone() }),
                    scope,
                })
            }
            Statement::ResetVariable(ast::ResetVariable { var }) => {
                let var = match var {
                    ast::VariableOrAll::Variable(v) => {
                        let name = v.0[0].as_normalized_string(); // TODO: Allow compound references?
                        let var = self.vars.get_var(&name)?;
                        VariableOrAll::Variable(var.clone())
                    }
                    ast::VariableOrAll::All => VariableOrAll::All,
                };
                Ok(LogicalQuery {
                    root: LogicalOperator::ResetVar(ResetVar { var }),
                    scope: Scope::empty(),
                })
            }
        }
    }

    /// Create a new nested plan context for planning subqueries.
    fn nested(&self, outer: Scope) -> Self {
        PlanContext {
            tx: self.tx,
            resolver: self.resolver,
            vars: self.vars,
            outer_scopes: std::iter::once(outer)
                .chain(self.outer_scopes.clone())
                .collect(),
        }
    }

    fn plan_insert(&mut self, insert: ast::Insert) -> Result<LogicalQuery> {
        let (_reference, ent) = self.resolve_table(insert.table)?;

        let source = self.plan_query(insert.source)?;

        // TODO: Handle specified columns. If provided, insert a projection that
        // maps the columns to the right position.

        Ok(LogicalQuery {
            root: LogicalOperator::Insert(Insert {
                table: ent,
                input: Box::new(source.root),
            }),
            scope: Scope::empty(),
        })
    }

    fn plan_drop(&mut self, drop: ast::DropStatement) -> Result<LogicalQuery> {
        match drop.drop_type {
            ast::DropType::Schema => {
                // TODO: Get 'default' catalog.
                if drop.name.0.len() != 2 {
                    return Err(RayexecError::new(
                        "Only qualified schemas can be dropped right now",
                    ));
                }

                let catalog = drop.name.0[0].as_normalized_string();
                let schema = drop.name.0[1].as_normalized_string();
                // Dropping defaults to restricting (erroring) on dependencies.
                let deps = drop.deps.unwrap_or(ast::DropDependents::Restrict);

                let plan = LogicalOperator::Drop(DropEntry {
                    info: DropInfo {
                        catalog,
                        schema,
                        object: DropObject::Schema,
                        cascade: ast::DropDependents::Cascade == deps,
                        if_exists: drop.if_exists,
                    },
                });

                Ok(LogicalQuery {
                    root: plan,
                    scope: Scope::empty(),
                })
            }
            _other => unimplemented!(),
        }
    }

    fn plan_create_schema(&mut self, create: ast::CreateSchema) -> Result<LogicalQuery> {
        let on_conflict = if create.if_not_exists {
            OnConflict::Ignore
        } else {
            OnConflict::Error
        };

        // TODO: Get 'default' catalog
        if create.name.0.len() != 2 {
            return Err(RayexecError::new(
                "Only qualified schemas can be create right now",
            ));
        }

        let catalog = create.name.0[0].as_normalized_string();
        if !self.resolver.catalog_exists(&catalog) {
            return Err(RayexecError::new(format!("Missing catalog: {catalog}")));
        }

        let name = create.name.0[1].as_normalized_string();

        Ok(LogicalQuery {
            root: LogicalOperator::CreateSchema(CreateSchema {
                catalog,
                name,
                on_conflict,
            }),
            scope: Scope::empty(),
        })
    }

    fn plan_create_table(&mut self, create: ast::CreateTable) -> Result<LogicalQuery> {
        let on_conflict = match (create.or_replace, create.if_not_exists) {
            (true, false) => OnConflict::Replace,
            (false, true) => OnConflict::Ignore,
            (false, false) => OnConflict::Error,
            (true, true) => {
                return Err(RayexecError::new(
                    "Cannot specify both OR REPLACE and IF NOT EXISTS",
                ))
            }
        };

        // TODO: Better name handling.
        // TODO: Get schema from name or search path.
        let name = create.name.0[0].as_normalized_string();

        // TODO: Constraints.
        let mut columns: Vec<_> = create
            .columns
            .into_iter()
            .map(|col| {
                Field::new(
                    col.name.to_string(),
                    Self::ast_datatype_to_exec_datatype(col.datatype),
                    true,
                )
            })
            .collect();

        let input = match create.source {
            Some(source) => {
                // If we have an input to the table, adjust the column definitions for the table
                // to be the output schema of the input.

                // TODO: We could allow this though. We'd just need to do some
                // projections as necessary.
                if !columns.is_empty() {
                    return Err(RayexecError::new(
                        "Cannot specify columns when running CREATE TABLE ... AS ...",
                    ));
                }

                let input = self.plan_query(source)?;
                let type_schema = input.root.output_schema(&[])?; // Source input to table should not depend on any outer queries.

                if type_schema.types.len() != input.scope.items.len() {
                    // An "us" bug. These should be the same lengths.
                    return Err(RayexecError::new(
                        "Output scope and type schemas differ in lengths",
                    ));
                }

                let fields: Vec<_> = input
                    .scope
                    .items
                    .iter()
                    .zip(type_schema.types)
                    .map(|(item, typ)| Field::new(&item.column, typ, true))
                    .collect();

                // Update columns to the fields we've generated from the input.
                columns = fields;

                Some(Box::new(input.root))
            }
            None => None,
        };

        Ok(LogicalQuery {
            root: LogicalOperator::CreateTable(CreateTable {
                name,
                temp: create.temp,
                columns,
                on_conflict,
                input,
            }),
            scope: Scope::empty(),
        })
    }

    fn plan_query(&mut self, query: ast::QueryNode) -> Result<LogicalQuery> {
        // TODO: CTEs

        let mut planned = match query.body {
            ast::QueryNodeBody::Select(select) => self.plan_select(*select, query.order_by)?,

            ast::QueryNodeBody::Set {
                left: _,
                right: _,
                operation: _,
            } => unimplemented!(),
            ast::QueryNodeBody::Values(values) => self.plan_values(values)?,
        };

        // Handle LIMIT/OFFSET
        let expr_ctx = ExpressionContext::new(self, EMPTY_SCOPE, EMPTY_TYPE_SCHEMA);
        if let Some(limit_expr) = query.limit.limit {
            let expr = expr_ctx.plan_expression(limit_expr)?;
            let limit = expr.try_into_scalar()?.try_as_i64()? as usize;

            let offset = match query.limit.offset {
                Some(offset_expr) => {
                    let expr = expr_ctx.plan_expression(offset_expr)?;
                    let offset = expr.try_into_scalar()?.try_as_i64()?;
                    Some(offset as usize)
                }
                None => None,
            };

            // Update plan, does not change scope.
            planned.root = LogicalOperator::Limit(Limit {
                offset,
                limit,
                input: Box::new(planned.root),
            });
        }

        Ok(planned)
    }

    fn plan_select(
        &mut self,
        select: ast::SelectNode,
        order_by: Vec<ast::OrderByNode>,
    ) -> Result<LogicalQuery> {
        // Handle FROM
        let mut plan = match select.from {
            Some(from) => self.plan_from_node(from, Scope::empty())?,
            None => LogicalQuery {
                root: LogicalOperator::Empty,
                scope: Scope::empty(),
            },
        };

        let from_type_schema = plan.root.output_schema(&[])?;

        // Handle WHERE
        if let Some(where_expr) = select.where_expr {
            let expr_ctx = ExpressionContext::new(self, &plan.scope, &from_type_schema);
            let expr = expr_ctx.plan_expression(where_expr)?;

            // Add filter to the plan, does not change the scope.
            plan.root = LogicalOperator::Filter(Filter {
                predicate: expr,
                input: Box::new(plan.root),
            });
        }

        // Expand projections.
        // TODO: Error on wildcards if no from.
        let expr_ctx = ExpressionContext::new(self, &plan.scope, &from_type_schema);
        let mut projections = Vec::new();
        for select_proj in select.projections {
            let mut expanded = expr_ctx.expand_select_expr(select_proj)?;
            projections.append(&mut expanded);
        }

        // TODO:
        // - HAVING

        // Add projections to plan using previously expanded select items.
        let mut select_exprs = Vec::with_capacity(projections.len());
        let mut names = Vec::with_capacity(projections.len());
        let expr_ctx = ExpressionContext::new(self, &plan.scope, &from_type_schema);
        for proj in projections {
            match proj {
                ExpandedSelectExpr::Expr { expr, name } => {
                    let expr = expr_ctx.plan_expression(expr)?;
                    select_exprs.push(expr);
                    names.push(name);
                }
                ExpandedSelectExpr::Column { idx, name } => {
                    let expr = LogicalExpression::ColumnRef(ColumnRef {
                        scope_level: 0,
                        item_idx: idx,
                    });
                    select_exprs.push(expr);
                    names.push(name);
                }
            }
        }

        // Plan and append HAVING and ORDER BY expressions.
        //
        // This may result in new expressions that need to be added to the
        // select expressions. However, this should not modify the final query
        // projection.
        let mut num_appended = 0;
        let having_expr = match select.having {
            Some(expr) => {
                let mut expr = expr_ctx.plan_expression(expr)?;
                num_appended += Self::append_hidden(&mut select_exprs, &mut expr)?;
                Some(expr)
            }
            None => None,
        };

        let mut order_by_exprs = order_by
            .into_iter()
            .map(|order_by| {
                let expr = expr_ctx.plan_expression(order_by.expr)?;
                Ok(OrderByExpr {
                    expr,
                    desc: matches!(order_by.typ.unwrap_or(OrderByType::Asc), OrderByType::Desc),
                    nulls_first: matches!(
                        order_by.nulls.unwrap_or(OrderByNulls::First),
                        OrderByNulls::First
                    ),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        num_appended += Self::append_order_by_exprs(&mut select_exprs, &mut order_by_exprs)?;

        // Convert select expressions into (expression, modified) pairs. This
        // let's us skip trying to modify expressions in
        // `extract_group_by_exprs` if it's been modified by
        // `extract_aggregates`.
        let mut select_exprs: Vec<(LogicalExpression, bool)> =
            select_exprs.into_iter().map(|e| (e, false)).collect();

        // GROUP BY
        let aggregates = Self::extract_aggregates(&mut select_exprs)?;
        let grouping_expr = match select.group_by {
            Some(group_by) => match group_by {
                ast::GroupByNode::All => unimplemented!(),
                ast::GroupByNode::Exprs { mut exprs } => {
                    let expr = match exprs.len() {
                        1 => exprs.pop().unwrap(),
                        _ => {
                            return Err(RayexecError::new("Invalid number of group by expressions"))
                        }
                    };

                    // Group by has access to everything we've planned so far.
                    let expr_ctx = ExpressionContext::new(self, &plan.scope, &from_type_schema);

                    let plan_exprs = |exprs: Vec<ast::Expr>| {
                        exprs
                            .into_iter()
                            .map(|e| expr_ctx.plan_expression(e))
                            .collect::<Result<Vec<_>>>()
                    };

                    match expr {
                        ast::GroupByExpr::Expr(exprs) => {
                            let exprs = plan_exprs(exprs)?;
                            Some(GroupingExpr::GroupBy(Self::extract_group_by_exprs(
                                &mut select_exprs,
                                exprs,
                                aggregates.len(),
                            )?))
                        }
                        ast::GroupByExpr::Rollup(exprs) => {
                            let exprs = plan_exprs(exprs)?;
                            Some(GroupingExpr::Rollup(Self::extract_group_by_exprs(
                                &mut select_exprs,
                                exprs,
                                aggregates.len(),
                            )?))
                        }
                        ast::GroupByExpr::Cube(exprs) => {
                            let exprs = plan_exprs(exprs)?;
                            Some(GroupingExpr::Cube(Self::extract_group_by_exprs(
                                &mut select_exprs,
                                exprs,
                                aggregates.len(),
                            )?))
                        }
                        ast::GroupByExpr::GroupingSets(_) => unimplemented!("grouping sets"),
                    }
                }
            },
            None => None,
        };

        if !aggregates.is_empty() || grouping_expr.is_some() {
            plan = Self::build_aggregate(aggregates, grouping_expr, plan)?;
        }

        // Project the full select list.
        let select_exprs: Vec<_> = select_exprs.into_iter().map(|(expr, _)| expr).collect();
        plan = LogicalQuery {
            root: LogicalOperator::Projection(Projection {
                exprs: select_exprs.clone(),
                input: Box::new(plan.root),
            }),
            scope: plan.scope,
        };

        // Add filter for HAVING.
        if let Some(expr) = having_expr {
            plan = LogicalQuery {
                root: LogicalOperator::Filter(Filter {
                    predicate: expr,
                    input: Box::new(plan.root),
                }),
                scope: plan.scope,
            }
        }

        // Add order by node.
        if !order_by_exprs.is_empty() {
            plan = LogicalQuery {
                root: LogicalOperator::Order(Order {
                    exprs: order_by_exprs,
                    input: Box::new(plan.root),
                }),
                scope: plan.scope,
            }
        }

        // Turn select expressions back into only the expressions for the
        // output.
        if num_appended > 0 {
            let output_len = select_exprs.len() - num_appended;

            let projections = (0..output_len).map(LogicalExpression::new_column).collect();

            plan = LogicalQuery {
                root: LogicalOperator::Projection(Projection {
                    exprs: projections,
                    input: Box::new(plan.root),
                }),
                scope: plan.scope,
            };
        }

        // Cleaned scope containing only output columns in the final output.
        plan.scope = Scope::with_columns(None, names);

        Ok(plan)
    }

    fn plan_from_node(&self, from: ast::FromNode, current_scope: Scope) -> Result<LogicalQuery> {
        // Plan the "body" of the FROM.
        let body = match from.body {
            ast::FromNodeBody::BaseTable(ast::FromBaseTable { reference }) => {
                let (reference, ent) = self.resolve_table(reference)?;
                let scope = Scope::with_columns(
                    Some(reference),
                    ent.columns.iter().map(|f| f.name.clone()),
                );

                // TODO: We need a "resolved" entry type that wraps a table
                // entry telling us which catalog/schema it's from.
                LogicalQuery {
                    root: LogicalOperator::Scan(Scan {
                        catalog: "temp".to_string(),
                        schema: "temp".to_string(),
                        source: ent,
                    }),
                    scope,
                }
            }
            ast::FromNodeBody::Subquery(ast::FromSubquery { query }) => {
                let mut nested = self.nested(current_scope);
                nested.plan_query(query)?
            }
            ast::FromNodeBody::TableFunction(ast::FromTableFunction {
                reference: _,
                args: _,
            }) => {
                // 1. Resolve table func
                // 2. Specialize based on args.
                // 3. Load schema?

                unimplemented!()
            }
            ast::FromNodeBody::Join(ast::FromJoin {
                left,
                right,
                join_type,
                join_condition,
            }) => {
                // Plan left side of join.
                let left_nested = self.nested(current_scope.clone());
                let left_plan = left_nested.plan_from_node(*left, Scope::empty())?; // TODO: Determine if should be empty.

                // Plan right side of join.
                //
                // Note this uses a plan context that has the "left" scope as
                // its outer scope.
                let right_nested = left_nested.nested(left_plan.scope.clone());
                let right_plan = right_nested.plan_from_node(*right, Scope::empty())?; // TODO: Determine if this should be empty.

                match join_condition {
                    ast::JoinCondition::On(on) => {
                        let merged = left_plan.scope.merge(right_plan.scope)?;
                        let left_schema = left_plan.root.output_schema(&[])?; // TODO: Outers
                        let right_schema = right_plan.root.output_schema(&[])?; // TODO: Outers
                        let merged_schema = left_schema.merge(right_schema);
                        let expr_ctx =
                            ExpressionContext::new(&left_nested, &merged, &merged_schema);

                        let on_expr = expr_ctx.plan_expression(on)?;

                        let join_type = match join_type {
                            ast::JoinType::Inner => JoinType::Inner,
                            ast::JoinType::Left => JoinType::Left,
                            ast::JoinType::Right => JoinType::Right,
                            ast::JoinType::Cross => {
                                unreachable!("Cross join should not have a join condition")
                            }
                            _ => unimplemented!(),
                        };

                        LogicalQuery {
                            root: LogicalOperator::AnyJoin(AnyJoin {
                                left: Box::new(left_plan.root),
                                right: Box::new(right_plan.root),
                                join_type,
                                on: on_expr,
                            }),
                            scope: merged,
                        }
                    }
                    ast::JoinCondition::None => match join_type {
                        ast::JoinType::Cross => {
                            let merged = left_plan.scope.merge(right_plan.scope)?;
                            LogicalQuery {
                                root: LogicalOperator::CrossJoin(CrossJoin {
                                    left: Box::new(left_plan.root),
                                    right: Box::new(right_plan.root),
                                }),
                                scope: merged,
                            }
                        }
                        _other => return Err(RayexecError::new("Missing join condition for join")),
                    },
                    _ => unimplemented!(),
                }
            }
        };

        // Apply aliases if provided.
        let aliased_scope = Self::apply_alias(body.scope, from.alias)?;

        Ok(LogicalQuery {
            root: body.root,
            scope: aliased_scope,
        })
    }

    /// Apply table and column aliases to a scope.
    fn apply_alias(mut scope: Scope, alias: Option<ast::FromAlias>) -> Result<Scope> {
        Ok(match alias {
            Some(ast::FromAlias { alias, columns }) => {
                let reference = TableReference {
                    database: None,
                    schema: None,
                    table: alias.into_normalized_string(),
                };

                // Modify all items in the scope to now have the new table
                // alias.
                for item in scope.items.iter_mut() {
                    // TODO: Make sure that it's correct to apply this to
                    // everything in the scope.
                    item.alias = Some(reference.clone());
                }

                // If column aliases are provided as well, apply those to the
                // columns in the scope.
                //
                // Note that if the user supplies less aliases than there are
                // columns in the scope, then the remaining columns will retain
                // their original names.
                if let Some(columns) = columns {
                    if columns.len() > scope.items.len() {
                        return Err(RayexecError::new(format!(
                            "Specified {} column aliases when only {} columns exist",
                            columns.len(),
                            scope.items.len()
                        )));
                    }

                    for (item, new_alias) in scope.items.iter_mut().zip(columns.into_iter()) {
                        item.column = new_alias.into_normalized_string();
                    }
                }

                scope
            }
            None => scope,
        })
    }

    fn plan_values(&self, values: ast::Values) -> Result<LogicalQuery> {
        if values.rows.is_empty() {
            return Err(RayexecError::new("Empty VALUES expression"));
        }

        // Convert AST expressions to logical expressions.
        let expr_ctx = ExpressionContext::new(self, EMPTY_SCOPE, EMPTY_TYPE_SCHEMA);
        let num_cols = values.rows[0].len();
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
        let mut scope = Scope::empty();
        scope.add_columns(None, (0..num_cols).map(|i| format!("column{}", i + 1)));

        Ok(LogicalQuery {
            root: operator,
            scope,
        })
    }

    /// Builds an aggregate node in the plan.
    ///
    /// Inputs to the aggregation will be placed in a pre-projection.
    fn build_aggregate(
        mut agg_exprs: Vec<LogicalExpression>,
        mut grouping_expr: Option<GroupingExpr>,
        current: LogicalQuery,
    ) -> Result<LogicalQuery> {
        let mut projections = Vec::with_capacity(agg_exprs.len());
        for agg_expr in agg_exprs.iter_mut() {
            match agg_expr {
                LogicalExpression::Aggregate { inputs, .. } => {
                    let idx = projections.len();
                    let new_inputs = (0..inputs.len())
                        .map(|i| LogicalExpression::new_column(idx + i))
                        .collect();
                    let mut old = std::mem::replace(inputs, new_inputs);
                    projections.append(&mut old);
                }
                other => {
                    return Err(RayexecError::new(format!(
                        "Unexpected logical expression: {other:?}"
                    )))
                }
            }
        }

        if let Some(grouping_expr) = &mut grouping_expr {
            for expr in grouping_expr.expressions_mut() {
                let idx = projections.len();
                let old = std::mem::replace(expr, LogicalExpression::new_column(idx));
                projections.push(old);
            }
        }

        let projection = LogicalOperator::Projection(Projection {
            exprs: projections,
            input: Box::new(current.root),
        });

        Ok(LogicalQuery {
            root: LogicalOperator::Aggregate(Aggregate {
                exprs: agg_exprs,
                grouping_expr,
                input: Box::new(projection),
            }),
            scope: current.scope,
        })
    }

    /// Extract aggregates functions from the logical select list, returning them.
    ///
    /// This will replace the aggregate expression in `exprs` with a column
    /// reference that points to column position in the returned aggregate list.
    fn extract_aggregates(
        exprs: &mut [(LogicalExpression, bool)],
    ) -> Result<Vec<LogicalExpression>> {
        let mut aggs = Vec::new();
        for (expr, modified) in exprs {
            let mut in_aggregate = false;
            expr.walk_mut_pre(&mut |expr| {
                if expr.is_aggregate() {
                    if in_aggregate {
                        return Err(RayexecError::new("Cannot nest aggregate functions"));
                    }
                    in_aggregate = true;

                    let column_ref = LogicalExpression::new_column(aggs.len());
                    let agg = std::mem::replace(expr, column_ref);
                    *modified = true;
                    aggs.push(agg);
                }
                Ok(())
            })?;
        }

        Ok(aggs)
    }

    /// Extract group by expressions from both the select list, and the
    /// expressions in the GROUP BY clause.
    ///
    /// When a select expression matches exactly one of the group by
    /// expressions, it will be updated with a column reference pointing to that
    /// expression.
    fn extract_group_by_exprs(
        select_exprs: &mut [(LogicalExpression, bool)],
        group_by_exprs: Vec<LogicalExpression>,
        offset: usize,
    ) -> Result<Vec<LogicalExpression>> {
        let mut groups = Vec::new();
        for group_by_expr in group_by_exprs {
            // TODO: Select aliases
            //
            // This is valid:
            // SELECT c1 / 100 AS my_alias, SUM(c2) FROM t1 GROUP BY my_alias

            // Extract expressions from the select that equal our group by
            // expression, and replace with a column reference.
            let mut group_added = false;
            for (expr, modified) in select_exprs.iter_mut() {
                // If this select expression was previously modified, skip it
                // since it should already be pointing to the right thing.
                if *modified {
                    continue;
                }

                if expr == &group_by_expr {
                    if !group_added {
                        let column_ref = LogicalExpression::new_column(groups.len() + offset);
                        let group = std::mem::replace(expr, column_ref);
                        groups.push(group);
                        group_added = true;
                    } else {
                        // We're already tracking the group. Just find the
                        // existing index, and replace the expression.
                        let group_idx = groups.iter().position(|g| g == expr).unwrap();
                        let column_ref = LogicalExpression::new_column(group_idx + offset);
                        *expr = column_ref;
                    }

                    *modified = true;
                }
            }

            if !group_added {
                // Group not referenced in select. Go ahead and add it.
                groups.push(group_by_expr);
            }
        }

        Ok(groups)
    }

    /// Appends new expressions that need to be added to the plan for order by
    /// expressions. The order by expression will be rewritten to point to the
    /// new expression, or to an expression already in the select exprs.
    fn append_order_by_exprs(
        select_exprs: &mut Vec<LogicalExpression>,
        order_by_exprs: &mut [OrderByExpr],
    ) -> Result<usize> {
        let mut num_appended = 0;
        for order_by_expr in order_by_exprs.iter_mut() {
            num_appended += Self::append_hidden(select_exprs, &mut order_by_expr.expr)?;
        }

        Ok(num_appended)
    }

    /// Appends hidden columns (columns that aren't referenced in the outer
    /// plan's scope) to the select expressions if the provided expression is
    /// not already included in the select expressions.
    ///
    /// This is needed for the pre-projection into ORDER BY and HAVING, where
    /// the expressions in those clauses may not actually exist in the output.
    ///
    /// Returns the number of appended expressions.
    fn append_hidden(
        select_exprs: &mut Vec<LogicalExpression>,
        expr: &mut LogicalExpression,
    ) -> Result<usize> {
        // Check to see if our expression matches anything already in the select
        // list. If it does, replace our expression with a reference to it.
        for (select_idx, select_expr) in select_exprs.iter().enumerate() {
            if expr == select_expr {
                *expr = LogicalExpression::new_column(select_idx);
                return Ok(0);
            }
        }

        // TODO: Check if `expr` is already a column ref pointing to a select
        // expr.

        // Otherwise need to put the expression into the select list, and
        // replace it with a reference.
        let col_ref = LogicalExpression::new_column(select_exprs.len());
        let orig = std::mem::replace(expr, col_ref);

        select_exprs.push(orig);

        Ok(1)
    }

    fn resolve_table(
        &self,
        reference: ast::ObjectReference,
    ) -> Result<(TableReference, TableEntry)> {
        // TODO: Better handling, also search path.
        let name = &reference.0[0].as_normalized_string();

        // Search temp first
        if let Some(ent) = self
            .resolver
            .get_catalog("temp")?
            .get_table_entry(self.tx, "temp", name)?
        {
            let reference = TableReference {
                database: None,
                schema: None,
                table: name.clone(),
            };

            Ok((reference, ent))
        } else {
            // Search other catalogs/schemas in the search path (once we
            // have them).

            Err(RayexecError::new(format!(
                "Unable to find entry for '{name}'"
            )))
        }
    }

    fn ast_datatype_to_exec_datatype(datatype: ast::DataType) -> DataType {
        match datatype {
            ast::DataType::Varchar(_) => DataType::Utf8,
            ast::DataType::SmallInt => DataType::Int16,
            ast::DataType::Integer => DataType::Int32,
            ast::DataType::BigInt => DataType::Int64,
            ast::DataType::Real => DataType::Float32,
            ast::DataType::Double => DataType::Float64,
            ast::DataType::Bool => DataType::Boolean,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::functions::aggregate::numeric::Sum;

    use super::*;

    #[test]
    fn extract_aggregates_only_aggregate() {
        let mut selects = vec![(
            LogicalExpression::Aggregate {
                agg: Box::new(Sum),
                inputs: vec![LogicalExpression::new_column(0)],
                filter: None,
            },
            false,
        )];

        let out = PlanContext::extract_aggregates(&mut selects).unwrap();

        let expect_aggs = vec![LogicalExpression::Aggregate {
            agg: Box::new(Sum),
            inputs: vec![LogicalExpression::new_column(0)],
            filter: None,
        }];
        assert_eq!(expect_aggs, out);

        let expected_selects = vec![(LogicalExpression::new_column(0), true)];
        assert_eq!(expected_selects, selects);
    }

    #[test]
    fn extract_aggregates_with_other_expressions() {
        let mut selects = vec![
            (LogicalExpression::new_column(1), false),
            (
                LogicalExpression::Aggregate {
                    agg: Box::new(Sum),
                    inputs: vec![LogicalExpression::new_column(0)],
                    filter: None,
                },
                false,
            ),
        ];

        let out = PlanContext::extract_aggregates(&mut selects).unwrap();

        let expect_aggs = vec![LogicalExpression::Aggregate {
            agg: Box::new(Sum),
            inputs: vec![LogicalExpression::new_column(0)],
            filter: None,
        }];
        assert_eq!(expect_aggs, out);

        let expected_selects = vec![
            (LogicalExpression::new_column(1), false),
            (LogicalExpression::new_column(0), true),
        ];
        assert_eq!(expected_selects, selects);
    }

    #[test]
    fn extract_group_by_single() {
        let mut selects = vec![
            (LogicalExpression::new_column(1), false),
            (LogicalExpression::new_column(0), false),
        ];

        let group_by = vec![LogicalExpression::new_column(0)];

        let out = PlanContext::extract_group_by_exprs(&mut selects, group_by, 2).unwrap();

        let expected_group_by = vec![LogicalExpression::new_column(0)];
        assert_eq!(expected_group_by, out);

        let expected_selects = vec![
            (LogicalExpression::new_column(1), false),
            (LogicalExpression::new_column(2), true),
        ];
        assert_eq!(expected_selects, selects);
    }

    #[test]
    fn extract_aggregate_and_group_by() {
        // t1(c1, c2)
        //
        // SELECT c1, sum(c2) FROM t1 GROUP BY c1

        let mut selects = vec![
            (LogicalExpression::new_column(0), false),
            (
                LogicalExpression::Aggregate {
                    agg: Box::new(Sum),
                    inputs: vec![LogicalExpression::new_column(1)],
                    filter: None,
                },
                false,
            ),
        ];

        let group_by = vec![LogicalExpression::new_column(0)];

        let got_aggs = PlanContext::extract_aggregates(&mut selects).unwrap();
        let got_groups = PlanContext::extract_group_by_exprs(&mut selects, group_by, 1).unwrap();

        let expected_aggs = vec![LogicalExpression::Aggregate {
            agg: Box::new(Sum),
            inputs: vec![LogicalExpression::new_column(1)],
            filter: None,
        }];
        let expected_groups = vec![LogicalExpression::new_column(0)];
        let expected_selects = vec![
            (LogicalExpression::new_column(1), true),
            (LogicalExpression::new_column(0), true),
        ];

        assert_eq!(expected_aggs, got_aggs);
        assert_eq!(expected_groups, got_groups);
        assert_eq!(expected_selects, selects);
    }
}
