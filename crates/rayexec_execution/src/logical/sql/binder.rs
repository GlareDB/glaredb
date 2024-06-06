use std::collections::HashMap;
use std::fmt;

use rayexec_bullet::field::DataType;
use rayexec_error::{RayexecError, Result};
use rayexec_parser::{
    ast::{self, ColumnDef, ObjectReference, ReplaceColumn},
    meta::{AstMeta, Raw},
    statement::{RawStatement, Statement},
};

use crate::{
    database::{catalog::CatalogTx, entry::TableEntry, DatabaseContext},
    functions::{aggregate::GenericAggregateFunction, scalar::GenericScalarFunction},
};

pub type BoundStatement = Statement<Bound>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bound;

// TODO: Table function
#[derive(Debug, Clone, PartialEq)]
pub enum BoundFunctionReference {
    Scalar(Box<dyn GenericScalarFunction>),
    Aggregate(Box<dyn GenericAggregateFunction>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundCteReference {
    /// Index into the CTE map.
    pub idx: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundTableOrCteReference {
    Table {
        catalog: String,
        schema: String,
        entry: TableEntry,
    },
    Cte(BoundCteReference),
}

// TODO: Figure out how we want to represent things like tables in a CREATE
// TABLE. We don't want to resolve, so a vec of strings works for now.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundItemReference(pub Vec<String>);

impl BoundItemReference {
    pub fn pop(&mut self) -> Result<String> {
        // TODO: Could be more informative with this error.
        self.0
            .pop()
            .ok_or_else(|| RayexecError::new("End of reference"))
    }

    pub fn pop_2(&mut self) -> Result<[String; 2]> {
        let a = self
            .0
            .pop()
            .ok_or_else(|| RayexecError::new("Expected 2 identifiers, got 0"))?;
        let b = self
            .0
            .pop()
            .ok_or_else(|| RayexecError::new("Expected 2 identifiers, got 1"))?;
        Ok([b, a])
    }

    pub fn pop_3(&mut self) -> Result<[String; 3]> {
        let a = self
            .0
            .pop()
            .ok_or_else(|| RayexecError::new("Expected 3 identifiers, got 0"))?;
        let b = self
            .0
            .pop()
            .ok_or_else(|| RayexecError::new("Expected 3 identifiers, got 1"))?;
        let c = self
            .0
            .pop()
            .ok_or_else(|| RayexecError::new("Expected 3 identifiers, got 2"))?;
        Ok([c, b, a])
    }
}

impl From<Vec<String>> for BoundItemReference {
    fn from(value: Vec<String>) -> Self {
        BoundItemReference(value)
    }
}

impl fmt::Display for BoundItemReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.join(","))
    }
}

// TODO: Table function associated type (separate from above). Will likely be
// the specialized table function.
impl AstMeta for Bound {
    type DataSourceName = String;
    type ItemReference = BoundItemReference;
    type TableReference = BoundTableOrCteReference;
    type FunctionReference = BoundFunctionReference;
    type ColumnReference = String;
    type DataType = DataType;
}

#[derive(Debug)]
pub struct BindData {}

/// Binds a raw SQL AST with entries in the catalog.
#[derive(Debug)]
pub struct Binder<'a> {
    tx: &'a CatalogTx,
    context: &'a DatabaseContext,
    data: BindData,
}

impl<'a> Binder<'a> {
    pub fn new(tx: &'a CatalogTx, context: &'a DatabaseContext) -> Self {
        Binder {
            tx,
            context,
            data: BindData {},
        }
    }

    pub async fn bind_statement(
        mut self,
        stmt: RawStatement,
    ) -> Result<(BoundStatement, BindData)> {
        let bound = match stmt {
            Statement::Explain(explain) => {
                let body = match explain.body {
                    ast::ExplainBody::Query(query) => {
                        ast::ExplainBody::Query(self.bind_query(query).await?)
                    }
                };
                Statement::Explain(ast::ExplainNode {
                    analyze: explain.analyze,
                    verbose: explain.verbose,
                    body,
                    output: explain.output,
                })
            }
            Statement::Query(query) => Statement::Query(self.bind_query(query).await?),
            Statement::Insert(insert) => Statement::Insert(self.bind_insert(insert).await?),
            Statement::CreateTable(create) => {
                Statement::CreateTable(self.bind_create_table(create).await?)
            }
            Statement::CreateSchema(create) => {
                Statement::CreateSchema(self.bind_create_schema(create).await?)
            }
            Statement::Drop(drop) => Statement::Drop(self.bind_drop(drop).await?),
            Statement::SetVariable(set) => Statement::SetVariable(ast::SetVariable {
                reference: Self::reference_to_strings(set.reference).into(),
                value: ExpressionBinder::new(&self)
                    .bind_expression(set.value)
                    .await?,
            }),
            Statement::ShowVariable(show) => Statement::ShowVariable(ast::ShowVariable {
                reference: Self::reference_to_strings(show.reference).into(),
            }),
            Statement::ResetVariable(reset) => Statement::ResetVariable(ast::ResetVariable {
                var: match reset.var {
                    ast::VariableOrAll::All => ast::VariableOrAll::All,
                    ast::VariableOrAll::Variable(var) => {
                        ast::VariableOrAll::Variable(Self::reference_to_strings(var).into())
                    }
                },
            }),
            Statement::Attach(attach) => Statement::Attach(self.bind_attach(attach).await?),
            Statement::Detach(detach) => Statement::Detach(self.bind_detach(detach).await?),
        };

        Ok((bound, self.data))
    }

    async fn bind_attach(&mut self, attach: ast::Attach<Raw>) -> Result<ast::Attach<Bound>> {
        let mut options = HashMap::new();
        for (k, v) in attach.options {
            let v = ExpressionBinder::new(self).bind_expression(v).await?;
            options.insert(k, v);
        }

        Ok(ast::Attach {
            datasource_name: attach.datasource_name.into_normalized_string(),
            attach_type: attach.attach_type,
            alias: Self::reference_to_strings(attach.alias).into(),
            options,
        })
    }

    async fn bind_detach(&mut self, detach: ast::Detach<Raw>) -> Result<ast::Detach<Bound>> {
        // TODO: Replace 'ItemReference' with actual catalog reference. Similar
        // things will happen with Drop.
        Ok(ast::Detach {
            attach_type: detach.attach_type,
            alias: Self::reference_to_strings(detach.alias).into(),
        })
    }

    async fn bind_drop(
        &mut self,
        drop: ast::DropStatement<Raw>,
    ) -> Result<ast::DropStatement<Bound>> {
        // TODO: Use search path.
        let mut name: BoundItemReference = Self::reference_to_strings(drop.name).into();
        match drop.drop_type {
            ast::DropType::Schema => {
                if name.0.len() == 1 {
                    name.0.insert(0, "temp".to_string()); // Catalog
                }
            }
            _ => {
                if name.0.len() == 1 {
                    name.0.insert(0, "temp".to_string()); // Schema
                    name.0.insert(0, "temp".to_string()); // Catalog
                }
                if name.0.len() == 2 {
                    name.0.insert(0, "temp".to_string()); // Catalog
                }
            }
        }

        Ok(ast::DropStatement {
            drop_type: drop.drop_type,
            if_exists: drop.if_exists,
            name,
            deps: drop.deps,
        })
    }

    async fn bind_create_schema(
        &mut self,
        create: ast::CreateSchema<Raw>,
    ) -> Result<ast::CreateSchema<Bound>> {
        // TODO: Search path.
        let mut name: BoundItemReference = Self::reference_to_strings(create.name).into();
        if name.0.len() == 1 {
            name.0.insert(0, "temp".to_string()); // Catalog
        }

        Ok(ast::CreateSchema {
            if_not_exists: create.if_not_exists,
            name,
        })
    }

    async fn bind_create_table(
        &mut self,
        create: ast::CreateTable<Raw>,
    ) -> Result<ast::CreateTable<Bound>> {
        // TODO: Search path
        let mut name: BoundItemReference = Self::reference_to_strings(create.name).into();
        if create.temp {
            if name.0.len() == 1 {
                name.0.insert(0, "temp".to_string()); // Schema
                name.0.insert(0, "temp".to_string()); // Catalog
            }
            if name.0.len() == 2 {
                name.0.insert(0, "temp".to_string()); // Catalog
            }
        }

        let columns: Vec<_> = create
            .columns
            .into_iter()
            .map(|col| ColumnDef::<Bound> {
                name: col.name.into_normalized_string(),
                datatype: Self::ast_datatype_to_exec_datatype(col.datatype),
                opts: col.opts,
            })
            .collect();

        let source = match create.source {
            Some(source) => Some(self.bind_query(source).await?),
            None => None,
        };

        Ok(ast::CreateTable {
            or_replace: create.or_replace,
            if_not_exists: create.if_not_exists,
            temp: create.temp,
            external: create.external,
            name,
            columns,
            source,
        })
    }

    async fn bind_insert(&mut self, insert: ast::Insert<Raw>) -> Result<ast::Insert<Bound>> {
        let table = self.resolve_table(insert.table).await?;
        let source = self.bind_query(insert.source).await?;
        Ok(ast::Insert {
            table,
            columns: insert.columns,
            source,
        })
    }

    async fn bind_query(&mut self, query: ast::QueryNode<Raw>) -> Result<ast::QueryNode<Bound>> {
        let ctes = match query.ctes {
            Some(ctes) => Some(self.bind_ctes(ctes).await?),
            None => None,
        };

        let body = match query.body {
            ast::QueryNodeBody::Select(select) => {
                ast::QueryNodeBody::Select(Box::new(self.bind_select(*select).await?))
            }
            ast::QueryNodeBody::Values(values) => {
                ast::QueryNodeBody::Values(self.bind_values(values).await?)
            }
            ast::QueryNodeBody::Set { .. } => unimplemented!(),
        };

        // Bind ORDER BY
        let mut order_by = Vec::with_capacity(query.order_by.len());
        for expr in query.order_by {
            order_by.push(self.bind_order_by(expr).await?);
        }

        // Bind LIMIT/OFFSET
        let limit = match query.limit.limit {
            Some(expr) => Some(ExpressionBinder::new(self).bind_expression(expr).await?),
            None => None,
        };
        let offset = match query.limit.offset {
            Some(expr) => Some(ExpressionBinder::new(self).bind_expression(expr).await?),
            None => None,
        };

        Ok(ast::QueryNode {
            ctes,
            body,
            order_by,
            limit: ast::LimitModifier { limit, offset },
        })
    }

    async fn bind_ctes(
        &mut self,
        _ctes: ast::CommonTableExprDefs<Raw>,
    ) -> Result<ast::CommonTableExprDefs<Bound>> {
        unimplemented!()
    }

    async fn bind_select(
        &mut self,
        select: ast::SelectNode<Raw>,
    ) -> Result<ast::SelectNode<Bound>> {
        // Bind DISTINCT
        let distinct = match select.distinct {
            Some(distinct) => Some(match distinct {
                ast::DistinctModifier::On(exprs) => {
                    let mut bound = Vec::with_capacity(exprs.len());
                    for expr in exprs {
                        bound.push(ExpressionBinder::new(self).bind_expression(expr).await?);
                    }
                    ast::DistinctModifier::On(bound)
                }
                ast::DistinctModifier::All => ast::DistinctModifier::All,
            }),
            None => None,
        };

        // Bind FROM
        let from = match select.from {
            Some(from) => Some(self.bind_from(from).await?),
            None => None,
        };

        // Bind WHERE
        let where_expr = match select.where_expr {
            Some(expr) => Some(ExpressionBinder::new(self).bind_expression(expr).await?),
            None => None,
        };

        // Bind SELECT list
        let mut projections = Vec::with_capacity(select.projections.len());
        for projection in select.projections {
            projections.push(
                ExpressionBinder::new(self)
                    .bind_select_expr(projection)
                    .await?,
            );
        }

        // Bind GROUP BY
        let group_by = match select.group_by {
            Some(group_by) => Some(match group_by {
                ast::GroupByNode::All => ast::GroupByNode::All,
                ast::GroupByNode::Exprs { exprs } => {
                    let mut bound = Vec::with_capacity(exprs.len());
                    for expr in exprs {
                        bound.push(ExpressionBinder::new(self).bind_group_by_expr(expr).await?);
                    }
                    ast::GroupByNode::Exprs { exprs: bound }
                }
            }),
            None => None,
        };

        // Bind HAVING
        let having = match select.having {
            Some(expr) => Some(ExpressionBinder::new(self).bind_expression(expr).await?),
            None => None,
        };

        Ok(ast::SelectNode {
            distinct,
            projections,
            from,
            where_expr,
            group_by,
            having,
        })
    }

    async fn bind_values(&mut self, values: ast::Values<Raw>) -> Result<ast::Values<Bound>> {
        let mut bound = Vec::with_capacity(values.rows.len());
        for row in values.rows {
            bound.push(ExpressionBinder::new(self).bind_expressions(row).await?);
        }
        Ok(ast::Values { rows: bound })
    }

    async fn bind_order_by(
        &mut self,
        order_by: ast::OrderByNode<Raw>,
    ) -> Result<ast::OrderByNode<Bound>> {
        let expr = ExpressionBinder::new(self)
            .bind_expression(order_by.expr)
            .await?;
        Ok(ast::OrderByNode {
            typ: order_by.typ,
            nulls: order_by.nulls,
            expr,
        })
    }

    async fn bind_from(&mut self, from: ast::FromNode<Raw>) -> Result<ast::FromNode<Bound>> {
        let body = match from.body {
            ast::FromNodeBody::BaseTable(ast::FromBaseTable { reference }) => {
                ast::FromNodeBody::BaseTable(ast::FromBaseTable {
                    reference: self.resolve_table(reference).await?,
                })
            }
            ast::FromNodeBody::Subquery(ast::FromSubquery { query }) => {
                ast::FromNodeBody::Subquery(ast::FromSubquery {
                    query: Box::pin(self.bind_query(query)).await?,
                })
            }
            ast::FromNodeBody::TableFunction(ast::FromTableFunction { .. }) => {
                unimplemented!()
            }
            ast::FromNodeBody::Join(ast::FromJoin {
                left,
                right,
                join_type,
                join_condition,
            }) => {
                let left = Box::pin(self.bind_from(*left)).await?;
                let right = Box::pin(self.bind_from(*right)).await?;

                let join_condition = match join_condition {
                    ast::JoinCondition::On(expr) => {
                        let expr = ExpressionBinder::new(self).bind_expression(expr).await?;
                        ast::JoinCondition::On(expr)
                    }
                    ast::JoinCondition::Using(idents) => ast::JoinCondition::Using(idents),
                    ast::JoinCondition::Natural => ast::JoinCondition::Natural,
                    ast::JoinCondition::None => ast::JoinCondition::None,
                };

                ast::FromNodeBody::Join(ast::FromJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type,
                    join_condition,
                })
            }
        };

        Ok(ast::FromNode {
            alias: from.alias,
            body,
        })
    }

    async fn resolve_table(
        &mut self,
        mut reference: ast::ObjectReference,
    ) -> Result<BoundTableOrCteReference> {
        // TODO: If len == 1, search in CTE map in bind data.

        // TODO: Seach path.
        let [catalog, schema, table] = match reference.0.len() {
            1 => [
                "temp".to_string(),
                "temp".to_string(),
                reference.0.pop().unwrap().into_normalized_string(),
            ],
            2 => {
                let table = reference.0.pop().unwrap().into_normalized_string();
                let schema = reference.0.pop().unwrap().into_normalized_string();
                ["temp".to_string(), schema, table]
            }
            3 => {
                let table = reference.0.pop().unwrap().into_normalized_string();
                let schema = reference.0.pop().unwrap().into_normalized_string();
                let catalog = reference.0.pop().unwrap().into_normalized_string();
                [catalog, schema, table]
            }
            _ => {
                return Err(RayexecError::new(
                    "Unexpected number of identifiers in table reference",
                ))
            }
        };

        if let Some(entry) = self
            .context
            .get_catalog(&catalog)?
            .get_table_entry(self.tx, &schema, &table)
            .await?
        {
            Ok(BoundTableOrCteReference::Table {
                catalog,
                schema,
                entry,
            })
        } else {
            Err(RayexecError::new(format!(
                "Unable to find table or view for '{catalog}.{schema}.{table}'"
            )))
        }
    }

    fn reference_to_strings(reference: ObjectReference) -> Vec<String> {
        reference
            .0
            .into_iter()
            .map(|ident| ident.into_normalized_string())
            .collect()
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

struct ExpressionBinder<'a> {
    binder: &'a Binder<'a>,
}

impl<'a> ExpressionBinder<'a> {
    fn new(binder: &'a Binder) -> Self {
        ExpressionBinder { binder }
    }

    async fn bind_select_expr(
        &self,
        select_expr: ast::SelectExpr<Raw>,
    ) -> Result<ast::SelectExpr<Bound>> {
        match select_expr {
            ast::SelectExpr::Expr(expr) => {
                Ok(ast::SelectExpr::Expr(self.bind_expression(expr).await?))
            }
            ast::SelectExpr::AliasedExpr(expr, alias) => Ok(ast::SelectExpr::AliasedExpr(
                self.bind_expression(expr).await?,
                alias,
            )),
            ast::SelectExpr::QualifiedWildcard(object_name, wildcard) => {
                Ok(ast::SelectExpr::QualifiedWildcard(
                    object_name,
                    self.bind_wildcard(wildcard).await?,
                ))
            }
            ast::SelectExpr::Wildcard(wildcard) => Ok(ast::SelectExpr::Wildcard(
                self.bind_wildcard(wildcard).await?,
            )),
        }
    }

    async fn bind_wildcard(&self, wildcard: ast::Wildcard<Raw>) -> Result<ast::Wildcard<Bound>> {
        let mut replace_cols = Vec::with_capacity(wildcard.replace_cols.len());
        for replace in wildcard.replace_cols {
            replace_cols.push(ReplaceColumn {
                col: replace.col,
                expr: self.bind_expression(replace.expr).await?,
            });
        }

        Ok(ast::Wildcard {
            exclude_cols: wildcard.exclude_cols,
            replace_cols,
        })
    }

    async fn bind_group_by_expr(
        &self,
        expr: ast::GroupByExpr<Raw>,
    ) -> Result<ast::GroupByExpr<Bound>> {
        Ok(match expr {
            ast::GroupByExpr::Expr(exprs) => {
                ast::GroupByExpr::Expr(self.bind_expressions(exprs).await?)
            }
            ast::GroupByExpr::Cube(exprs) => {
                ast::GroupByExpr::Cube(self.bind_expressions(exprs).await?)
            }
            ast::GroupByExpr::Rollup(exprs) => {
                ast::GroupByExpr::Rollup(self.bind_expressions(exprs).await?)
            }
            ast::GroupByExpr::GroupingSets(exprs) => {
                ast::GroupByExpr::GroupingSets(self.bind_expressions(exprs).await?)
            }
        })
    }

    async fn bind_expressions(
        &self,
        exprs: impl IntoIterator<Item = ast::Expr<Raw>>,
    ) -> Result<Vec<ast::Expr<Bound>>> {
        let mut bound = Vec::new();
        for expr in exprs {
            bound.push(self.bind_expression(expr).await?);
        }
        Ok(bound)
    }

    /// Bind an expression.
    async fn bind_expression(&self, expr: ast::Expr<Raw>) -> Result<ast::Expr<Bound>> {
        match expr {
            ast::Expr::Ident(ident) => Ok(ast::Expr::Ident(ident)),
            ast::Expr::CompoundIdent(idents) => Ok(ast::Expr::CompoundIdent(idents)),
            ast::Expr::Literal(lit) => Ok(ast::Expr::Literal(match lit {
                ast::Literal::Number(s) => ast::Literal::Number(s),
                ast::Literal::SingleQuotedString(s) => ast::Literal::SingleQuotedString(s),
                ast::Literal::Boolean(b) => ast::Literal::Boolean(b),
                ast::Literal::Null => ast::Literal::Null,
                ast::Literal::Struct { keys, values } => {
                    let bound = Box::pin(self.bind_expressions(values)).await?;
                    ast::Literal::Struct {
                        keys,
                        values: bound,
                    }
                }
            })),
            ast::Expr::BinaryExpr { left, op, right } => Ok(ast::Expr::BinaryExpr {
                left: Box::new(Box::pin(self.bind_expression(*left)).await?),
                op,
                right: Box::new(Box::pin(self.bind_expression(*right)).await?),
            }),
            ast::Expr::Function(func) => {
                // TODO: Search path (with system being the first to check)
                if func.reference.0.len() != 1 {
                    return Err(RayexecError::new(
                        "Qualified function names not yet supported",
                    ));
                }
                let func_name = &func.reference.0[0].as_normalized_string();
                let catalog = "system";
                let schema = "glare_catalog";

                let filter = match func.filter {
                    Some(filter) => Some(Box::new(Box::pin(self.bind_expression(*filter)).await?)),
                    None => None,
                };

                let mut args = Vec::with_capacity(func.args.len());
                for func_arg in func.args {
                    let func_arg = match func_arg {
                        ast::FunctionArg::Named { name, arg } => ast::FunctionArg::Named {
                            name,
                            arg: Box::pin(self.bind_expression(arg)).await?,
                        },
                        ast::FunctionArg::Unnamed { arg } => ast::FunctionArg::Unnamed {
                            arg: Box::pin(self.bind_expression(arg)).await?,
                        },
                    };
                    args.push(func_arg);
                }

                // Check scalars first.
                if let Some(scalar) = self
                    .binder
                    .context
                    .get_catalog(catalog)?
                    .get_scalar_fn(self.binder.tx, schema, func_name)
                    .await?
                {
                    return Ok(ast::Expr::Function(ast::Function {
                        reference: BoundFunctionReference::Scalar(scalar),
                        args,
                        filter,
                    }));
                }

                // Now check aggregates.
                if let Some(aggregate) = self
                    .binder
                    .context
                    .get_catalog(catalog)?
                    .get_aggregate_fn(self.binder.tx, schema, func_name)
                    .await?
                {
                    return Ok(ast::Expr::Function(ast::Function {
                        reference: BoundFunctionReference::Aggregate(aggregate),
                        args,
                        filter,
                    }));
                }

                Err(RayexecError::new(format!(
                    "Cannot resolve function with name {}",
                    func.reference
                )))
            }
            _ => unimplemented!(),
        }
    }
}
