pub mod expr_resolver;
pub mod resolve_context;
pub mod resolve_hybrid;
pub mod resolve_normal;
pub mod resolved_copy_to;
pub mod resolved_cte;
pub mod resolved_function;
pub mod resolved_table;
pub mod resolved_table_function;

use std::collections::HashMap;

use expr_resolver::ExpressionResolver;
use rayexec_bullet::{
    datatype::{DataType, DecimalTypeMeta, TimeUnit, TimestampTypeMeta},
    scalar::{
        decimal::{Decimal128Type, Decimal64Type, DecimalType},
        OwnedScalarValue, ScalarValue,
    },
};
use rayexec_error::{OptionExt, RayexecError, Result};
use rayexec_io::location::FileLocation;
use rayexec_parser::{
    ast::{self, ColumnDef, ObjectReference},
    meta::{AstMeta, Raw},
    statement::{RawStatement, Statement},
};
use resolve_context::{ItemReference, MaybeResolved, ResolveContext, ResolveListIdx};
use resolve_normal::{MaybeResolvedTable, NormalResolver};
use resolved_copy_to::ResolvedCopyTo;
use resolved_cte::ResolvedCte;
use resolved_table_function::{ResolvedTableFunctionReference, UnresolvedTableFunctionReference};
use serde::{Deserialize, Serialize};

use crate::{
    database::{catalog::CatalogTx, DatabaseContext},
    datasource::FileHandlers,
    functions::{copy::CopyToArgs, proto::FUNCTION_LOOKUP_CATALOG, table::TableFunctionArgs},
    logical::operator::LocationRequirement,
};

use super::binder::expr_binder::BaseExpressionBinder;

/// An AST statement with references bound to data inside of the `resolve_context`.
pub type ResolvedStatement = Statement<ResolvedMeta>;

/// Implementation of `AstMeta` which annotates the AST query with
/// tables/functions/etc found in the db.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedMeta;

impl AstMeta for ResolvedMeta {
    type ItemReference = ItemReference;
    /// Index into the tables bind list in bind data.
    type TableReference = ResolveListIdx;
    /// Index into the table functions bind list in bind data
    type TableFunctionReference = ResolveListIdx;
    // TODO: Having this be the actual table function args does require that we
    // clone them, and the args that go back into the ast don't actually do
    // anything, they're never referenced again.
    type TableFunctionArgs = TableFunctionArgs;
    /// Index into the functions bind list in bind data.
    type FunctionReference = ResolveListIdx;
    type DataType = DataType;
    type CopyToDestination = FileLocation;
    type CopyToOptions = CopyToArgs;
}

/// Determines the logic taken when encountering an unknown object in a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolveMode {
    /// Normal resolving, on missing object, return an appropriate error.
    Normal,
    /// Hybrid resolving, allow query resolveing to continue with the assumption
    /// that a remote node will handle anything that's left unresolved.
    Hybrid,
}

impl ResolveMode {
    pub const fn is_hybrid(&self) -> bool {
        matches!(self, ResolveMode::Hybrid)
    }
}

/// Resolves references in a raw SQL AST with entries in the catalog.
#[derive(Debug)]
pub struct Resolver<'a> {
    pub resolve_mode: ResolveMode,
    pub tx: &'a CatalogTx,
    pub context: &'a DatabaseContext,
    pub file_handlers: &'a FileHandlers,
}

impl<'a> Resolver<'a> {
    pub fn new(
        resolve_mode: ResolveMode,
        tx: &'a CatalogTx,
        context: &'a DatabaseContext,
        file_handlers: &'a FileHandlers,
    ) -> Self {
        Resolver {
            resolve_mode,
            tx,
            context,
            file_handlers,
        }
    }

    pub async fn resolve_statement(
        self,
        stmt: RawStatement,
    ) -> Result<(ResolvedStatement, ResolveContext)> {
        let mut resolve_context = ResolveContext::default();
        let bound = match stmt {
            Statement::Explain(explain) => {
                let body = match explain.body {
                    ast::ExplainBody::Query(query) => ast::ExplainBody::Query(
                        self.resolve_query(query, &mut resolve_context).await?,
                    ),
                };
                Statement::Explain(ast::ExplainNode {
                    analyze: explain.analyze,
                    verbose: explain.verbose,
                    body,
                    output: explain.output,
                })
            }
            Statement::CopyTo(copy_to) => {
                Statement::CopyTo(self.resolve_copy_to(copy_to, &mut resolve_context).await?)
            }
            Statement::Describe(describe) => match describe {
                ast::Describe::Query(query) => Statement::Describe(ast::Describe::Query(
                    self.resolve_query(query, &mut resolve_context).await?,
                )),
                ast::Describe::FromNode(from) => Statement::Describe(ast::Describe::FromNode(
                    self.resolve_from(from, &mut resolve_context).await?,
                )),
            },
            Statement::Query(query) => {
                Statement::Query(self.resolve_query(query, &mut resolve_context).await?)
            }
            Statement::Insert(insert) => {
                Statement::Insert(self.resolve_insert(insert, &mut resolve_context).await?)
            }
            Statement::CreateTable(create) => Statement::CreateTable(
                self.resolve_create_table(create, &mut resolve_context)
                    .await?,
            ),
            Statement::CreateSchema(create) => {
                Statement::CreateSchema(self.resolve_create_schema(create).await?)
            }
            Statement::Drop(drop) => Statement::Drop(self.resolve_drop(drop).await?),
            Statement::SetVariable(set) => Statement::SetVariable(ast::SetVariable {
                reference: Self::reference_to_strings(set.reference).into(),
                value: ExpressionResolver::new(&self)
                    .resolve_expression(set.value, &mut resolve_context)
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
            Statement::Attach(attach) => {
                Statement::Attach(self.resolve_attach(attach, &mut resolve_context).await?)
            }
            Statement::Detach(detach) => Statement::Detach(self.resolve_detach(detach).await?),
        };

        Ok((bound, resolve_context))
    }

    async fn resolve_attach(
        &self,
        attach: ast::Attach<Raw>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::Attach<ResolvedMeta>> {
        let mut options = HashMap::new();
        for (k, v) in attach.options {
            let v = ExpressionResolver::new(self)
                .resolve_expression(v, resolve_context)
                .await?;
            options.insert(k, v);
        }

        Ok(ast::Attach {
            datasource_name: attach.datasource_name,
            attach_type: attach.attach_type,
            alias: Self::reference_to_strings(attach.alias).into(),
            options,
        })
    }

    async fn resolve_detach(&self, detach: ast::Detach<Raw>) -> Result<ast::Detach<ResolvedMeta>> {
        // TODO: Replace 'ItemReference' with actual catalog reference. Similar
        // things will happen with Drop.
        Ok(ast::Detach {
            attach_type: detach.attach_type,
            alias: Self::reference_to_strings(detach.alias).into(),
        })
    }

    async fn resolve_copy_to(
        &self,
        copy_to: ast::CopyTo<Raw>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::CopyTo<ResolvedMeta>> {
        let source = match copy_to.source {
            ast::CopyToSource::Query(query) => {
                ast::CopyToSource::Query(self.resolve_query(query, resolve_context).await?)
            }
            ast::CopyToSource::Table(reference) => {
                let table = match self.resolve_mode {
                    ResolveMode::Normal => {
                        let table = NormalResolver::new(self.tx, self.context)
                            .require_resolve_table_or_cte(&reference, resolve_context)
                            .await?;
                        MaybeResolved::Resolved(table, LocationRequirement::ClientLocal)
                    }
                    ResolveMode::Hybrid => {
                        let table = NormalResolver::new(self.tx, self.context)
                            .resolve_table_or_cte(&reference, resolve_context)
                            .await?;

                        match table {
                            MaybeResolvedTable::Resolved(table) => {
                                MaybeResolved::Resolved(table, LocationRequirement::ClientLocal)
                            }
                            MaybeResolvedTable::UnresolvedWithCatalog(unbound) => {
                                MaybeResolved::Unresolved(unbound)
                            }
                            MaybeResolvedTable::Unresolved => {
                                return Err(RayexecError::new(format!(
                                    "Missing table or view for reference '{}'",
                                    reference
                                )))
                            }
                        }
                    }
                };

                let idx = resolve_context.tables.push_maybe_resolved(table);
                ast::CopyToSource::Table(idx)
            }
        };

        let mut options = HashMap::with_capacity(copy_to.options.len());
        for opt in copy_to.options {
            let key = opt.key.into_normalized_string();
            let expr = ExpressionResolver::new(self)
                .resolve_expression(opt.val, resolve_context)
                .await?;

            let val = match expr {
                ast::Expr::Literal(lit) => {
                    BaseExpressionBinder::bind_literal(&lit)?.try_into_scalar()?
                }
                // Ident allows for example `(FORMAT parquet)`, the user doesn't need to quote parquet.
                ast::Expr::Ident(ident) => {
                    OwnedScalarValue::Utf8(ident.into_normalized_string().into())
                }
                other => {
                    return Err(RayexecError::new(format!(
                        "COPY TO options must be constant, got: {other:?}"
                    )))
                }
            };

            options.insert(key, val);
        }

        let mut options = CopyToArgs { named: options };

        let target = match copy_to.target {
            ast::CopyToTarget::File(file_name) => {
                let func = match options.try_remove_format() {
                    Some(ScalarValue::Utf8(format)) => {
                        // User specified a format, lookup in system catalog.
                        let ent = self
                            .context
                            .system_catalog()?
                            .get_schema(self.tx, FUNCTION_LOOKUP_CATALOG)?
                            .required("function lookup schema")?
                            .get_copy_to_function_for_format(self.tx, &format)?;

                        match ent {
                            Some(ent) => ent.try_as_copy_to_function_entry()?.function.clone(),
                            None => {
                                return Err(RayexecError::new(format!(
                                    "No registered COPY TO function for format '{format}'"
                                )))
                            }
                        }
                    }
                    Some(other) => {
                        return Err(RayexecError::new(format!(
                            "Invalid format expression: {other}"
                        )))
                    }
                    None => {
                        // Infer from file name.
                        let handler =
                            self.file_handlers.find_match(&file_name).ok_or_else(|| {
                                RayexecError::new(format!(
                                    "No registered file handler for file '{file_name}'"
                                ))
                            })?;
                        handler
                            .copy_to
                            .as_ref()
                            .ok_or_else(|| RayexecError::new("No registered COPY TO function"))?
                            .clone()
                    }
                };

                resolve_context.copy_to = Some(ResolvedCopyTo { func });

                FileLocation::parse(&file_name)
            }
        };

        Ok(ast::CopyTo {
            source,
            target,
            options,
        })
    }

    async fn resolve_drop(
        &self,
        drop: ast::DropStatement<Raw>,
    ) -> Result<ast::DropStatement<ResolvedMeta>> {
        // TODO: Use search path.
        let mut name: ItemReference = Self::reference_to_strings(drop.name).into();
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

    async fn resolve_create_schema(
        &self,
        create: ast::CreateSchema<Raw>,
    ) -> Result<ast::CreateSchema<ResolvedMeta>> {
        // TODO: Search path.
        let mut name: ItemReference = Self::reference_to_strings(create.name).into();
        if name.0.len() == 1 {
            name.0.insert(0, "temp".to_string()); // Catalog
        }

        Ok(ast::CreateSchema {
            if_not_exists: create.if_not_exists,
            name,
        })
    }

    async fn resolve_create_table(
        &self,
        create: ast::CreateTable<Raw>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::CreateTable<ResolvedMeta>> {
        // TODO: Search path
        let mut name: ItemReference = Self::reference_to_strings(create.name).into();
        if create.temp {
            if name.0.len() == 1 {
                name.0.insert(0, "temp".to_string()); // Schema
                name.0.insert(0, "temp".to_string()); // Catalog
            }
            if name.0.len() == 2 {
                name.0.insert(0, "temp".to_string()); // Catalog
            }
        }

        let columns = create
            .columns
            .into_iter()
            .map(|col| {
                Ok(ColumnDef::<ResolvedMeta> {
                    name: col.name,
                    datatype: Self::ast_datatype_to_exec_datatype(col.datatype)?,
                    opts: col.opts,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let source = match create.source {
            Some(source) => Some(self.resolve_query(source, resolve_context).await?),
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

    async fn resolve_insert(
        &self,
        insert: ast::Insert<Raw>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::Insert<ResolvedMeta>> {
        let table = match self.resolve_mode {
            ResolveMode::Normal => {
                let table = NormalResolver::new(self.tx, self.context)
                    .require_resolve_table_or_cte(&insert.table, resolve_context)
                    .await?;
                MaybeResolved::Resolved(table, LocationRequirement::ClientLocal)
            }
            ResolveMode::Hybrid => {
                let table = NormalResolver::new(self.tx, self.context)
                    .resolve_table_or_cte(&insert.table, resolve_context)
                    .await?;

                match table {
                    MaybeResolvedTable::Resolved(table) => {
                        MaybeResolved::Resolved(table, LocationRequirement::ClientLocal)
                    }
                    MaybeResolvedTable::UnresolvedWithCatalog(unbound) => {
                        MaybeResolved::Unresolved(unbound)
                    }
                    MaybeResolvedTable::Unresolved => {
                        return Err(RayexecError::new(format!(
                            "Missing table or view for reference '{}'",
                            insert.table
                        )))
                    }
                }
            }
        };

        let source = self.resolve_query(insert.source, resolve_context).await?;

        let idx = resolve_context.tables.push_maybe_resolved(table);

        Ok(ast::Insert {
            table: idx,
            columns: insert.columns,
            source,
        })
    }

    async fn resolve_query(
        &self,
        query: ast::QueryNode<Raw>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::QueryNode<ResolvedMeta>> {
        /// Helper containing the actual logic for the resolve.
        ///
        /// Pulled out so we can accurately set the bind data depth before and
        /// after this.
        async fn resolve_query_inner(
            resolver: &Resolver<'_>,
            query: ast::QueryNode<Raw>,
            resolve_context: &mut ResolveContext,
        ) -> Result<ast::QueryNode<ResolvedMeta>> {
            let ctes = match query.ctes {
                Some(ctes) => Some(resolver.resolve_ctes(ctes, resolve_context).await?),
                None => None,
            };

            let body = resolver
                .resolve_query_node_body(query.body, resolve_context)
                .await?;

            // Resolve ORDER BY
            let order_by = match query.order_by {
                Some(order_by) => {
                    let mut order_bys = Vec::with_capacity(order_by.order_by_nodes.len());
                    for expr in order_by.order_by_nodes {
                        order_bys.push(resolver.resolve_order_by(expr, resolve_context).await?);
                    }
                    Some(ast::OrderByModifier {
                        order_by_nodes: order_bys,
                    })
                }
                None => None,
            };

            // Resolve LIMIT/OFFSET
            let limit = match query.limit.limit {
                Some(expr) => Some(
                    ExpressionResolver::new(resolver)
                        .resolve_expression(expr, resolve_context)
                        .await?,
                ),
                None => None,
            };
            let offset = match query.limit.offset {
                Some(expr) => Some(
                    ExpressionResolver::new(resolver)
                        .resolve_expression(expr, resolve_context)
                        .await?,
                ),
                None => None,
            };

            Ok(ast::QueryNode {
                ctes,
                body,
                order_by,
                limit: ast::LimitModifier { limit, offset },
            })
        }

        resolve_context.inc_depth();
        let result = resolve_query_inner(self, query, resolve_context).await;
        resolve_context.dec_depth();

        result
    }

    async fn resolve_query_node_body(
        &self,
        body: ast::QueryNodeBody<Raw>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::QueryNodeBody<ResolvedMeta>> {
        Ok(match body {
            ast::QueryNodeBody::Select(select) => ast::QueryNodeBody::Select(Box::new(
                self.resolve_select(*select, resolve_context).await?,
            )),
            ast::QueryNodeBody::Nested(nested) => ast::QueryNodeBody::Nested(Box::new(
                Box::pin(self.resolve_query(*nested, resolve_context)).await?,
            )),
            ast::QueryNodeBody::Values(values) => {
                ast::QueryNodeBody::Values(self.resolve_values(values, resolve_context).await?)
            }
            ast::QueryNodeBody::Set(ast::SetOp {
                left,
                right,
                operation,
                all,
            }) => {
                let left = Box::pin(self.resolve_query_node_body(*left, resolve_context)).await?;
                let right = Box::pin(self.resolve_query_node_body(*right, resolve_context)).await?;
                ast::QueryNodeBody::Set(ast::SetOp {
                    left: Box::new(left),
                    right: Box::new(right),
                    operation,
                    all,
                })
            }
        })
    }

    async fn resolve_ctes(
        &self,
        ctes: ast::CommonTableExprs<Raw>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::CommonTableExprs<ResolvedMeta>> {
        let mut resolved_ctes = Vec::with_capacity(ctes.ctes.len());

        for cte in ctes.ctes.into_iter() {
            let depth = resolve_context.current_depth;

            let resolved_body = Box::pin(self.resolve_query(*cte.body, resolve_context)).await?;
            let resolved_cte = ResolvedCte {
                name: cte.alias.as_normalized_string(),
                depth,
            };

            resolve_context.push_cte(resolved_cte);

            resolved_ctes.push(ast::CommonTableExpr {
                alias: cte.alias,
                column_aliases: cte.column_aliases,
                materialized: cte.materialized,
                body: Box::new(resolved_body),
            });
        }

        Ok(ast::CommonTableExprs {
            recursive: ctes.recursive,
            ctes: resolved_ctes,
        })
    }

    async fn resolve_select(
        &self,
        select: ast::SelectNode<Raw>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::SelectNode<ResolvedMeta>> {
        let distinct = match select.distinct {
            Some(distinct) => Some(match distinct {
                ast::DistinctModifier::On(exprs) => {
                    let mut bound = Vec::with_capacity(exprs.len());
                    for expr in exprs {
                        bound.push(
                            ExpressionResolver::new(self)
                                .resolve_expression(expr, resolve_context)
                                .await?,
                        );
                    }
                    ast::DistinctModifier::On(bound)
                }
                ast::DistinctModifier::All => ast::DistinctModifier::All,
            }),
            None => None,
        };

        let from = match select.from {
            Some(from) => Some(self.resolve_from(from, resolve_context).await?),
            None => None,
        };

        let where_expr = match select.where_expr {
            Some(expr) => Some(
                ExpressionResolver::new(self)
                    .resolve_expression(expr, resolve_context)
                    .await?,
            ),
            None => None,
        };

        let mut projections = Vec::with_capacity(select.projections.len());
        for projection in select.projections {
            projections.push(
                ExpressionResolver::new(self)
                    .resolve_select_expr(projection, resolve_context)
                    .await?,
            );
        }

        let group_by = match select.group_by {
            Some(group_by) => Some(match group_by {
                ast::GroupByNode::All => ast::GroupByNode::All,
                ast::GroupByNode::Exprs { exprs } => {
                    let mut bound = Vec::with_capacity(exprs.len());
                    for expr in exprs {
                        bound.push(
                            ExpressionResolver::new(self)
                                .resolve_group_by_expr(expr, resolve_context)
                                .await?,
                        );
                    }
                    ast::GroupByNode::Exprs { exprs: bound }
                }
            }),
            None => None,
        };

        let having = match select.having {
            Some(expr) => Some(
                ExpressionResolver::new(self)
                    .resolve_expression(expr, resolve_context)
                    .await?,
            ),
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

    async fn resolve_values(
        &self,
        values: ast::Values<Raw>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::Values<ResolvedMeta>> {
        let mut bound = Vec::with_capacity(values.rows.len());
        for row in values.rows {
            bound.push(
                ExpressionResolver::new(self)
                    .resolve_expressions(row, resolve_context)
                    .await?,
            );
        }
        Ok(ast::Values { rows: bound })
    }

    async fn resolve_order_by(
        &self,
        order_by: ast::OrderByNode<Raw>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::OrderByNode<ResolvedMeta>> {
        let expr = ExpressionResolver::new(self)
            .resolve_expression(order_by.expr, resolve_context)
            .await?;
        Ok(ast::OrderByNode {
            typ: order_by.typ,
            nulls: order_by.nulls,
            expr,
        })
    }

    async fn resolve_from(
        &self,
        from: ast::FromNode<Raw>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::FromNode<ResolvedMeta>> {
        let body = match from.body {
            ast::FromNodeBody::BaseTable(ast::FromBaseTable { reference }) => {
                let table = match self.resolve_mode {
                    ResolveMode::Normal => {
                        let table = NormalResolver::new(self.tx, self.context)
                            .require_resolve_table_or_cte(&reference, resolve_context)
                            .await?;
                        MaybeResolved::Resolved(table, LocationRequirement::ClientLocal)
                    }
                    ResolveMode::Hybrid => {
                        let table = NormalResolver::new(self.tx, self.context)
                            .resolve_table_or_cte(&reference, resolve_context)
                            .await?;

                        match table {
                            MaybeResolvedTable::Resolved(table) => {
                                MaybeResolved::Resolved(table, LocationRequirement::ClientLocal)
                            }
                            MaybeResolvedTable::UnresolvedWithCatalog(unbound) => {
                                MaybeResolved::Unresolved(unbound)
                            }
                            MaybeResolvedTable::Unresolved => {
                                return Err(RayexecError::new(format!(
                                    "Missing table or view for reference '{}'",
                                    reference
                                )))
                            }
                        }
                    }
                };

                let idx = resolve_context.tables.push_maybe_resolved(table);
                ast::FromNodeBody::BaseTable(ast::FromBaseTable { reference: idx })
            }
            ast::FromNodeBody::Subquery(ast::FromSubquery { query }) => {
                ast::FromNodeBody::Subquery(ast::FromSubquery {
                    query: Box::pin(self.resolve_query(query, resolve_context)).await?,
                })
            }
            ast::FromNodeBody::File(ast::FromFilePath { path }) => {
                match self.file_handlers.find_match(&path) {
                    Some(handler) => {
                        let args = TableFunctionArgs {
                            named: HashMap::new(),
                            positional: vec![OwnedScalarValue::Utf8(path.into())],
                        };

                        let name = handler.table_func.name().to_string();
                        let func = handler
                            .table_func
                            .plan_and_initialize(self.context, args.clone())
                            .await?;

                        let resolve_idx = resolve_context.table_functions.push_resolved(
                            ResolvedTableFunctionReference { name, func },
                            LocationRequirement::ClientLocal,
                        );

                        ast::FromNodeBody::TableFunction(ast::FromTableFunction {
                            reference: resolve_idx,
                            // TODO: Not needed.
                            args,
                        })
                    }
                    None => {
                        return Err(RayexecError::new(format!(
                            "No suitable file handlers found for '{path}'"
                        )))
                    }
                }
            }
            ast::FromNodeBody::TableFunction(ast::FromTableFunction { reference, args }) => {
                let args = ExpressionResolver::new(self)
                    .resolve_table_function_args(args)
                    .await?;

                let function = match self.resolve_mode {
                    ResolveMode::Normal => {
                        let function = NormalResolver::new(self.tx, self.context)
                            .require_resolve_table_function(&reference)?;
                        let function = function
                            .plan_and_initialize(self.context, args.clone())
                            .await?;

                        MaybeResolved::Resolved(
                            ResolvedTableFunctionReference {
                                name: function.table_function().name().to_string(),
                                func: function,
                            },
                            LocationRequirement::ClientLocal,
                        )
                    }
                    ResolveMode::Hybrid => {
                        match NormalResolver::new(self.tx, self.context)
                            .resolve_table_function(&reference)?
                        {
                            Some(function) => {
                                let function = function
                                    .plan_and_initialize(self.context, args.clone())
                                    .await?;
                                MaybeResolved::Resolved(
                                    ResolvedTableFunctionReference {
                                        name: function.table_function().name().to_string(),
                                        func: function,
                                    },
                                    LocationRequirement::ClientLocal,
                                )
                            }
                            None => MaybeResolved::Unresolved(UnresolvedTableFunctionReference {
                                reference,
                                args: args.clone(),
                            }),
                        }
                    }
                };

                let resolve_idx = resolve_context
                    .table_functions
                    .push_maybe_resolved(function);
                ast::FromNodeBody::TableFunction(ast::FromTableFunction {
                    reference: resolve_idx,
                    // TODO: These args aren't actually needed when bound. Not
                    // sure completely sure what we want to do here.
                    args,
                })
            }
            ast::FromNodeBody::Join(ast::FromJoin {
                left,
                right,
                join_type,
                join_condition,
            }) => {
                let left = Box::pin(self.resolve_from(*left, resolve_context)).await?;
                let right = Box::pin(self.resolve_from(*right, resolve_context)).await?;

                let join_condition = match join_condition {
                    ast::JoinCondition::On(expr) => {
                        let expr = ExpressionResolver::new(self)
                            .resolve_expression(expr, resolve_context)
                            .await?;
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

    fn reference_to_strings(reference: ObjectReference) -> Vec<String> {
        reference
            .0
            .into_iter()
            .map(|ident| ident.into_normalized_string())
            .collect()
    }

    fn ast_datatype_to_exec_datatype(datatype: ast::DataType) -> Result<DataType> {
        Ok(match datatype {
            ast::DataType::Varchar(_) => DataType::Utf8,
            ast::DataType::TinyInt => DataType::Int8,
            ast::DataType::SmallInt => DataType::Int16,
            ast::DataType::Integer => DataType::Int32,
            ast::DataType::BigInt => DataType::Int64,
            ast::DataType::Real => DataType::Float32,
            ast::DataType::Double => DataType::Float64,
            ast::DataType::Decimal(prec, scale) => {
                // - Precision cannot be negative.
                // - Specifying just precision defaults to a 0 scale.
                // - Defaults to decimal64 prec and scale if neither provided.
                match prec {
                    Some(prec) if prec < 0 => {
                        return Err(RayexecError::new("Precision cannot be negative"))
                    }
                    Some(prec) => {
                        let prec: u8 = prec.try_into().map_err(|_| {
                            RayexecError::new(format!("Precision too high: {prec}"))
                        })?;

                        let scale: i8 = match scale {
                            Some(scale) => scale.try_into().map_err(|_| {
                                RayexecError::new(format!("Scale too high: {scale}"))
                            })?,
                            None => 0, // TODO: I'm not sure what behavior we want here, but it seems to match postgres.
                        };

                        if scale as i16 > prec as i16 {
                            return Err(RayexecError::new(
                                "Decimal scale cannot be larger than precision",
                            ));
                        }

                        if prec <= Decimal64Type::MAX_PRECISION {
                            DataType::Decimal64(DecimalTypeMeta::new(prec, scale))
                        } else if prec <= Decimal128Type::MAX_PRECISION {
                            DataType::Decimal128(DecimalTypeMeta::new(prec, scale))
                        } else {
                            return Err(RayexecError::new(
                                "Decimal precision too big for max decimal size",
                            ));
                        }
                    }
                    None => DataType::Decimal64(DecimalTypeMeta::new(
                        Decimal64Type::MAX_PRECISION,
                        Decimal64Type::DEFAULT_SCALE,
                    )),
                }
            }
            ast::DataType::Bool => DataType::Boolean,
            ast::DataType::Date => DataType::Date32,
            ast::DataType::Timestamp => {
                // Microsecond matches postgres default.
                DataType::Timestamp(TimestampTypeMeta::new(TimeUnit::Microsecond))
            }
            ast::DataType::Interval => DataType::Interval,
        })
    }
}
