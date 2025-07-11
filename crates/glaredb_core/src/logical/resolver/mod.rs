pub mod expr_resolver;
pub mod resolve_context;
pub mod resolve_normal;
pub mod resolved_cte;
pub mod resolved_function;
pub mod resolved_table;
pub mod resolved_table_function;

use std::collections::HashMap;

use expr_resolver::ExpressionResolver;
use glaredb_error::{DbError, Result, not_implemented};
use glaredb_parser::ast::{self, ColumnDef, ObjectReference};
use glaredb_parser::meta::{AstMeta, Raw};
use glaredb_parser::parser;
use glaredb_parser::statement::{RawStatement, Statement};
use resolve_context::{ItemReference, MaybeResolved, ResolveContext, ResolveListIdx};
use resolve_normal::{MaybeResolvedTable, NormalResolver};
use resolved_cte::ResolvedCte;
use resolved_table::ResolvedTableOrCteReference;
use resolved_table_function::ResolvedTableFunctionReference;
use serde::{Deserialize, Serialize};

use super::binder::constant_binder::ConstantBinder;
use super::binder::expr_binder::BaseExpressionBinder;
use super::binder::ident::BinderIdent;
use super::binder::table_list::TableAlias;
use crate::arrays::datatype::{DataType, DecimalTypeMeta, TimeUnit, TimestampTypeMeta};
use crate::arrays::scalar::ScalarValue;
use crate::arrays::scalar::decimal::{Decimal64Type, Decimal128Type, DecimalType};
use crate::catalog::context::DatabaseContext;
use crate::catalog::system::{
    BuiltinView,
    SHOW_DATABASES_VIEW,
    SHOW_SCHEMAS_VIEW,
    SHOW_TABLES_VIEW,
};
use crate::expr;
use crate::functions::table::TableFunctionInput;
use crate::functions::table::scan::ScanContext;
use crate::logical::operator::LocationRequirement;
use crate::runtime::system::SystemRuntime;

// TODO: Lots of Box::pin calls for recursive async resolves. These don't seem
// to affect performance, but it does look "off". There may be a way to avoid
// it.

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
    /// Index into the functions bind list in bind data.
    type FunctionReference = ResolveListIdx;
    type SubqueryOptions = ResolvedSubqueryOptions;
    type DataType = DataType;
    type CopyToDestination = (); // TODO
    type CopyToOptions = ();
    /// SHOW statements will be converted to views if need during the resolve
    /// step (e.g. for SHOW DATABASES). If we produce a resolved SHOW, it will
    /// always be pointing to a variable.
    type ShowReference = ItemReference;
}

/// Options for a resolved subquery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResolvedSubqueryOptions {
    /// Normal subquery, no additional options needed.
    ///
    /// Normal subqueries can reference columns outside of its scope.
    Normal,
    /// View subquery.
    ///
    /// We include a table alias representing the path of the view according to
    /// the catalog to enable qualifying column references in the query.
    ///
    /// Column aliases have the following precedence:
    /// 1. Aliases applied when calling the view in FROM
    /// 2. Aliases stored on the view during create
    /// 3. Unaliases inner columns
    ///
    /// View subqueries cannot reference columns outside of itself.
    View {
        table_alias: TableAlias,
        column_aliases: Vec<String>,
    },
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

#[derive(Debug)]
pub struct ResolveConfig {
    pub enable_function_chaining: bool,
}

/// Resolves references in a raw SQL AST with entries in the catalog.
#[derive(Debug)]
pub struct Resolver<'a, R: SystemRuntime> {
    pub resolve_mode: ResolveMode,
    pub context: &'a DatabaseContext,
    pub runtime: &'a R,
    pub config: ResolveConfig,
}

impl<'a, R> Resolver<'a, R>
where
    R: SystemRuntime,
{
    pub fn new(
        resolve_mode: ResolveMode,
        context: &'a DatabaseContext,
        runtime: &'a R,
        config: ResolveConfig,
    ) -> Self {
        Resolver {
            resolve_mode,
            context,
            runtime,
            config,
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
            Statement::CreateView(create) => Statement::CreateView(
                self.resolve_create_view(create, &mut resolve_context)
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
            Statement::Show(show) => self.resolve_show(show, &mut resolve_context).await?,
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
            Statement::Discard(discard) => Statement::Discard(discard),
        };

        Ok((bound, resolve_context))
    }

    /// Resolve a SHOW statement.
    ///
    /// This may replace the SHOW with a SELECT for special statements (e.g.
    /// SHOW DATABASES).
    async fn resolve_show(
        &self,
        show: ast::Show<Raw>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ResolvedStatement> {
        let get_view_query = |view: BuiltinView| {
            let mut stmts = parser::parse(view.view)?;
            let stmt = match stmts.len() {
                1 => stmts.pop().unwrap(),
                other => return Err(DbError::new(format!("Expected 1 statement, got {other}"))),
            };

            match stmt {
                Statement::Query(q) => Ok(q),
                other => Err(DbError::new(format!(
                    "Expected query statement, got {other:?}"
                ))),
            }
        };

        match show.reference {
            ast::ShowReference::Variable(var) => Ok(Statement::Show(ast::Show {
                reference: Self::reference_to_strings(var).into(),
            })),
            ast::ShowReference::Databases => {
                let query = get_view_query(SHOW_DATABASES_VIEW)?;
                let query = Box::pin(self.resolve_query(query, resolve_context)).await?;
                Ok(Statement::Query(query))
            }
            ast::ShowReference::Schemas => {
                let query = get_view_query(SHOW_SCHEMAS_VIEW)?;
                let query = Box::pin(self.resolve_query(query, resolve_context)).await?;
                Ok(Statement::Query(query))
            }
            ast::ShowReference::Tables => {
                let query = get_view_query(SHOW_TABLES_VIEW)?;
                let query = Box::pin(self.resolve_query(query, resolve_context)).await?;
                Ok(Statement::Query(query))
            }
        }
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
        let _source = match copy_to.source {
            ast::CopyToSource::Query(query) => ast::CopyToSource::Query(Box::new(
                self.resolve_query(*query, resolve_context).await?,
            )),
            ast::CopyToSource::Table(reference) => {
                let table = match self.resolve_mode {
                    ResolveMode::Normal => {
                        let table = NormalResolver::new(self.context, self.runtime)
                            .require_resolve_table_or_cte(&reference, resolve_context)
                            .await?;
                        MaybeResolved::Resolved(table, LocationRequirement::ClientLocal)
                    }
                    ResolveMode::Hybrid => {
                        let table = NormalResolver::new(self.context, self.runtime)
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
                                return Err(DbError::new(format!(
                                    "Missing table or view for reference '{reference}'"
                                )));
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
                ast::Expr::Ident(ident) => ScalarValue::Utf8(ident.into_normalized_string().into()),
                other => {
                    return Err(DbError::new(format!(
                        "COPY TO options must be constant, got: {other:?}"
                    )));
                }
            };

            options.insert(key, val);
        }

        not_implemented!("COPY TO")
        // let mut options = CopyToArgs { named: options };

        // let target = match copy_to.target {
        //     ast::CopyToTarget::File(file_name) => {
        //         let func = match options.try_remove_format() {
        //             Some(BorrowedScalarValue::Utf8(format)) => {
        //                 not_implemented!("COPY TO")
        //                 // // User specified a format, lookup in system catalog.
        //                 // let ent = self
        //                 //     .context
        //                 //     .system_catalog()?
        //                 //     .get_schema(self.tx, FUNCTION_LOOKUP_CATALOG)?
        //                 //     .required("function lookup schema")?
        //                 //     .get_copy_to_function_for_format(self.tx, &format)?;

        //                 // match ent {
        //                 //     Some(ent) => ent.try_as_copy_to_function_entry()?.function.clone(),
        //                 //     None => {
        //                 //         return Err(RayexecError::new(format!(
        //                 //             "No registered COPY TO function for format '{format}'"
        //                 //         )))
        //                 //     }
        //                 // }
        //             }
        //             Some(other) => {
        //                 return Err(RayexecError::new(format!(
        //                     "Invalid format expression: {other}"
        //                 )))
        //             }
        //             None => {
        //                 // Infer from file name.
        //                 let handler =
        //                     self.file_handlers.find_match(&file_name).ok_or_else(|| {
        //                         RayexecError::new(format!(
        //                             "No registered file handler for file '{file_name}'"
        //                         ))
        //                     })?;
        //                 handler
        //                     .copy_to
        //                     .as_ref()
        //                     .ok_or_else(|| RayexecError::new("No registered COPY TO function"))?
        //                     .clone()
        //             }
        //         };

        //         FileLocation::parse(&file_name)
        //     }
        // };

        // Ok(ast::CopyTo {
        //     source,
        //     target,
        //     options,
        // })
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
        } else {
            return Err(DbError::new(
                "Persistent tables not yet supported, use CREATE TEMP TABLE",
            ));
        }

        let columns = create
            .columns
            .into_iter()
            .map(|col| {
                Ok(ColumnDef::<ResolvedMeta> {
                    name: col.name,
                    datatype: ast_datatype_to_exec_datatype(col.datatype)?,
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

    async fn resolve_create_view(
        &self,
        create: ast::CreateView<Raw>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::CreateView<ResolvedMeta>> {
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
        } else {
            return Err(DbError::new(
                "Persistent views not yet supported, use CREATE TEMP VIEW",
            ));
        }

        let query = self.resolve_query(create.query, resolve_context).await?;

        Ok(ast::CreateView {
            or_replace: create.or_replace,
            temp: create.temp,
            name,
            column_aliases: create.column_aliases,
            query_sql: create.query_sql,
            query,
        })
    }

    async fn resolve_insert(
        &self,
        insert: ast::Insert<Raw>,
        resolve_context: &mut ResolveContext,
    ) -> Result<ast::Insert<ResolvedMeta>> {
        let table = match self.resolve_mode {
            ResolveMode::Normal => {
                let table = NormalResolver::new(self.context, self.runtime)
                    .require_resolve_table_or_cte(&insert.table, resolve_context)
                    .await?;
                MaybeResolved::Resolved(table, LocationRequirement::ClientLocal)
            }
            ResolveMode::Hybrid => {
                let table = NormalResolver::new(self.context, self.runtime)
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
                        return Err(DbError::new(format!(
                            "Missing table or view for reference '{}'",
                            insert.table
                        )));
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
        async fn resolve_query_inner<R>(
            resolver: &Resolver<'_, R>,
            query: ast::QueryNode<Raw>,
            resolve_context: &mut ResolveContext,
        ) -> Result<ast::QueryNode<ResolvedMeta>>
        where
            R: SystemRuntime,
        {
            let ctes = match query.ctes {
                Some(ctes) => Some(resolver.resolve_ctes(ctes, resolve_context).await?),
                None => None,
            };

            let body =
                Box::pin(resolver.resolve_query_node_body(query.body, resolve_context)).await?;

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
            ast::QueryNodeBody::Describe(describe) => {
                let describe = match *describe {
                    ast::Describe::Query(query) => {
                        ast::Describe::Query(self.resolve_query(query, resolve_context).await?)
                    }
                    ast::Describe::FromNode(from) => {
                        ast::Describe::FromNode(self.resolve_from(from, resolve_context).await?)
                    }
                };
                ast::QueryNodeBody::Describe(Box::new(describe))
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
                ast::DistinctModifier::Distinct => ast::DistinctModifier::Distinct,
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
        // TODO: Very deeply nested... Also rustfmt seems to have trouble
        // properly formatting this.
        // fffffffffffff
        let body = match from.body {
            ast::FromNodeBody::BaseTable(ast::FromBaseTable { reference }) => {
                match self.resolve_mode {
                    ResolveMode::Normal => {
                        let table = NormalResolver::new(self.context, self.runtime)
                            .require_resolve_table_or_cte(&reference, resolve_context)
                            .await?;

                        match table {
                            ResolvedTableOrCteReference::View(ent) => {
                                // Special case for view. If we resolved, then we'll go
                                // ahead and parse the sql and treat it as a subquery.

                                let view = ent.entry.try_as_view_entry()?;

                                let mut statements = parser::parse(&view.query_sql)?;
                                let statement = match statements.len() {
                                    1 => statements.pop().unwrap(),
                                    other => {
                                        return Err(DbError::new(format!(
                                            "Unexpected number of statements inside view body, expected 1, got {other}"
                                        )));
                                    }
                                };

                                let query = match statement {
                                    Statement::Query(query) => {
                                        // TODO: Detect a view referencing itself and error.
                                        Box::pin(self.resolve_query(query, resolve_context)).await?
                                    }
                                    other => {
                                        return Err(DbError::new(format!(
                                            "Unexpected statement type for view: {other:?}"
                                        )));
                                    }
                                };

                                // TODO: We may want to just include the database/schema
                                // on the alias too. Need to see what we're doing for
                                // tables and just do the same here.
                                ast::FromNodeBody::Subquery(ast::FromSubquery {
                                    lateral: false,
                                    options: ResolvedSubqueryOptions::View {
                                        table_alias: TableAlias {
                                            database: None,
                                            schema: None,
                                            table: BinderIdent::from(ent.entry.name.clone()),
                                        },
                                        column_aliases: view
                                            .column_aliases
                                            .clone()
                                            .unwrap_or_default(),
                                    },
                                    query,
                                })
                            }
                            _ => {
                                // Either a table or cte, we can stick these on the
                                // context directly.
                                let idx = resolve_context.tables.push_maybe_resolved(
                                    MaybeResolved::Resolved(
                                        table,
                                        LocationRequirement::ClientLocal,
                                    ),
                                );
                                ast::FromNodeBody::BaseTable(ast::FromBaseTable { reference: idx })
                            }
                        }
                    }
                    ResolveMode::Hybrid => {
                        not_implemented!("resolve table hybrid")
                    }
                }
            }
            ast::FromNodeBody::Subquery(ast::FromSubquery {
                lateral,
                options: (),
                query,
            }) => ast::FromNodeBody::Subquery(ast::FromSubquery {
                lateral,
                options: ResolvedSubqueryOptions::Normal,
                query: Box::pin(self.resolve_query(query, resolve_context)).await?,
            }),
            ast::FromNodeBody::File(ast::FromFilePath { path }) => {
                let function = match self.resolve_mode {
                    ResolveMode::Normal => {
                        let function = NormalResolver::new(self.context, self.runtime)
                            .require_resolve_function_for_path(&path)?;
                        if !function.is_scan_function() {
                            return Err(DbError::new(
                                "Expected scan function when inferring from path",
                            )
                            .with_field("name", function.name.to_string()));
                        }

                        let scan_context = ScanContext {
                            dispatch: self.runtime.filesystem_dispatch(),
                            database_context: self.context,
                        };
                        let planned = expr::bind_table_scan_function(
                            function,
                            scan_context,
                            TableFunctionInput {
                                positional: vec![expr::lit(path.clone()).into()],
                                named: HashMap::new(),
                            },
                        )
                        .await?;

                        MaybeResolved::Resolved(
                            ResolvedTableFunctionReference::Planned(planned),
                            LocationRequirement::ClientLocal,
                        )
                    }
                    ResolveMode::Hybrid => {
                        not_implemented!("infer from path using hybrid resolve")
                    }
                };

                // TODO: Kinda disgusting. Hopefully I can refactor this all
                // soon... I don't even remember if args is required here. _But_
                // it's what the below table function resolve does, so keeping
                // it for consistency.
                let resolve_idx = resolve_context
                    .table_functions
                    .push_maybe_resolved(function);
                ast::FromNodeBody::TableFunction(ast::FromTableFunction {
                    lateral: false,
                    reference: resolve_idx,
                    args: vec![ast::FunctionArg::Unnamed {
                        arg: ast::Expr::Literal(ast::Literal::SingleQuotedString(path)),
                    }],
                })
            }
            ast::FromNodeBody::TableFunction(ast::FromTableFunction {
                lateral,
                reference,
                args,
            }) => {
                let args = Box::pin(
                    ExpressionResolver::new(self).resolve_function_args(args, resolve_context),
                )
                .await?;

                let function = match self.resolve_mode {
                    ResolveMode::Normal => {
                        let function = NormalResolver::new(self.context, self.runtime)
                            .require_resolve_table_function(&reference)?;

                        if function.is_scan_function() {
                            let binder = ConstantBinder::new(resolve_context);
                            let constant_args = binder.bind_constant_function_args(&args)?;

                            let scan_context = ScanContext {
                                dispatch: self.runtime.filesystem_dispatch(),
                                database_context: self.context,
                            };
                            let planned = expr::bind_table_scan_function(
                                function,
                                scan_context,
                                constant_args,
                            )
                            .await?;

                            MaybeResolved::Resolved(
                                ResolvedTableFunctionReference::Planned(planned),
                                LocationRequirement::ClientLocal,
                            )
                        } else {
                            MaybeResolved::Resolved(
                                ResolvedTableFunctionReference::Delayed(function),
                                LocationRequirement::ClientLocal,
                            )
                        }
                    }
                    ResolveMode::Hybrid => {
                        not_implemented!("resolve hybrid table func")
                    }
                };

                let resolve_idx = resolve_context
                    .table_functions
                    .push_maybe_resolved(function);
                ast::FromNodeBody::TableFunction(ast::FromTableFunction {
                    lateral,
                    reference: resolve_idx,
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
}

/// Convert an AST datatype to an internal datatype.
pub fn ast_datatype_to_exec_datatype(datatype: ast::DataType) -> Result<DataType> {
    Ok(match datatype {
        ast::DataType::Varchar(_) => DataType::utf8(),
        ast::DataType::Binary(_) => DataType::binary(),
        ast::DataType::TinyInt => DataType::int8(),
        ast::DataType::SmallInt => DataType::int16(),
        ast::DataType::Integer => DataType::int32(),
        ast::DataType::BigInt => DataType::int64(),
        ast::DataType::UnsignedTinyInt => DataType::uint8(),
        ast::DataType::UnsignedSmallInt => DataType::uint16(),
        ast::DataType::UnsignedInt => DataType::uint32(),
        ast::DataType::UnsignedBigInt => DataType::uint64(),
        ast::DataType::Half => DataType::float16(),
        ast::DataType::Real => DataType::float32(),
        ast::DataType::Double => DataType::float64(),
        ast::DataType::Decimal(prec, scale) => {
            // - Precision cannot be negative.
            // - Specifying just precision defaults to a 0 scale.
            // - Defaults to decimal64 prec and scale if neither provided.
            match prec {
                Some(prec) if prec < 0 => {
                    return Err(DbError::new("Precision cannot be negative"));
                }
                Some(prec) => {
                    let prec: u8 = prec
                        .try_into()
                        .map_err(|_| DbError::new(format!("Precision too high: {prec}")))?;

                    let scale: i8 = match scale {
                        Some(scale) => scale
                            .try_into()
                            .map_err(|_| DbError::new(format!("Scale too high: {scale}")))?,
                        None => 0, // TODO: I'm not sure what behavior we want here, but it seems to match postgres.
                    };

                    if scale as i16 > prec as i16 {
                        return Err(DbError::new(
                            "Decimal scale cannot be larger than precision",
                        ));
                    }

                    if prec <= Decimal64Type::MAX_PRECISION {
                        DataType::decimal64(DecimalTypeMeta::new(prec, scale))
                    } else if prec <= Decimal128Type::MAX_PRECISION {
                        DataType::decimal128(DecimalTypeMeta::new(prec, scale))
                    } else {
                        return Err(DbError::new(
                            "Decimal precision too big for max decimal size",
                        ));
                    }
                }
                None => DataType::decimal64(DecimalTypeMeta::new(
                    Decimal64Type::MAX_PRECISION,
                    Decimal64Type::DEFAULT_SCALE,
                )),
            }
        }
        ast::DataType::Bool => DataType::boolean(),
        ast::DataType::Date => DataType::date32(),
        ast::DataType::Timestamp => {
            // Microsecond matches postgres default.
            DataType::timestamp(TimestampTypeMeta::new(TimeUnit::Microsecond))
        }
        ast::DataType::Interval => DataType::interval(),
    })
}
