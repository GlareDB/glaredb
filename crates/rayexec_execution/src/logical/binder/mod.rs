pub mod bind_data;
pub mod binder_expr;
pub mod bound_cte;
pub mod bound_function;
pub mod bound_table;
pub mod bound_table_function;
pub mod resolve_hybrid;
pub mod resolve_normal;

use std::collections::HashMap;

use bind_data::{BindData, BindListIdx, ItemReference, MaybeBound};
use binder_expr::ExpressionBinder;
use bound_cte::BoundCte;
use bound_table::CteIndex;
use bound_table_function::{BoundTableFunctionReference, UnboundTableFunctionReference};
use rayexec_bullet::{
    datatype::{DataType, DecimalTypeMeta, TimeUnit, TimestampTypeMeta},
    scalar::{
        decimal::{Decimal128Type, Decimal64Type, DecimalType, DECIMAL_DEFUALT_SCALE},
        OwnedScalarValue,
    },
};
use rayexec_error::{RayexecError, Result};
use rayexec_io::location::FileLocation;
use rayexec_parser::{
    ast::{self, ColumnDef, ObjectReference},
    meta::{AstMeta, Raw},
    statement::{RawStatement, Statement},
};
use resolve_normal::Resolver;
use serde::{Deserialize, Serialize};

use crate::{
    database::{catalog::CatalogTx, DatabaseContext},
    datasource::FileHandlers,
    expr::scalar::{BinaryOperator, UnaryOperator},
    functions::{copy::CopyToFunction, table::TableFunctionArgs},
    logical::operator::LocationRequirement,
};

/// An AST statement with references bound to data inside of the `bind_data`.
pub type BoundStatement = Statement<Bound>;

/// Implementation of `AstMeta` which annotates the AST query with
/// tables/functions/etc found in the db.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bound;

impl AstMeta for Bound {
    type DataSourceName = String;
    type ItemReference = ItemReference;
    type TableReference = BindListIdx;
    type TableFunctionReference = BindListIdx;
    // TODO: Having this be the actual table function args does require that we
    // clone them, and the args that go back into the ast don't actually do
    // anything, they're never referenced again.
    type TableFunctionArgs = TableFunctionArgs;
    type CteReference = CteIndex;
    type FunctionReference = BindListIdx;
    type ColumnReference = String;
    type DataType = DataType;
    type CopyToDestination = BoundCopyTo; // TODO: Move this here.
    type BinaryOperator = BinaryOperator;
    type UnaryOperator = UnaryOperator;
}

// TODO: Move func to bind data
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BoundCopyTo {
    pub location: FileLocation,
    // TODO: Remote skip and Option when serializing is figured out.
    #[serde(skip)]
    pub func: Option<Box<dyn CopyToFunction>>,
}

/// Determines the logic taken when encountering an unknown object in a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindMode {
    /// Normal binding, on missing object, return an appropriate error.
    Normal,
    /// Hybrid binding, allow query binding to continue with the assumption that
    /// a remote node will handle anything that's left unbound.
    Hybrid,
}

impl BindMode {
    pub const fn is_hybrid(&self) -> bool {
        matches!(self, BindMode::Hybrid)
    }
}

/// Binds a raw SQL AST with entries in the catalog.
#[derive(Debug)]
pub struct Binder<'a> {
    pub bindmode: BindMode,
    pub tx: &'a CatalogTx,
    pub context: &'a DatabaseContext,
    pub file_handlers: &'a FileHandlers,
}

impl<'a> Binder<'a> {
    pub fn new(
        bindmode: BindMode,
        tx: &'a CatalogTx,
        context: &'a DatabaseContext,
        file_handlers: &'a FileHandlers,
    ) -> Self {
        Binder {
            bindmode,
            tx,
            context,
            file_handlers,
        }
    }

    pub async fn bind_statement(self, stmt: RawStatement) -> Result<(BoundStatement, BindData)> {
        let mut bind_data = BindData::default();
        let bound = match stmt {
            Statement::Explain(explain) => {
                let body = match explain.body {
                    ast::ExplainBody::Query(query) => {
                        ast::ExplainBody::Query(self.bind_query(query, &mut bind_data).await?)
                    }
                };
                Statement::Explain(ast::ExplainNode {
                    analyze: explain.analyze,
                    verbose: explain.verbose,
                    body,
                    output: explain.output,
                })
            }
            Statement::CopyTo(copy_to) => {
                Statement::CopyTo(self.bind_copy_to(copy_to, &mut bind_data).await?)
            }
            Statement::Describe(describe) => match describe {
                ast::Describe::Query(query) => Statement::Describe(ast::Describe::Query(
                    self.bind_query(query, &mut bind_data).await?,
                )),
                ast::Describe::FromNode(from) => Statement::Describe(ast::Describe::FromNode(
                    self.bind_from(from, &mut bind_data).await?,
                )),
            },
            Statement::Query(query) => {
                Statement::Query(self.bind_query(query, &mut bind_data).await?)
            }
            Statement::Insert(insert) => {
                Statement::Insert(self.bind_insert(insert, &mut bind_data).await?)
            }
            Statement::CreateTable(create) => {
                Statement::CreateTable(self.bind_create_table(create, &mut bind_data).await?)
            }
            Statement::CreateSchema(create) => {
                Statement::CreateSchema(self.bind_create_schema(create).await?)
            }
            Statement::Drop(drop) => Statement::Drop(self.bind_drop(drop).await?),
            Statement::SetVariable(set) => Statement::SetVariable(ast::SetVariable {
                reference: Self::reference_to_strings(set.reference).into(),
                value: ExpressionBinder::new(&self)
                    .bind_expression(set.value, &mut bind_data)
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
                Statement::Attach(self.bind_attach(attach, &mut bind_data).await?)
            }
            Statement::Detach(detach) => Statement::Detach(self.bind_detach(detach).await?),
        };

        Ok((bound, bind_data))
    }

    async fn bind_attach(
        &self,
        attach: ast::Attach<Raw>,
        bind_data: &mut BindData,
    ) -> Result<ast::Attach<Bound>> {
        let mut options = HashMap::new();
        for (k, v) in attach.options {
            let v = ExpressionBinder::new(self)
                .bind_expression(v, bind_data)
                .await?;
            options.insert(k, v);
        }

        Ok(ast::Attach {
            datasource_name: attach.datasource_name.into_normalized_string(),
            attach_type: attach.attach_type,
            alias: Self::reference_to_strings(attach.alias).into(),
            options,
        })
    }

    async fn bind_detach(&self, detach: ast::Detach<Raw>) -> Result<ast::Detach<Bound>> {
        // TODO: Replace 'ItemReference' with actual catalog reference. Similar
        // things will happen with Drop.
        Ok(ast::Detach {
            attach_type: detach.attach_type,
            alias: Self::reference_to_strings(detach.alias).into(),
        })
    }

    async fn bind_copy_to(
        &self,
        copy_to: ast::CopyTo<Raw>,
        bind_data: &mut BindData,
    ) -> Result<ast::CopyTo<Bound>> {
        let source = match copy_to.source {
            ast::CopyToSource::Query(query) => {
                ast::CopyToSource::Query(self.bind_query(query, bind_data).await?)
            }
            ast::CopyToSource::Table(reference) => {
                let table = match self.bindmode {
                    BindMode::Normal => {
                        let table = Resolver::new(self.tx, self.context)
                            .require_resolve_table_or_cte(&reference, bind_data)
                            .await?;
                        MaybeBound::Bound(table, LocationRequirement::ClientLocal)
                    }
                    BindMode::Hybrid => {
                        let table = Resolver::new(self.tx, self.context)
                            .resolve_table_or_cte(&reference, bind_data)
                            .await?;

                        match table {
                            Some(table) => {
                                MaybeBound::Bound(table, LocationRequirement::ClientLocal)
                            }
                            None => MaybeBound::Unbound(reference),
                        }
                    }
                };

                let idx = bind_data.tables.push_maybe_bound(table);
                ast::CopyToSource::Table(idx)
            }
        };

        let target = match copy_to.target {
            ast::CopyToTarget::File(file_name) => {
                let handler = self.file_handlers.find_match(&file_name).ok_or_else(|| {
                    RayexecError::new(format!("No registered file handler for file '{file_name}'"))
                })?;
                let func = handler
                    .copy_to
                    .as_ref()
                    .ok_or_else(|| RayexecError::new("No registered COPY TO function"))?
                    .clone();

                BoundCopyTo {
                    location: FileLocation::parse(&file_name),
                    func: Some(func),
                }
            }
        };

        Ok(ast::CopyTo { source, target })
    }

    async fn bind_drop(&self, drop: ast::DropStatement<Raw>) -> Result<ast::DropStatement<Bound>> {
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

    async fn bind_create_schema(
        &self,
        create: ast::CreateSchema<Raw>,
    ) -> Result<ast::CreateSchema<Bound>> {
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

    async fn bind_create_table(
        &self,
        create: ast::CreateTable<Raw>,
        bind_data: &mut BindData,
    ) -> Result<ast::CreateTable<Bound>> {
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
                Ok(ColumnDef::<Bound> {
                    name: col.name.into_normalized_string(),
                    datatype: Self::ast_datatype_to_exec_datatype(col.datatype)?,
                    opts: col.opts,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let source = match create.source {
            Some(source) => Some(self.bind_query(source, bind_data).await?),
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

    async fn bind_insert(
        &self,
        insert: ast::Insert<Raw>,
        bind_data: &mut BindData,
    ) -> Result<ast::Insert<Bound>> {
        let table = match self.bindmode {
            BindMode::Normal => {
                let table = Resolver::new(self.tx, self.context)
                    .require_resolve_table_or_cte(&insert.table, bind_data)
                    .await?;
                MaybeBound::Bound(table, LocationRequirement::ClientLocal)
            }
            BindMode::Hybrid => {
                let table = Resolver::new(self.tx, self.context)
                    .resolve_table_or_cte(&insert.table, bind_data)
                    .await?;

                match table {
                    Some(table) => MaybeBound::Bound(table, LocationRequirement::ClientLocal),
                    None => MaybeBound::Unbound(insert.table),
                }
            }
        };

        let source = self.bind_query(insert.source, bind_data).await?;

        let idx = bind_data.tables.push_maybe_bound(table);

        Ok(ast::Insert {
            table: idx,
            columns: insert.columns,
            source,
        })
    }

    async fn bind_query(
        &self,
        query: ast::QueryNode<Raw>,
        bind_data: &mut BindData,
    ) -> Result<ast::QueryNode<Bound>> {
        /// Helper containing the actual logic for the bind.
        ///
        /// Pulled out so we can accurately set the bind data depth before and
        /// after this.
        async fn bind_query_inner(
            binder: &Binder<'_>,
            query: ast::QueryNode<Raw>,
            bind_data: &mut BindData,
        ) -> Result<ast::QueryNode<Bound>> {
            let ctes = match query.ctes {
                Some(ctes) => Some(binder.bind_ctes(ctes, bind_data).await?),
                None => None,
            };

            let body = binder.bind_query_node_body(query.body, bind_data).await?;

            // Bind ORDER BY
            let mut order_by = Vec::with_capacity(query.order_by.len());
            for expr in query.order_by {
                order_by.push(binder.bind_order_by(expr, bind_data).await?);
            }

            // Bind LIMIT/OFFSET
            let limit = match query.limit.limit {
                Some(expr) => Some(
                    ExpressionBinder::new(binder)
                        .bind_expression(expr, bind_data)
                        .await?,
                ),
                None => None,
            };
            let offset = match query.limit.offset {
                Some(expr) => Some(
                    ExpressionBinder::new(binder)
                        .bind_expression(expr, bind_data)
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

        bind_data.inc_depth();
        let result = bind_query_inner(self, query, bind_data).await;
        bind_data.dec_depth();

        result
    }

    async fn bind_query_node_body(
        &self,
        body: ast::QueryNodeBody<Raw>,
        bind_data: &mut BindData,
    ) -> Result<ast::QueryNodeBody<Bound>> {
        Ok(match body {
            ast::QueryNodeBody::Select(select) => {
                ast::QueryNodeBody::Select(Box::new(self.bind_select(*select, bind_data).await?))
            }
            ast::QueryNodeBody::Nested(nested) => ast::QueryNodeBody::Nested(Box::new(
                Box::pin(self.bind_query(*nested, bind_data)).await?,
            )),
            ast::QueryNodeBody::Values(values) => {
                ast::QueryNodeBody::Values(self.bind_values(values, bind_data).await?)
            }
            ast::QueryNodeBody::Set {
                left,
                right,
                operation,
                all,
            } => {
                let left = Box::pin(self.bind_query_node_body(*left, bind_data)).await?;
                let right = Box::pin(self.bind_query_node_body(*right, bind_data)).await?;
                ast::QueryNodeBody::Set {
                    left: Box::new(left),
                    right: Box::new(right),
                    operation,
                    all,
                }
            }
        })
    }

    async fn bind_ctes(
        &self,
        ctes: ast::CommonTableExprDefs<Raw>,
        bind_data: &mut BindData,
    ) -> Result<ast::CommonTableExprDefs<Bound>> {
        let mut bound_refs = Vec::with_capacity(ctes.ctes.len());
        for cte in ctes.ctes.into_iter() {
            let depth = bind_data.current_depth;

            let bound_body = Box::pin(self.bind_query(*cte.body, bind_data)).await?;
            let bound_cte = BoundCte {
                name: cte.alias.into_normalized_string(),
                depth,
                column_aliases: cte.column_aliases,
                body: bound_body,
                materialized: cte.materialized,
            };

            let bound_ref = bind_data.push_cte(bound_cte);
            bound_refs.push(bound_ref);
        }

        Ok(ast::CommonTableExprDefs {
            recursive: ctes.recursive,
            ctes: bound_refs,
        })
    }

    async fn bind_select(
        &self,
        select: ast::SelectNode<Raw>,
        bind_data: &mut BindData,
    ) -> Result<ast::SelectNode<Bound>> {
        // Bind DISTINCT
        let distinct = match select.distinct {
            Some(distinct) => Some(match distinct {
                ast::DistinctModifier::On(exprs) => {
                    let mut bound = Vec::with_capacity(exprs.len());
                    for expr in exprs {
                        bound.push(
                            ExpressionBinder::new(self)
                                .bind_expression(expr, bind_data)
                                .await?,
                        );
                    }
                    ast::DistinctModifier::On(bound)
                }
                ast::DistinctModifier::All => ast::DistinctModifier::All,
            }),
            None => None,
        };

        // Bind FROM
        let from = match select.from {
            Some(from) => Some(self.bind_from(from, bind_data).await?),
            None => None,
        };

        // Bind WHERE
        let where_expr = match select.where_expr {
            Some(expr) => Some(
                ExpressionBinder::new(self)
                    .bind_expression(expr, bind_data)
                    .await?,
            ),
            None => None,
        };

        // Bind SELECT list
        let mut projections = Vec::with_capacity(select.projections.len());
        for projection in select.projections {
            projections.push(
                ExpressionBinder::new(self)
                    .bind_select_expr(projection, bind_data)
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
                        bound.push(
                            ExpressionBinder::new(self)
                                .bind_group_by_expr(expr, bind_data)
                                .await?,
                        );
                    }
                    ast::GroupByNode::Exprs { exprs: bound }
                }
            }),
            None => None,
        };

        // Bind HAVING
        let having = match select.having {
            Some(expr) => Some(
                ExpressionBinder::new(self)
                    .bind_expression(expr, bind_data)
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

    async fn bind_values(
        &self,
        values: ast::Values<Raw>,
        bind_data: &mut BindData,
    ) -> Result<ast::Values<Bound>> {
        let mut bound = Vec::with_capacity(values.rows.len());
        for row in values.rows {
            bound.push(
                ExpressionBinder::new(self)
                    .bind_expressions(row, bind_data)
                    .await?,
            );
        }
        Ok(ast::Values { rows: bound })
    }

    async fn bind_order_by(
        &self,
        order_by: ast::OrderByNode<Raw>,
        bind_data: &mut BindData,
    ) -> Result<ast::OrderByNode<Bound>> {
        let expr = ExpressionBinder::new(self)
            .bind_expression(order_by.expr, bind_data)
            .await?;
        Ok(ast::OrderByNode {
            typ: order_by.typ,
            nulls: order_by.nulls,
            expr,
        })
    }

    async fn bind_from(
        &self,
        from: ast::FromNode<Raw>,
        bind_data: &mut BindData,
    ) -> Result<ast::FromNode<Bound>> {
        let body = match from.body {
            ast::FromNodeBody::BaseTable(ast::FromBaseTable { reference }) => {
                let table = match self.bindmode {
                    BindMode::Normal => {
                        let table = Resolver::new(self.tx, self.context)
                            .require_resolve_table_or_cte(&reference, bind_data)
                            .await?;
                        MaybeBound::Bound(table, LocationRequirement::ClientLocal)
                    }
                    BindMode::Hybrid => {
                        let table = Resolver::new(self.tx, self.context)
                            .resolve_table_or_cte(&reference, bind_data)
                            .await?;

                        match table {
                            Some(table) => {
                                MaybeBound::Bound(table, LocationRequirement::ClientLocal)
                            }
                            None => MaybeBound::Unbound(reference),
                        }
                    }
                };

                let idx = bind_data.tables.push_maybe_bound(table);
                ast::FromNodeBody::BaseTable(ast::FromBaseTable { reference: idx })
            }
            ast::FromNodeBody::Subquery(ast::FromSubquery { query }) => {
                ast::FromNodeBody::Subquery(ast::FromSubquery {
                    query: Box::pin(self.bind_query(query, bind_data)).await?,
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

                        let bind_idx = bind_data.table_functions.push_bound(
                            BoundTableFunctionReference { name, func },
                            LocationRequirement::ClientLocal,
                        );

                        ast::FromNodeBody::TableFunction(ast::FromTableFunction {
                            reference: bind_idx,
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
                let args = ExpressionBinder::new(self)
                    .bind_table_function_args(args)
                    .await?;

                let function = match self.bindmode {
                    BindMode::Normal => {
                        let function = Resolver::new(self.tx, self.context)
                            .require_resolve_table_function(&reference)?;
                        let function = function
                            .plan_and_initialize(self.context, args.clone())
                            .await?;

                        MaybeBound::Bound(
                            BoundTableFunctionReference {
                                name: function.table_function().name().to_string(),
                                func: function,
                            },
                            LocationRequirement::ClientLocal,
                        )
                    }
                    BindMode::Hybrid => {
                        match Resolver::new(self.tx, self.context)
                            .resolve_table_function(&reference)?
                        {
                            Some(function) => {
                                let function = function
                                    .plan_and_initialize(self.context, args.clone())
                                    .await?;
                                MaybeBound::Bound(
                                    BoundTableFunctionReference {
                                        name: function.table_function().name().to_string(),
                                        func: function,
                                    },
                                    LocationRequirement::ClientLocal,
                                )
                            }
                            None => MaybeBound::Unbound(UnboundTableFunctionReference {
                                reference,
                                args: args.clone(),
                            }),
                        }
                    }
                };

                let bind_idx = bind_data.table_functions.push_maybe_bound(function);
                ast::FromNodeBody::TableFunction(ast::FromTableFunction {
                    reference: bind_idx,
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
                let left = Box::pin(self.bind_from(*left, bind_data)).await?;
                let right = Box::pin(self.bind_from(*right, bind_data)).await?;

                let join_condition = match join_condition {
                    ast::JoinCondition::On(expr) => {
                        let expr = ExpressionBinder::new(self)
                            .bind_expression(expr, bind_data)
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
                let scale: i8 = match scale {
                    Some(scale) => scale
                        .try_into()
                        .map_err(|_| RayexecError::new(format!("Scale too high: {scale}")))?,
                    None if prec.is_some() => 0, // TODO: I'm not sure what behavior we want here, but it seems to match postgres.
                    None => DECIMAL_DEFUALT_SCALE,
                };

                let prec: u8 = match prec {
                    Some(prec) if prec < 0 => {
                        return Err(RayexecError::new("Precision cannot be negative"))
                    }
                    Some(prec) => prec
                        .try_into()
                        .map_err(|_| RayexecError::new(format!("Precision too high: {prec}")))?,
                    None => Decimal64Type::MAX_PRECISION,
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
