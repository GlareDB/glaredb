use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use rayexec_bullet::{
    datatype::{DataType, DecimalTypeMeta, TimeUnit, TimestampTypeMeta},
    scalar::{
        decimal::{Decimal128Type, Decimal64Type, DecimalType, DECIMAL_DEFUALT_SCALE},
        OwnedScalarValue,
    },
};
use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_parser::{
    ast::{self, ColumnDef, FunctionArg, ObjectReference, QueryNode, ReplaceColumn},
    meta::{AstMeta, Raw},
    statement::{RawStatement, Statement},
};

use crate::{
    database::{catalog::CatalogTx, entry::TableEntry, DatabaseContext},
    datasource::FileHandlers,
    functions::{
        aggregate::AggregateFunction,
        scalar::ScalarFunction,
        table::{GenericTableFunction, InitializedTableFunction, TableFunctionArgs},
    },
    logical::sql::expr::ExpressionContext,
    runtime::ExecutionRuntime,
};

pub type BoundStatement = Statement<Bound>;

/// Implementation of `AstMeta` which annotates the AST query with
/// tables/functions/etc found in the db.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bound;

impl AstMeta for Bound {
    type DataSourceName = String;
    type ItemReference = BoundItemReference;
    type TableReference = BoundTableOrCteReference;
    type TableFunctionReference = BoundTableFunctionReference;
    type TableFunctionArguments = TableFunctionArgs;
    type CteReference = BoundCteReference;
    type FunctionReference = BoundFunctionReference;
    type ColumnReference = String;
    type DataType = DataType;
}

/// Represents a bound function found in the select list, where clause, or order
/// by/group by clause.
// TODO: If we want to support table functions in the select list, this will
// need to be extended.
#[derive(Debug, Clone, PartialEq)]
pub enum BoundFunctionReference {
    Scalar(Box<dyn ScalarFunction>),
    Aggregate(Box<dyn AggregateFunction>),
}

impl BoundFunctionReference {
    pub fn name(&self) -> &str {
        match self {
            Self::Scalar(scalar) => scalar.name(),
            Self::Aggregate(agg) => agg.name(),
        }
    }
}

/// References a CTE that can be found in `BindData`.
///
/// Note that this doesn't hold the CTE itself since it may be referenced more
/// than once in a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BoundCteReference {
    /// Index into the CTE map.
    pub idx: usize,
}

/// Table or CTE found in the FROM clause.
#[derive(Debug, Clone, PartialEq)]
pub enum BoundTableOrCteReference {
    Table {
        catalog: String,
        schema: String,
        entry: TableEntry,
    },
    Cte(BoundCteReference),
}

/// Table function found in the FROM clause.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundTableFunctionReference {
    /// Name of the original function.
    ///
    /// This is used to allow the user to reference the output of the function
    /// if not provided an alias.
    pub name: String,

    /// The initialized table function.
    pub func: Box<dyn InitializedTableFunction>,
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

// TODO: This might need some scoping information.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundCte {
    /// Normalized name for the CTE.
    pub name: String,

    /// Depth this CTE was found at.
    pub depth: usize,

    /// Column aliases taken directly from the ast.
    pub column_aliases: Option<Vec<ast::Ident>>,

    /// The bound query node.
    pub body: QueryNode<Bound>,

    pub materialized: bool,
}

#[derive(Debug, Default, PartialEq)]
pub struct BindData {
    /// How "deep" in the plan are we.
    ///
    /// Incremented everytime we dive into a subquery.
    ///
    /// This provides a primitive form of scoping for CTE resolution.
    pub current_depth: usize,

    /// CTEs are appended to the vec as they're encountered.
    ///
    /// When search for a CTE, the vec should be iterated from right to left to
    /// try to get the "closest" CTE to the reference.
    pub ctes: Vec<BoundCte>,
}

impl BindData {
    /// Try to find a CTE by its normalized name.
    ///
    /// This will iterate the cte vec right to left to find best cte that
    /// matches this name.
    ///
    /// The current depth will be used to determine if a CTE is valid to
    /// reference or not. What this means is as we iterate, we can go "up" in
    /// depth, but never back down, as going back down would mean we're
    /// attempting to resolve a cte from a "sibling" subquery.
    // TODO: This doesn't account for CTEs defined in sibling subqueries yet
    // that happen to have the same name and depths _and_ there's no CTEs in the
    // parent.
    fn find_cte(&self, name: &str) -> Option<BoundCteReference> {
        let mut search_depth = self.current_depth;

        for (idx, cte) in self.ctes.iter().rev().enumerate() {
            if cte.depth > search_depth {
                // We're looking another subquery's CTEs.
                return None;
            }

            if cte.name == name {
                // We found a good reference.
                return Some(BoundCteReference {
                    idx: (self.ctes.len() - 1) - idx, // Since we're iterating backwards.
                });
            }

            // Otherwise keep searching, even if the cte is up a level.
            search_depth = cte.depth;
        }

        // No CTE found.
        None
    }

    fn inc_depth(&mut self) {
        self.current_depth += 1
    }

    fn dec_depth(&mut self) {
        self.current_depth -= 1;
    }

    /// Push a CTE into bind data, returning a CTE reference.
    fn push_cte(&mut self, cte: BoundCte) -> BoundCteReference {
        let idx = self.ctes.len();
        self.ctes.push(cte);
        BoundCteReference { idx }
    }
}

/// Binds a raw SQL AST with entries in the catalog.
#[derive(Debug)]
pub struct Binder<'a> {
    tx: &'a CatalogTx,
    context: &'a DatabaseContext,
    file_handlers: &'a FileHandlers,
    runtime: &'a Arc<dyn ExecutionRuntime>,
}

impl<'a> Binder<'a> {
    pub fn new(
        tx: &'a CatalogTx,
        context: &'a DatabaseContext,
        file_handlers: &'a FileHandlers,
        runtime: &'a Arc<dyn ExecutionRuntime>,
    ) -> Self {
        Binder {
            tx,
            context,
            file_handlers,
            runtime,
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

    async fn bind_drop(&self, drop: ast::DropStatement<Raw>) -> Result<ast::DropStatement<Bound>> {
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
        &self,
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
        &self,
        create: ast::CreateTable<Raw>,
        bind_data: &mut BindData,
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
        let table = self.resolve_table_or_cte(insert.table, bind_data).await?;
        let source = self.bind_query(insert.source, bind_data).await?;
        Ok(ast::Insert {
            table,
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
                ast::FromNodeBody::BaseTable(ast::FromBaseTable {
                    reference: self.resolve_table_or_cte(reference, bind_data).await?,
                })
            }
            ast::FromNodeBody::Subquery(ast::FromSubquery { query }) => {
                ast::FromNodeBody::Subquery(ast::FromSubquery {
                    query: Box::pin(self.bind_query(query, bind_data)).await?,
                })
            }
            ast::FromNodeBody::File(ast::FromFilePath { path }) => {
                match self.file_handlers.find_match(&path) {
                    Some(func) => {
                        let args = TableFunctionArgs {
                            named: HashMap::new(),
                            positional: vec![OwnedScalarValue::Utf8(path.into())],
                        };

                        let name = func.name().to_string();
                        let func = func
                            .specialize(args.clone())?
                            .initialize(self.runtime)
                            .await?;

                        ast::FromNodeBody::TableFunction(ast::FromTableFunction {
                            reference: BoundTableFunctionReference { name, func },
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
                let table_fn = self.resolve_table_function(reference).await?;
                let args = ExpressionBinder::new(self)
                    .bind_table_function_args(args)
                    .await?;

                let name = table_fn.name().to_string();
                let func = table_fn
                    .specialize(args.clone())?
                    .initialize(self.runtime)
                    .await?;

                ast::FromNodeBody::TableFunction(ast::FromTableFunction {
                    reference: BoundTableFunctionReference { name, func },
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

    async fn resolve_table_or_cte(
        &self,
        mut reference: ast::ObjectReference,
        bind_data: &BindData,
    ) -> Result<BoundTableOrCteReference> {
        // TODO: Seach path.
        let [catalog, schema, table] = match reference.0.len() {
            1 => {
                let name = reference.0.pop().unwrap().into_normalized_string();

                // Check bind data for cte that would satisfy this reference.
                if let Some(cte) = bind_data.find_cte(&name) {
                    return Ok(BoundTableOrCteReference::Cte(cte));
                }

                // Other wise continue with trying to resolve from the catalogs.
                ["temp".to_string(), "temp".to_string(), name]
            }
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

    async fn resolve_table_function(
        &self,
        mut reference: ast::ObjectReference,
    ) -> Result<Box<dyn GenericTableFunction>> {
        // TODO: Search path.
        let [catalog, schema, name] = match reference.0.len() {
            1 => [
                "system".to_string(),
                "glare_catalog".to_string(),
                reference.0.pop().unwrap().into_normalized_string(),
            ],
            2 => {
                let name = reference.0.pop().unwrap().into_normalized_string();
                let schema = reference.0.pop().unwrap().into_normalized_string();
                ["system".to_string(), schema, name]
            }
            3 => {
                let name = reference.0.pop().unwrap().into_normalized_string();
                let schema = reference.0.pop().unwrap().into_normalized_string();
                let catalog = reference.0.pop().unwrap().into_normalized_string();
                [catalog, schema, name]
            }
            _ => {
                return Err(RayexecError::new(
                    "Unexpected number of identifiers in table function reference",
                ))
            }
        };

        if let Some(entry) = self
            .context
            .get_catalog(&catalog)?
            .get_table_fn(self.tx, &schema, &name)
            .await?
        {
            Ok(entry)
        } else {
            Err(RayexecError::new(format!(
                "Unable to find table function '{catalog}.{schema}.{name}'"
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
        bind_data: &mut BindData,
    ) -> Result<ast::SelectExpr<Bound>> {
        match select_expr {
            ast::SelectExpr::Expr(expr) => Ok(ast::SelectExpr::Expr(
                self.bind_expression(expr, bind_data).await?,
            )),
            ast::SelectExpr::AliasedExpr(expr, alias) => Ok(ast::SelectExpr::AliasedExpr(
                self.bind_expression(expr, bind_data).await?,
                alias,
            )),
            ast::SelectExpr::QualifiedWildcard(object_name, wildcard) => {
                Ok(ast::SelectExpr::QualifiedWildcard(
                    object_name,
                    self.bind_wildcard(wildcard, bind_data).await?,
                ))
            }
            ast::SelectExpr::Wildcard(wildcard) => Ok(ast::SelectExpr::Wildcard(
                self.bind_wildcard(wildcard, bind_data).await?,
            )),
        }
    }

    async fn bind_wildcard(
        &self,
        wildcard: ast::Wildcard<Raw>,
        bind_data: &mut BindData,
    ) -> Result<ast::Wildcard<Bound>> {
        let mut replace_cols = Vec::with_capacity(wildcard.replace_cols.len());
        for replace in wildcard.replace_cols {
            replace_cols.push(ReplaceColumn {
                col: replace.col,
                expr: self.bind_expression(replace.expr, bind_data).await?,
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
        bind_data: &mut BindData,
    ) -> Result<ast::GroupByExpr<Bound>> {
        Ok(match expr {
            ast::GroupByExpr::Expr(exprs) => {
                ast::GroupByExpr::Expr(self.bind_expressions(exprs, bind_data).await?)
            }
            ast::GroupByExpr::Cube(exprs) => {
                ast::GroupByExpr::Cube(self.bind_expressions(exprs, bind_data).await?)
            }
            ast::GroupByExpr::Rollup(exprs) => {
                ast::GroupByExpr::Rollup(self.bind_expressions(exprs, bind_data).await?)
            }
            ast::GroupByExpr::GroupingSets(exprs) => {
                ast::GroupByExpr::GroupingSets(self.bind_expressions(exprs, bind_data).await?)
            }
        })
    }

    /// Binds functions arguments for a table function.
    ///
    /// Slightly different from normal argument binding since arguments to a
    /// table function are more restrictive. E.g. we only allow literals as
    /// arguments.
    ///
    /// Note in the future we could allow more complex expressions as arguments,
    /// and we could support table function that accept columns as inputs.
    async fn bind_table_function_args(
        &self,
        args: Vec<FunctionArg<Raw>>,
    ) -> Result<TableFunctionArgs> {
        let bind_data = &mut BindData::default(); // Empty bind data since we don't allow complex expressions.

        let mut named = HashMap::new();
        let mut positional = Vec::new();

        for func_arg in args {
            match func_arg {
                ast::FunctionArg::Named { name, arg } => {
                    let name = name.into_normalized_string();
                    let arg = match arg {
                        ast::FunctionArgExpr::Wildcard => {
                            return Err(RayexecError::new(
                                "Cannot use '*' as an argument to a table function",
                            ))
                        }
                        ast::FunctionArgExpr::Expr(expr) => {
                            match Box::pin(self.bind_expression(expr, bind_data)).await? {
                                ast::Expr::Literal(lit) => {
                                    ExpressionContext::plan_literal(lit)?.try_into_scalar()?
                                }
                                other => {
                                    return Err(RayexecError::new(format!(
                                        "Table function arguments must be constant, got {other:?}"
                                    )))
                                }
                            }
                        }
                    };

                    if named.contains_key(&name) {
                        return Err(RayexecError::new(format!("Duplicate argument: {name}")));
                    }
                    named.insert(name, arg);
                }
                FunctionArg::Unnamed { arg } => {
                    let arg = match arg {
                        ast::FunctionArgExpr::Wildcard => {
                            return Err(RayexecError::new(
                                "Cannot use '*' as an argument to a table function",
                            ))
                        }
                        ast::FunctionArgExpr::Expr(expr) => {
                            match Box::pin(self.bind_expression(expr, bind_data)).await? {
                                ast::Expr::Literal(lit) => {
                                    ExpressionContext::plan_literal(lit)?.try_into_scalar()?
                                }
                                other => {
                                    return Err(RayexecError::new(format!(
                                        "Table function arguments must be constant, got {other:?}"
                                    )))
                                }
                            }
                        }
                    };
                    positional.push(arg);
                }
            }
        }

        Ok(TableFunctionArgs { named, positional })
    }

    async fn bind_expressions(
        &self,
        exprs: impl IntoIterator<Item = ast::Expr<Raw>>,
        bind_data: &mut BindData,
    ) -> Result<Vec<ast::Expr<Bound>>> {
        let mut bound = Vec::new();
        for expr in exprs {
            bound.push(self.bind_expression(expr, bind_data).await?);
        }
        Ok(bound)
    }

    /// Bind an expression.
    async fn bind_expression(
        &self,
        expr: ast::Expr<Raw>,
        bind_data: &mut BindData,
    ) -> Result<ast::Expr<Bound>> {
        match expr {
            ast::Expr::Ident(ident) => Ok(ast::Expr::Ident(ident)),
            ast::Expr::CompoundIdent(idents) => Ok(ast::Expr::CompoundIdent(idents)),
            ast::Expr::Literal(lit) => Ok(ast::Expr::Literal(match lit {
                ast::Literal::Number(s) => ast::Literal::Number(s),
                ast::Literal::SingleQuotedString(s) => ast::Literal::SingleQuotedString(s),
                ast::Literal::Boolean(b) => ast::Literal::Boolean(b),
                ast::Literal::Null => ast::Literal::Null,
                ast::Literal::Struct { keys, values } => {
                    let bound = Box::pin(self.bind_expressions(values, bind_data)).await?;
                    ast::Literal::Struct {
                        keys,
                        values: bound,
                    }
                }
            })),
            ast::Expr::Array(arr) => {
                let mut new_arr = Vec::with_capacity(arr.len());
                for v in arr {
                    let new_v = Box::pin(self.bind_expression(v, bind_data)).await?;
                    new_arr.push(new_v);
                }
                Ok(ast::Expr::Array(new_arr))
            }
            ast::Expr::ArraySubscript { expr, subscript } => {
                let expr = Box::pin(self.bind_expression(*expr, bind_data)).await?;
                let subscript = match *subscript {
                    ast::ArraySubscript::Index(index) => ast::ArraySubscript::Index(
                        Box::pin(self.bind_expression(index, bind_data)).await?,
                    ),
                    ast::ArraySubscript::Slice {
                        lower,
                        upper,
                        stride,
                    } => {
                        let lower = match lower {
                            Some(lower) => {
                                Some(Box::pin(self.bind_expression(lower, bind_data)).await?)
                            }
                            None => None,
                        };
                        let upper = match upper {
                            Some(upper) => {
                                Some(Box::pin(self.bind_expression(upper, bind_data)).await?)
                            }
                            None => None,
                        };
                        let stride = match stride {
                            Some(stride) => {
                                Some(Box::pin(self.bind_expression(stride, bind_data)).await?)
                            }
                            None => None,
                        };

                        ast::ArraySubscript::Slice {
                            lower,
                            upper,
                            stride,
                        }
                    }
                };

                Ok(ast::Expr::ArraySubscript {
                    expr: Box::new(expr),
                    subscript: Box::new(subscript),
                })
            }
            ast::Expr::UnaryExpr { op, expr } => match (op, *expr) {
                (ast::UnaryOperator::Minus, ast::Expr::Literal(ast::Literal::Number(n))) => {
                    Ok(ast::Expr::Literal(ast::Literal::Number(format!("-{n}"))))
                }
                (_, expr) => Ok(ast::Expr::UnaryExpr {
                    op,
                    expr: Box::new(Box::pin(self.bind_expression(expr, bind_data)).await?),
                }),
            },
            ast::Expr::BinaryExpr { left, op, right } => Ok(ast::Expr::BinaryExpr {
                left: Box::new(Box::pin(self.bind_expression(*left, bind_data)).await?),
                op,
                right: Box::new(Box::pin(self.bind_expression(*right, bind_data)).await?),
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
                    Some(filter) => Some(Box::new(
                        Box::pin(self.bind_expression(*filter, bind_data)).await?,
                    )),
                    None => None,
                };

                let mut args = Vec::with_capacity(func.args.len());
                // TODO: This current rewrites '*' function arguments to 'true'.
                // This is for 'count(*)'. What we should be doing is rewriting
                // 'count(*)' to 'count_star()' and have a function
                // implementation for 'count_star'.
                //
                // No other function accepts a '*' (I think).
                for func_arg in func.args {
                    let func_arg = match func_arg {
                        ast::FunctionArg::Named { name, arg } => ast::FunctionArg::Named {
                            name,
                            arg: match arg {
                                ast::FunctionArgExpr::Wildcard => ast::FunctionArgExpr::Expr(
                                    ast::Expr::Literal(ast::Literal::Boolean(true)),
                                ),
                                ast::FunctionArgExpr::Expr(expr) => ast::FunctionArgExpr::Expr(
                                    Box::pin(self.bind_expression(expr, bind_data)).await?,
                                ),
                            },
                        },
                        ast::FunctionArg::Unnamed { arg } => ast::FunctionArg::Unnamed {
                            arg: match arg {
                                ast::FunctionArgExpr::Wildcard => ast::FunctionArgExpr::Expr(
                                    ast::Expr::Literal(ast::Literal::Boolean(true)),
                                ),
                                ast::FunctionArgExpr::Expr(expr) => ast::FunctionArgExpr::Expr(
                                    Box::pin(self.bind_expression(expr, bind_data)).await?,
                                ),
                            },
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
            ast::Expr::Subquery(subquery) => {
                let bound = Box::pin(self.binder.bind_query(*subquery, bind_data)).await?;
                Ok(ast::Expr::Subquery(Box::new(bound)))
            }
            ast::Expr::Exists {
                subquery,
                not_exists,
            } => {
                let bound = Box::pin(self.binder.bind_query(*subquery, bind_data)).await?;
                Ok(ast::Expr::Exists {
                    subquery: Box::new(bound),
                    not_exists,
                })
            }
            ast::Expr::TypedString { datatype, value } => {
                let datatype = Binder::ast_datatype_to_exec_datatype(datatype)?;
                Ok(ast::Expr::TypedString { datatype, value })
            }
            ast::Expr::Cast { datatype, expr } => {
                let expr = Box::pin(self.bind_expression(*expr, bind_data)).await?;
                let datatype = Binder::ast_datatype_to_exec_datatype(datatype)?;
                Ok(ast::Expr::Cast {
                    datatype,
                    expr: Box::new(expr),
                })
            }
            ast::Expr::Nested(expr) => {
                let expr = Box::pin(self.bind_expression(*expr, bind_data)).await?;
                Ok(ast::Expr::Nested(Box::new(expr)))
            }
            ast::Expr::Interval(ast::Interval {
                value,
                leading,
                trailing,
            }) => {
                let value = Box::pin(self.bind_expression(*value, bind_data)).await?;
                Ok(ast::Expr::Interval(ast::Interval {
                    value: Box::new(value),
                    leading,
                    trailing,
                }))
            }
            ast::Expr::Like {
                not_like,
                case_insensitive,
                expr,
                pattern,
            } => {
                let expr = Box::pin(self.bind_expression(*expr, bind_data)).await?;
                let pattern = Box::pin(self.bind_expression(*pattern, bind_data)).await?;
                Ok(ast::Expr::Like {
                    not_like,
                    case_insensitive,
                    expr: Box::new(expr),
                    pattern: Box::new(pattern),
                })
            }
            other => not_implemented!("bind expr {other:?}"),
        }
    }
}
