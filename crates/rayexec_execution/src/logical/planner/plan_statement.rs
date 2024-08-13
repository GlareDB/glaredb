use std::collections::HashMap;

use super::scope::Scope;
use crate::{
    database::{
        create::OnConflict,
        drop::{DropInfo, DropObject},
    },
    engine::vars::SessionVars,
    logical::{
        binder::{bind_data::BindData, bound_table::BoundTableOrCteReference, Bound},
        context::QueryContext,
        expr::LogicalExpression,
        operator::{
            AttachDatabase, CopyTo, CreateSchema, CreateTable, Describe, DetachDatabase, DropEntry,
            Explain, ExplainFormat, Insert, LogicalNode, LogicalOperator, Projection, ResetVar,
            Scan, SetVar, ShowVar, VariableOrAll,
        },
        planner::{plan_expr::ExpressionContext, plan_query::QueryNodePlanner},
    },
};
use rayexec_bullet::field::{Field, Schema, TypeSchema};
use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_parser::{ast, statement::Statement};
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

impl LogicalQuery {
    pub fn schema(&self) -> Result<Schema> {
        let type_schema = self.root.output_schema(&[])?;
        debug_assert_eq!(self.scope.num_columns(), type_schema.types.len());

        let schema = Schema::new(
            self.scope
                .items
                .iter()
                .zip(type_schema.types)
                .map(|(item, typ)| Field::new(item.column.clone(), typ, true)),
        );

        Ok(schema)
    }
}

#[derive(Debug, Clone)]
pub struct PlanContext<'a> {
    /// Session variables.
    pub vars: &'a SessionVars,
    pub bind_data: &'a BindData,
}

impl<'a> PlanContext<'a> {
    pub fn new(vars: &'a SessionVars, bind_data: &'a BindData) -> Self {
        PlanContext { vars, bind_data }
    }

    pub fn plan_statement(
        mut self,
        stmt: Statement<Bound>,
    ) -> Result<(LogicalQuery, QueryContext)> {
        trace!("planning statement");
        let mut context = QueryContext::new();
        let query = match stmt {
            Statement::Explain(explain) => {
                let mut planner = QueryNodePlanner::new(self.bind_data);
                let plan = match explain.body {
                    ast::ExplainBody::Query(query) => planner.plan_query(&mut context, query)?,
                };
                let format = match explain.output {
                    Some(ast::ExplainOutput::Text) => ExplainFormat::Text,
                    Some(ast::ExplainOutput::Json) => ExplainFormat::Json,
                    None => ExplainFormat::Text,
                };
                LogicalQuery {
                    root: LogicalOperator::Explain(LogicalNode::new(Explain {
                        analyze: explain.analyze,
                        verbose: explain.verbose,
                        format,
                        input: Box::new(plan.root),
                    })),
                    scope: Scope::with_columns(None, ["plan_type", "plan"]),
                }
            }
            Statement::Query(query) => {
                let mut planner = QueryNodePlanner::new(self.bind_data);
                planner.plan_query(&mut context, query)?
            }
            Statement::CopyTo(copy_to) => self.plan_copy_to(&mut context, copy_to)?,
            Statement::CreateTable(create) => self.plan_create_table(&mut context, create)?,
            Statement::CreateSchema(create) => self.plan_create_schema(create)?,
            Statement::Drop(drop) => self.plan_drop(drop)?,
            Statement::Insert(insert) => self.plan_insert(&mut context, insert)?,
            Statement::SetVariable(ast::SetVariable {
                mut reference,
                value,
            }) => {
                let planner = QueryNodePlanner::new(self.bind_data);
                let expr_ctx = ExpressionContext::new(&planner, EMPTY_SCOPE, EMPTY_TYPE_SCHEMA);
                let expr = expr_ctx.plan_expression(&mut context, value)?;
                LogicalQuery {
                    root: LogicalOperator::SetVar(LogicalNode::new(SetVar {
                        name: reference.pop()?, // TODO: Allow compound references?
                        value: expr.try_into_scalar()?,
                    })),
                    scope: Scope::empty(),
                }
            }
            Statement::ShowVariable(ast::ShowVariable { mut reference }) => {
                let name = reference.pop()?; // TODO: Allow compound references?
                let var = self.vars.get_var(&name)?;
                let scope = Scope::with_columns(None, [name.clone()]);
                LogicalQuery {
                    root: LogicalOperator::ShowVar(LogicalNode::new(ShowVar { var: var.clone() })),
                    scope,
                }
            }
            Statement::ResetVariable(ast::ResetVariable { var }) => {
                let var = match var {
                    ast::VariableOrAll::Variable(mut v) => {
                        let name = v.pop()?; // TODO: Allow compound references?
                        let var = self.vars.get_var(&name)?;
                        VariableOrAll::Variable(var.clone())
                    }
                    ast::VariableOrAll::All => VariableOrAll::All,
                };
                LogicalQuery {
                    root: LogicalOperator::ResetVar(LogicalNode::new(ResetVar { var })),
                    scope: Scope::empty(),
                }
            }
            Statement::Attach(attach) => self.plan_attach(attach)?,
            Statement::Detach(detach) => self.plan_detach(detach)?,
            Statement::Describe(describe) => {
                let mut planner = QueryNodePlanner::new(self.bind_data);
                let plan = match describe {
                    ast::Describe::Query(query) => planner.plan_query(&mut context, query)?,
                    ast::Describe::FromNode(from) => planner.plan_from_node(
                        &mut context,
                        from,
                        TypeSchema::empty(),
                        Scope::empty(),
                    )?,
                };

                let type_schema = plan.root.output_schema(&[])?; // TODO: Include outer schema
                debug_assert_eq!(plan.scope.num_columns(), type_schema.types.len());

                let schema = Schema::new(
                    plan.scope
                        .items
                        .into_iter()
                        .zip(type_schema.types)
                        .map(|(item, typ)| Field::new(item.column, typ, true)),
                );

                LogicalQuery {
                    root: LogicalOperator::Describe(LogicalNode::new(Describe { schema })),
                    scope: Scope::with_columns(None, ["column_name", "datatype"]),
                }
            }
        };

        Ok((query, context))
    }

    fn plan_attach(&mut self, mut attach: ast::Attach<Bound>) -> Result<LogicalQuery> {
        match attach.attach_type {
            ast::AttachType::Database => {
                let mut options = HashMap::new();
                let planner = QueryNodePlanner::new(self.bind_data);
                let expr_ctx = ExpressionContext::new(&planner, EMPTY_SCOPE, EMPTY_TYPE_SCHEMA);

                for (k, v) in attach.options {
                    let k = k.into_normalized_string();
                    let v = match expr_ctx.plan_expression(&mut QueryContext::new(), v)? {
                        LogicalExpression::Literal(v) => v,
                        other => {
                            return Err(RayexecError::new(format!(
                                "Non-literal expression provided as value: {other:?}"
                            )))
                        }
                    };
                    if options.contains_key(&k) {
                        return Err(RayexecError::new(format!(
                            "Option '{k}' provided more than once"
                        )));
                    }
                    options.insert(k, v);
                }

                if attach.alias.0.len() != 1 {
                    return Err(RayexecError::new(format!(
                        "Expected a single identifier, got '{}'",
                        attach.alias
                    )));
                }
                let name = attach.alias.pop()?;
                let datasource = attach.datasource_name;

                Ok(LogicalQuery {
                    root: LogicalOperator::AttachDatabase(LogicalNode::new(AttachDatabase {
                        datasource,
                        name,
                        options,
                    })),
                    scope: Scope::empty(),
                })
            }
            ast::AttachType::Table => Err(RayexecError::new("Attach tables not yet supported")),
        }
    }

    fn plan_detach(&mut self, mut detach: ast::Detach<Bound>) -> Result<LogicalQuery> {
        match detach.attach_type {
            ast::AttachType::Database => {
                if detach.alias.0.len() != 1 {
                    return Err(RayexecError::new(format!(
                        "Expected a single identifier, got '{}'",
                        detach.alias
                    )));
                }
                let name = detach.alias.pop()?;

                Ok(LogicalQuery {
                    root: LogicalOperator::DetachDatabase(LogicalNode::new(DetachDatabase {
                        name,
                    })),
                    scope: Scope::empty(),
                })
            }
            ast::AttachType::Table => Err(RayexecError::new("Detach tables not yet supported")),
        }
    }

    fn plan_copy_to(
        &mut self,
        context: &mut QueryContext,
        copy_to: ast::CopyTo<Bound>,
    ) -> Result<LogicalQuery> {
        let source = match copy_to.source {
            ast::CopyToSource::Query(query) => {
                let mut planner = QueryNodePlanner::new(self.bind_data);
                planner.plan_query(context, query)?
            }
            ast::CopyToSource::Table(table) => {
                let (catalog, schema, ent) = match self.bind_data.tables.try_get_bound(table)? {
                    (
                        BoundTableOrCteReference::Table {
                            catalog,
                            schema,
                            entry,
                        },
                        _,
                    ) => (catalog, schema, entry),
                    (BoundTableOrCteReference::Cte { .. }, _) => {
                        // Shouldn't be possible.
                        return Err(RayexecError::new("Cannot COPY from CTE"));
                    }
                };

                let scope = Scope::with_columns(None, ent.columns.iter().map(|f| f.name.clone()));

                // TODO: Loc
                LogicalQuery {
                    root: LogicalOperator::Scan(LogicalNode::new(Scan {
                        catalog: catalog.clone(),
                        schema: schema.clone(),
                        source: ent.clone(),
                    })),
                    scope,
                }
            }
        };

        let source_schema = source.schema()?;

        Ok(LogicalQuery {
            root: LogicalOperator::CopyTo(LogicalNode::new(CopyTo {
                source: Box::new(source.root),
                source_schema,
                location: copy_to.target.location,
                copy_to: copy_to.target.func.unwrap(), // TODO, remove unwrap when serialization works
            })),
            scope: Scope::empty(),
        })
    }

    fn plan_insert(
        &mut self,
        context: &mut QueryContext,
        insert: ast::Insert<Bound>,
    ) -> Result<LogicalQuery> {
        let mut planner = QueryNodePlanner::new(self.bind_data);
        let source = planner.plan_query(context, insert.source)?;

        let entry = match self.bind_data.tables.try_get_bound(insert.table)? {
            (BoundTableOrCteReference::Table { entry, .. }, _) => entry,
            (BoundTableOrCteReference::Cte { .. }, _) => {
                return Err(RayexecError::new("Cannot insert into CTE"))
            } // Shouldn't be possible.
        };

        let table_type_schema = TypeSchema::new(entry.columns.iter().map(|c| c.datatype.clone()));
        let source_schema = source.root.output_schema(&planner.outer_schemas)?;

        let input = Self::apply_cast_for_insert(&table_type_schema, &source_schema, source.root)?;

        // TODO: Handle specified columns. If provided, insert a projection that
        // maps the columns to the right position.

        Ok(LogicalQuery {
            root: LogicalOperator::Insert(LogicalNode::new(Insert {
                table: entry.clone(),
                input: Box::new(input),
            })),
            scope: Scope::empty(),
        })
    }

    fn plan_drop(&mut self, mut drop: ast::DropStatement<Bound>) -> Result<LogicalQuery> {
        match drop.drop_type {
            ast::DropType::Schema => {
                let [catalog, schema] = drop.name.pop_2()?;

                // Dropping defaults to restricting (erroring) on dependencies.
                let deps = drop.deps.unwrap_or(ast::DropDependents::Restrict);

                let plan = LogicalOperator::Drop(LogicalNode::new(DropEntry {
                    info: DropInfo {
                        catalog,
                        schema,
                        object: DropObject::Schema,
                        cascade: ast::DropDependents::Cascade == deps,
                        if_exists: drop.if_exists,
                    },
                }));

                Ok(LogicalQuery {
                    root: plan,
                    scope: Scope::empty(),
                })
            }
            other => not_implemented!("drop {other:?}"),
        }
    }

    fn plan_create_schema(&mut self, mut create: ast::CreateSchema<Bound>) -> Result<LogicalQuery> {
        let on_conflict = if create.if_not_exists {
            OnConflict::Ignore
        } else {
            OnConflict::Error
        };

        let [catalog, schema] = create.name.pop_2()?;

        Ok(LogicalQuery {
            root: LogicalOperator::CreateSchema(LogicalNode::new(CreateSchema {
                catalog,
                name: schema,
                on_conflict,
            })),
            scope: Scope::empty(),
        })
    }

    fn plan_create_table(
        &mut self,
        context: &mut QueryContext,
        mut create: ast::CreateTable<Bound>,
    ) -> Result<LogicalQuery> {
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

        // TODO: Verify column constraints.
        let mut columns: Vec<_> = create
            .columns
            .into_iter()
            .map(|col| Field::new(col.name, col.datatype, true))
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

                let mut planner = QueryNodePlanner::new(self.bind_data);
                let input = planner.plan_query(context, source)?;
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

        let [catalog, schema, name] = create.name.pop_3()?;

        Ok(LogicalQuery {
            root: LogicalOperator::CreateTable(LogicalNode::new(CreateTable {
                catalog,
                schema,
                name,
                columns,
                on_conflict,
                input,
            })),
            scope: Scope::empty(),
        })
    }

    /// Applies a projection cast to a root operator for use when inserting into
    /// a table.
    ///
    /// Errors if the number of columns in the plan does not match the number of
    /// types in the schema.
    ///
    /// If no casting is needed, the returned plan will be unchanged.
    fn apply_cast_for_insert(
        cast_to: &TypeSchema,
        root_schema: &TypeSchema,
        root: LogicalOperator,
    ) -> Result<LogicalOperator> {
        // TODO: This will be where we put the projections for default values too.

        if cast_to.types.len() != root_schema.types.len() {
            return Err(RayexecError::new(format!(
                "Invalid number of inputs. Expected {}, got {}",
                cast_to.types.len(),
                root_schema.types.len()
            )));
        }

        let mut projections = Vec::with_capacity(root_schema.types.len());
        let mut num_casts = 0;
        for (col_idx, (want, have)) in cast_to
            .types
            .iter()
            .zip(root_schema.types.iter())
            .enumerate()
        {
            if want == have {
                // No cast needed, just project the column.
                projections.push(LogicalExpression::new_column(col_idx));
            } else {
                // Need to cast.
                projections.push(LogicalExpression::Cast {
                    to: want.clone(),
                    expr: Box::new(LogicalExpression::new_column(col_idx)),
                });
                num_casts += 1;
            }
        }

        if num_casts == 0 {
            // No casting needed, just return the original plan.
            return Ok(root);
        }

        // Otherwise apply projection.
        Ok(LogicalOperator::Projection(LogicalNode::new(Projection {
            exprs: projections,
            input: Box::new(root),
        })))
    }
}
