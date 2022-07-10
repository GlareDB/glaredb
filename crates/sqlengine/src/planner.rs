use crate::catalog::{Catalog, ResolvedTableReference, TableReference, TableSchema};
use crate::logical::{
    AggregateFunc, CreateTable, CrossJoin, Filter, Insert, Join, JoinOperator, JoinType, Project,
    RelationalPlan, Scan, Values,
};
use crate::scope::{Scope, TableAliasOrReference};
use anyhow::anyhow;
use coretypes::{
    datatype::{DataType, DataValue, NullableType, RelationSchema},
    expr::{AggregateOperation, BinaryOperation, ExprError, ScalarExpr, UnaryOperation},
};
use sqlparser::ast;
use sqlparser::parser::ParserError;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error(transparent)]
    ParseFail(#[from] ParserError),
    #[error("failed to parse number: {0}")]
    FailedToParseNumber(String),
    #[error("duplicate table reference: {0}")]
    DuplicateTableReference(String),
    #[error("invalid column reference: {0}")]
    InvalidColumnReference(String),

    #[error(transparent)]
    Expr(#[from] ExprError),

    #[error("internal: {0}")]
    Internal(String),

    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

#[derive(Debug)]
pub struct Planner<'a, C> {
    catalog: &'a C,
}

impl<'a, C: Catalog> Planner<'a, C> {
    /// Create a new planner using the provided catalog.
    pub fn new(catalog: &'a C) -> Self {
        Planner { catalog }
    }

    /// Create a relational plan for the provided sql statement.
    pub fn plan_statement(&self, statement: ast::Statement) -> Result<RelationalPlan, PlanError> {
        match statement {
            ast::Statement::Query(query) => self.plan_query(*query),

            ast::Statement::CreateTable { name, columns, .. } => {
                // TODO: Constraints, indexes
                // TODO: Check if table exists (or maybe do that during execution).

                // TODO: Provide actual catalog and schema values.
                let reference = TableReference::try_from(name)?;
                let resolved = self.catalog.resolve_table(&reference)?;
                let col_names: Vec<_> = columns.iter().map(|col| col.name.value.clone()).collect();
                let col_types: Vec<_> = columns
                    .iter()
                    .map(|col| sql_type_to_data_type(&col.data_type))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .map(|typ| NullableType::new_nullable(typ))
                    .collect();

                let tbl_schema =
                    TableSchema::new(resolved, col_names, RelationSchema::new(col_types))?;

                Ok(RelationalPlan::CreateTable(CreateTable {
                    table: tbl_schema,
                }))
            }

            ast::Statement::Insert {
                table_name,
                columns, // TODO: Need to check columns.
                source,
                ..
            } => {
                // TODO: Handle default values. Right now, input is assumed to
                // produce results that match the schema of the full table.
                // TODO: Actually check that the source input schema is correct.

                let tbl_ref = TableReference::try_from(table_name)?;
                let tbl = self.catalog.get_table(&tbl_ref)?;
                let input = self.plan_query(*source)?;

                Ok(RelationalPlan::Insert(Insert {
                    table: tbl.reference,
                    input: Box::new(input),
                }))
            }

            statement => Err(PlanError::Unsupported(format!("statement: {}", statement))),
        }
    }

    pub fn plan_query(&self, query: ast::Query) -> Result<RelationalPlan, PlanError> {
        if query.with.is_some() {
            return Err(PlanError::Unsupported(String::from("CTEs")));
        }

        let body_plan = match query.body {
            ast::SetExpr::Values(values) => self.plan_values(values)?,
            ast::SetExpr::Select(select) => self.plan_select(*select)?,
            set_expr => return Err(PlanError::Unsupported(set_expr.to_string())),
        };

        Ok(body_plan)
    }

    fn plan_select(&self, select: ast::Select) -> Result<RelationalPlan, PlanError> {
        let mut scope = Scope::new();

        // Plan FROM clause.
        let mut plan = self.plan_from(&mut scope, select.from)?;

        // Plan WHERE clause.
        if let Some(expr) = select.selection {
            plan = RelationalPlan::Filter(Filter {
                predicate: self.sql_expr_to_scalar(&mut scope, expr)?,
                input: Box::new(plan),
            })
        }

        // Plan SELECT clause.
        let exprs: Vec<_> = select
            .projection
            .into_iter()
            .map(|item| self.projection_item_to_scalar(&scope, item))
            .collect::<Result<Vec<_>, _>>()?;
        let exprs = exprs.into_iter().flatten().collect::<Vec<_>>();
        plan = RelationalPlan::Project(Project {
            expressions: exprs,
            input: Box::new(plan),
        });

        Ok(plan)
    }

    fn plan_from(
        &self,
        scope: &mut Scope,
        froms: Vec<ast::TableWithJoins>,
    ) -> Result<RelationalPlan, PlanError> {
        let inner_scope = scope.clone();

        let mut froms = froms.into_iter();
        let leftmost = match froms.next() {
            Some(from) => from,
            None => return Ok(RelationalPlan::Nothing),
        };
        let mut plan = self.plan_from_item(scope, leftmost)?;

        for right in froms {
            let mut right_scope = inner_scope.clone();
            let right_plan = self.plan_from_item(&mut right_scope, right)?;
            plan = RelationalPlan::CrossJoin(CrossJoin {
                left: Box::new(plan),
                right: Box::new(right_plan),
            });
            scope.append(right_scope)?;
        }

        Ok(plan)
    }

    fn plan_from_item(
        &self,
        scope: &mut Scope,
        table: ast::TableWithJoins,
    ) -> Result<RelationalPlan, PlanError> {
        let mut left = self.plan_from_table(scope, table.relation)?;

        for right in table.joins {
            let right_table = self.plan_from_table(scope, right.relation)?;

            let (join_type, operator) = match right.join_operator {
                ast::JoinOperator::Inner(constraint) => {
                    let expr = self.join_constraint_to_scalar(scope, constraint)?;
                    (JoinType::Inner, JoinOperator::On(expr))
                }
                ast::JoinOperator::LeftOuter(constraint) => {
                    let expr = self.join_constraint_to_scalar(scope, constraint)?;
                    (JoinType::Left, JoinOperator::On(expr))
                }
                ast::JoinOperator::RightOuter(constraint) => {
                    let expr = self.join_constraint_to_scalar(scope, constraint)?;
                    (JoinType::Right, JoinOperator::On(expr))
                }

                other => return Err(PlanError::Unsupported(format!("join: {:?}", other))),
            };

            left = RelationalPlan::Join(Join {
                left: Box::new(left),
                right: Box::new(right_table),
                join_type,
                operator,
            });
        }

        Ok(left)
    }

    fn join_constraint_to_scalar(
        &self,
        scope: &Scope,
        constraint: ast::JoinConstraint,
    ) -> Result<ScalarExpr, PlanError> {
        match constraint {
            ast::JoinConstraint::On(expr) => {
                let expr = self.sql_expr_to_scalar(scope, expr)?;
                let input = scope.project_schema();
                let output = expr.output_type(&input)?;
                if !output.is_bool() {
                    return Err(anyhow!("qualified joins using ON must be boolean expressions, got '{}' for expr: {}", output, expr).into());
                }
                Ok(expr)
            }
            other => Err(PlanError::Unsupported(format!(
                "join constraint: {:?}",
                other
            ))),
        }
    }

    fn plan_from_table(
        &self,
        scope: &mut Scope,
        table: ast::TableFactor,
    ) -> Result<RelationalPlan, PlanError> {
        Ok(match table {
            ast::TableFactor::Table { name, alias, .. } => {
                if alias.is_some() {
                    return Err(PlanError::Unsupported("table alias".to_string()));
                }
                let tbl_ref = name.try_into()?;
                let tbl_schema = self.catalog.get_table(&tbl_ref)?;

                let schema = tbl_schema.schema.clone();
                let resolved = tbl_schema.reference.clone();
                let scan = Scan {
                    table: resolved,
                    projected_schema: schema,
                    project: None,
                    filter: None,
                };

                // When adding to scope, use the reference format provided by
                // the user.
                //
                // e.g. if a user references a table "my_table", they can only
                // reference it with "my_table", not "schema.my_table"
                // afterwards.
                scope.add_table(
                    TableAliasOrReference::Reference(tbl_ref),
                    tbl_schema.clone(),
                )?;

                RelationalPlan::Scan(scan)
            }
            factor => return Err(PlanError::Unsupported(format!("table factor: {}", factor))),
        })
    }

    fn plan_values(&self, values: ast::Values) -> Result<RelationalPlan, PlanError> {
        let rows = values.0;
        let mut values = Vec::with_capacity(rows.len());
        for row in rows.into_iter() {
            let scalars: Result<Vec<_>, PlanError> = row
                .into_iter()
                .map(|expr| match expr {
                    ast::Expr::Value(ast::Value::Number(s, _)) => parse_num(&s),
                    ast::Expr::Value(ast::Value::Boolean(b)) => Ok(ScalarExpr::Constant(
                        DataValue::Bool(b),
                        NullableType::new_nullable(DataType::Bool),
                    )),
                    ast::Expr::Value(ast::Value::Null) => Err(PlanError::Unsupported(
                        "null in values statement".to_string(),
                    )), // TODO: Support this.
                    ast::Expr::Value(ast::Value::SingleQuotedString(s)) => {
                        Ok(ScalarExpr::Constant(
                            DataValue::Utf8(s),
                            NullableType::new_nullable(DataType::Utf8),
                        ))
                    }
                    expr => return Err(PlanError::Unsupported(expr.to_string())),
                })
                .collect();
            values.push(scalars?);
        }

        // Determine the schema from the first row.
        //
        // TODO: This should probably check that all rows match the schema.
        let schema = if let Some(first) = values.first() {
            let types = first
                .iter()
                .map(|expr| expr.output_type(&RelationSchema::empty()))
                .collect::<Result<Vec<_>, _>>()?;
            RelationSchema::new(types)
        } else {
            RelationSchema::empty()
        };

        Ok(RelationalPlan::Values(Values { schema, values }))
    }

    fn projection_item_to_scalar(
        &self,
        scope: &Scope,
        item: ast::SelectItem,
    ) -> Result<Vec<ScalarExpr>, PlanError> {
        match item {
            ast::SelectItem::Wildcard => Ok(scope.resolve_wildcard()),
            ast::SelectItem::UnnamedExpr(expr) => Ok(vec![self.sql_expr_to_scalar(scope, expr)?]),
            ast::SelectItem::ExprWithAlias { expr, .. } => {
                // TODO: Handle alias.
                Ok(vec![self.sql_expr_to_scalar(scope, expr)?])
            }
            item => Err(PlanError::Unsupported(format!("select item: {:?}", item))),
        }
    }

    /// Get a scalar expression from the ast expression.
    fn sql_expr_to_scalar(&self, scope: &Scope, expr: ast::Expr) -> Result<ScalarExpr, PlanError> {
        use coretypes::expr::ScalarExpr::*;

        Ok(match expr {
            ast::Expr::Value(ast::Value::Null) => panic!("null"), // TODO
            ast::Expr::Value(ast::Value::Boolean(b)) => {
                Constant(DataValue::Bool(b), DataType::Bool.into())
            }
            ast::Expr::Value(ast::Value::Number(num, _)) => parse_num(&num)?,
            ast::Expr::Value(ast::Value::SingleQuotedString(s)) => {
                Constant(DataValue::Utf8(s), DataType::Utf8.into())
            }

            ast::Expr::BinaryOp { left, op, right } => {
                let logical = match op {
                    ast::BinaryOperator::Eq => BinaryOperation::Eq,
                    ast::BinaryOperator::NotEq => BinaryOperation::Neq,
                    ast::BinaryOperator::Gt => BinaryOperation::Gt,
                    ast::BinaryOperator::Lt => BinaryOperation::Lt,
                    ast::BinaryOperator::GtEq => BinaryOperation::GtEq,
                    ast::BinaryOperator::LtEq => BinaryOperation::LtEq,
                    ast::BinaryOperator::And => BinaryOperation::And,
                    ast::BinaryOperator::Or => BinaryOperation::Or,
                    ast::BinaryOperator::Like => BinaryOperation::Like,
                    ast::BinaryOperator::Plus => BinaryOperation::Add,
                    ast::BinaryOperator::Minus => BinaryOperation::Sub,
                    ast::BinaryOperator::Multiply => BinaryOperation::Mul,
                    ast::BinaryOperator::Divide => BinaryOperation::Div,
                    op => return Err(PlanError::Unsupported(format!("binary operator: {:?}", op))),
                };

                let left = self.sql_expr_to_scalar(scope, *left)?;
                let right = self.sql_expr_to_scalar(scope, *right)?;

                Binary {
                    operation: logical,
                    left: Box::new(left),
                    right: Box::new(right),
                }
            }

            ast::Expr::Identifier(ident) => scope
                .resolve_unqualified_column(&ident.value)
                .ok_or(PlanError::InvalidColumnReference(ident.value))?,

            ast::Expr::CompoundIdentifier(idents) => scope
                .resolve_qualified_column(&idents)
                .ok_or(anyhow!("unable to resolve compound ident: {:?}", idents))?,

            ast::Expr::TypedString { data_type, value } => Cast {
                expr: Box::new(Constant(DataValue::Utf8(value), DataType::Utf8.into())),
                datatype: sql_type_to_data_type(&data_type)?.into(),
            },

            ast::Expr::Function(function) => {
                // TODO: Do other functions too.

                // Only supporting unscoped aggregates for now.
                let agg = if function.name.0.len() == 1 {
                    self.aggregate_func_for_name(&function.name.0[0].value)
                        .ok_or(anyhow!(
                            "missing aggregate func for name: {}",
                            function.name.0[0]
                        ))?
                } else {
                    return Err(PlanError::Unsupported(format!(
                        "scoped functions: {}",
                        function.name
                    )));
                };

                // TODO: Support multiple expressions as arguments. Currently
                // just getting the first and discarding the rest.
                let expr = function
                    .args
                    .into_iter()
                    .map(|arg| self.function_arg_to_expr(arg, scope))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .next()
                    .ok_or(anyhow!("missing arg"))?;

                Aggregate {
                    operation: agg,
                    inner: Box::new(expr),
                }
            }

            expr => {
                return Err(PlanError::Unsupported(format!(
                    "unsupported expression: {0}, debug: {0:?}",
                    expr
                )))
            }
        })
    }

    /// Convert a function arg to an expression.
    ///
    /// E.g. arguments for aggregates.
    fn function_arg_to_expr(
        &self,
        arg: ast::FunctionArg,
        scope: &Scope,
    ) -> Result<ScalarExpr, PlanError> {
        match arg {
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => {
                self.sql_expr_to_scalar(scope, expr)
            }
            arg => Err(PlanError::Unsupported(format!("function arg: {}", arg))),
        }
    }

    /// Return the appropriate aggregate function for the given name.
    fn aggregate_func_for_name(&self, name: &str) -> Option<AggregateOperation> {
        Some(match name {
            "count" => AggregateOperation::Count,
            "sum" => AggregateOperation::Sum,
            "min" => AggregateOperation::Min,
            "max" => AggregateOperation::Max,
            _ => return None,
        })
    }
}

/// Convert a sql ast type to a datatype we can work with.
fn sql_type_to_data_type(sql_type: &ast::DataType) -> Result<DataType, PlanError> {
    Ok(match sql_type {
        ast::DataType::Boolean => DataType::Bool,
        ast::DataType::SmallInt(_) => DataType::Int16,
        ast::DataType::Int(_) => DataType::Int32,
        ast::DataType::BigInt(_) => DataType::Int64,
        ast::DataType::Float(_) => DataType::Float32,
        ast::DataType::Real => DataType::Float32,
        ast::DataType::Double => DataType::Float64,
        ast::DataType::Char(_)
        | ast::DataType::Varchar(_)
        | ast::DataType::Text
        | ast::DataType::String => DataType::Utf8,
        ast::DataType::Date => DataType::Date64,
        other => return Err(PlanError::Unsupported(format!("datatype: {}", other))),
    })
}

/// Parse a string representing a number into a constant scalar expression.
fn parse_num(s: &str) -> Result<ScalarExpr, PlanError> {
    // TODO: Big decimal?
    let (n, t) = match s.parse::<i64>() {
        Ok(n) => (DataValue::Int64(n), DataType::Int64),
        Err(_) => match s.parse::<f64>() {
            Ok(n) => {
                let f = n
                    .try_into()
                    .map_err(|_| PlanError::FailedToParseNumber(s.to_string()))?;
                (DataValue::Float64(f), DataType::Float64)
            }
            Err(_) => return Err(PlanError::FailedToParseNumber(s.to_string())),
        },
    };
    Ok(ScalarExpr::Constant(n, NullableType::new_nullable(t)))
}
