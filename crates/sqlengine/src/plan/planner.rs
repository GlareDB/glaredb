use super::data_definition::{CreateTable, DataDefinitionPlan};
use super::expr::PlanExpr;
use crate::catalog::{CatalogReader, Column, TableReference, TableSchema};
use crate::plan::read::*;
use crate::plan::scope::Scope;
use crate::plan::write::*;
use crate::plan::{Description, QueryPlan};
use anyhow::{anyhow, Result};
use lemur::repr::df::groupby::SortOrder;
use lemur::repr::expr::{AggregateExpr, AggregateOperation, BinaryOperation};
use lemur::repr::value::{Row, Value, ValueType};
use sqlparser::ast;
use tracing::trace;

pub struct Planner<'a, C> {
    catalog: &'a C,
}

impl<'a, C: CatalogReader> Planner<'a, C> {
    pub fn new(catalog: &'a C) -> Self {
        Planner { catalog }
    }

    /// Plan an individiual sql statement.
    pub fn plan_statement(&self, stmt: ast::Statement) -> Result<QueryPlan> {
        Ok(match stmt {
            ast::Statement::Query(query) => {
                let mut scope = Scope::empty();
                let plan = self.plan_query(&mut scope, *query)?;
                let desc = Description::from_read_plan_and_scope(&plan, &scope)?;
                QueryPlan::Read { plan, desc }
            }
            stmt @ ast::Statement::Insert { .. } => QueryPlan::Write(self.plan_insert(stmt)?),
            stmt @ ast::Statement::CreateTable { .. } => {
                QueryPlan::DataDefinition(self.plan_create_table(stmt)?)
            }
            other => return Err(anyhow!("unsupported statement: {}", other)),
        })
    }

    /// Plan a create table statement.
    pub fn plan_create_table(&self, create: ast::Statement) -> Result<DataDefinitionPlan> {
        match create {
            ast::Statement::CreateTable {
                mut name, columns, ..
            } => {
                let name = match name.0.len() {
                    1 => name.0.pop().unwrap().value,
                    _ => return Err(anyhow!("invalid table name: {:?}", name)),
                };

                let columns: Vec<_> = columns
                    .into_iter()
                    .map(column_def_to_column)
                    .collect::<Result<Vec<_>>>()?;
                // TODO: Get primary keys and other options.
                let schema = TableSchema {
                    name,
                    columns,
                    pk_idxs: Vec::new(),
                };
                Ok(DataDefinitionPlan::CreateTable(CreateTable { schema }))
            }
            _ => Err(anyhow!("invalid create table statement")),
        }
    }

    /// Plan an insert statement.
    fn plan_insert(&self, insert: ast::Statement) -> Result<WritePlan> {
        match insert {
            ast::Statement::Insert {
                table_name, source, ..
            } => {
                // TODO: Check columns.
                let (reference, schema) = self.table_from_catalog(table_name)?;
                let input = self.plan_query(&mut Scope::empty(), *source)?;
                Ok(WritePlan::Insert(Insert {
                    table: reference,
                    pk_idxs: schema.pk_idxs,
                    input,
                }))
            }
            _ => Err(anyhow!("invalid insert statement")),
        }
    }

    fn plan_values(&self, scope: &mut Scope, values: ast::Values) -> Result<ReadPlan> {
        let mut rows: Vec<Row> = Vec::with_capacity(values.0.len());
        for row_exprs in values.0.into_iter() {
            // Convert to plan expressions, then lower to scalar. It's an error
            // to not have only scalar expressions in values.
            let row = row_exprs
                .into_iter()
                .map(|expr| {
                    self.translate_expr(scope, expr)
                        .and_then(|expr| expr.lower_scalar())
                        .and_then(|expr| expr.try_evalulate_constant())
                })
                .collect::<Result<Vec<_>>>()?;
            rows.push(row.into());
        }

        let mut iter = rows.iter();
        match iter.next() {
            Some(row) => {
                let arity = row.arity();
                if !iter.all(|row| row.arity() == arity) {
                    return Err(anyhow!("rows in values have differing arities"));
                }
                // Ensure scope sees these new columns.
                for _i in 0..arity {
                    scope.add_column(None, None);
                }
            }
            None => return Ok(ReadPlan::Nothing),
        }

        Ok(ReadPlan::Values(Values { rows }))
    }

    /// Plan a query.
    ///
    /// The plan that's produced is bloated, but provides a relatively
    /// straightfoward path to coalescing and removing nodes.
    fn plan_query(&self, scope: &mut Scope, query: ast::Query) -> Result<ReadPlan> {
        let select = match query.body {
            ast::SetExpr::Select(select) => *select,
            ast::SetExpr::Values(values) => return self.plan_values(scope, values),
            other => return Err(anyhow!("unsupported query body: {:?}", other)),
        };

        // FROM ...
        let mut plan = self.plan_from_items(scope, select.from)?;
        let from_nothing = matches!(plan, ReadPlan::Nothing);

        // WHERE ...
        if let Some(expr) = select.selection {
            let expr = self.translate_expr(scope, expr)?;
            plan = ReadPlan::Filter(Filter {
                predicate: expr.lower_scalar()?,
                input: Box::new(plan),
            });
        }

        // SELECT ...
        //
        // Big kahuna.
        //
        // When there's order bys or group bys, the "projection" is split up
        // into multiple nodes as follows:
        //
        // - A pre-projection (a projection node) that projects the expressions
        // that the user has provided, arguments to aggregate functions (and
        // eventually window functions), group by expressions, and order by
        // expressions.
        //
        // - An aggregate node that has expressions referencing the pre-projection
        // node.
        //
        // - A post-projection node that references columns produced by the
        // aggregate node. This projection will produce only projections
        // provided by the user.

        let mut exprs = Vec::with_capacity(select.projection.len());
        for item in select.projection {
            match item {
                ast::SelectItem::UnnamedExpr(expr) => {
                    let expr = self.translate_expr(scope, expr)?;
                    exprs.push(expr);
                }
                ast::SelectItem::ExprWithAlias { expr, .. } => {
                    let expr = self.translate_expr(scope, expr)?;
                    exprs.push(expr);
                }
                ast::SelectItem::Wildcard => {
                    if from_nothing {
                        return Err(anyhow!("cannot select * from nothing"));
                    }
                    // Put everything that's currently in scope in the
                    // project expressions.
                    exprs.extend((0..scope.num_columns()).map(PlanExpr::Column))
                }
                other => return Err(anyhow!("unsupported select item: {:?}", other)),
            }
        }

        // If we have aggregates or window functions, we need to produce a
        // pre-projection.
        trace!(?exprs, "checking for non-scalar expressions");
        if !exprs.iter().all(|expr| expr.is_scalar()) {
            // Build the expression list for the pre-projection.
            //
            // For each expression, we check if it's an aggregate. If it is, the
            // argument to the expression is added to the list of pre-projection
            // expressions. E.g. the "a+b" in "sum(a+b)". The aggregate is then
            // added to the aggregate expression list.
            //
            // If the expression isn't an aggregation, it's added to the
            // pre-projection unchanged. A "first value" aggregate expression is
            // added allow passing the data through the aggregate node. E.g this
            // would pass up "c" in "sum(a+b), c".
            //
            // TODO: Check that the expressions that aren't aggregates are
            // referenced in the group by.
            let mut agg_exprs = Vec::with_capacity(exprs.len());
            let mut pre_exprs = Vec::with_capacity(exprs.len());
            for (idx, expr) in exprs.iter_mut().enumerate() {
                if expr.is_scalar() {
                    // No extraction needed.
                    agg_exprs.push(AggregateExpr {
                        op: AggregateOperation::First,
                        column: idx,
                    });
                    // Replace the post-projection with just a column reference.
                    let pre = std::mem::replace(expr, PlanExpr::Column(idx));
                    pre_exprs.push(pre);
                } else {
                    expr.transform_mut_pre(&mut |expr| match expr {
                        PlanExpr::Aggregate { op, arg } => {
                            pre_exprs.push(*arg);
                            agg_exprs.push(AggregateExpr {
                                op,
                                column: pre_exprs.len() - 1,
                            });
                            // The resulting expression should just
                            // reference the new aggregate.
                            Ok(PlanExpr::Column(agg_exprs.len() - 1))
                        }
                        _ => Ok(expr),
                    })?;
                }
            }
            trace!(?exprs, "expressions after aggregates extracted");

            // Append references to what we're grouping by in pre-projection.
            //
            // Note that this modifies scope.
            let group_by = select
                .group_by
                .into_iter()
                .map(|expr| self.translate_expr(scope, expr))
                .collect::<Result<Vec<_>>>()?;
            for _group_by in group_by.iter() {
                scope.add_column(None, None);
            }
            let group_by_cols = (0..group_by.len())
                .map(|idx| idx + pre_exprs.len())
                .collect();
            pre_exprs.extend(group_by);

            // Pre-projection.
            trace!(?pre_exprs, "creating pre-projection for aggregates");
            plan = ReadPlan::Project(Project {
                columns: pre_exprs
                    .into_iter()
                    .map(|expr| expr.lower_scalar())
                    .collect::<Result<Vec<_>>>()?,
                input: Box::new(plan),
            });

            // Aggregates
            plan = ReadPlan::Aggregate(Aggregate {
                group_by: group_by_cols,
                funcs: agg_exprs,
                input: Box::new(plan),
            });
        }

        // ORDER BY...

        // Add a pre-projection to get the expression results we need for order
        // by.
        //
        // Note that order by does not modify scope.
        if !query.order_by.is_empty() {
            // TODO: Collect asc/desc and nulls first.
            let order_by_exprs = query
                .order_by
                .into_iter()
                .map(|expr| self.translate_expr(scope, expr.expr))
                .collect::<Result<Vec<_>>>()?;
            let mut exprs = exprs.clone();
            let order_by_cols: Vec<_> = (0..order_by_exprs.len())
                .map(|idx| idx + exprs.len())
                .collect();
            exprs.extend(order_by_exprs);

            // Pre-projection.
            plan = ReadPlan::Project(Project {
                columns: exprs
                    .into_iter()
                    .map(|expr| expr.lower_scalar())
                    .collect::<Result<Vec<_>>>()?,
                input: Box::new(plan),
            });

            plan = ReadPlan::Sort(Sort {
                columns: order_by_cols,
                order: SortOrder::Asc, // TODO: Should be per-column.
                input: Box::new(plan),
            });
        }

        // Post-projection
        //
        // This ensures that only the original expressions being selected for
        // are in the final projection.
        plan = ReadPlan::Project(Project {
            columns: exprs
                .into_iter()
                .map(|expr| expr.lower_scalar())
                .collect::<Result<Vec<_>>>()?,
            input: Box::new(plan),
        });

        Ok(plan)
    }

    /// Plan multiple from items.
    fn plan_from_items(
        &self,
        scope: &mut Scope,
        items: Vec<ast::TableWithJoins>,
    ) -> Result<ReadPlan> {
        let base_scope = scope.clone();
        let mut items = items.into_iter();
        let mut left = match items.next() {
            Some(item) => self.plan_from_item(scope, item)?,
            None => return Ok(ReadPlan::Nothing),
        };

        for item in items {
            let mut inner_scope = base_scope.clone();
            let right = self.plan_from_item(&mut inner_scope, item)?;
            left = ReadPlan::CrossJoin(CrossJoin {
                left: Box::new(left),
                right: Box::new(right),
            });
            scope.merge(inner_scope)?;
        }

        Ok(left)
    }

    /// Plan a single from item.
    ///
    /// A from item is either a single table, or a table with multiple trailing
    /// joins. E.g. "select * from t1" and "select * from t1 inner join t2".
    fn plan_from_item(&self, scope: &mut Scope, item: ast::TableWithJoins) -> Result<ReadPlan> {
        let mut left = self.plan_table_factor(scope, item.relation)?;
        let on_expr = |constraint| match constraint {
            ast::JoinConstraint::On(expr) => Ok(expr),
            other => Err(anyhow!("unsupported join constraint: {:?}", other)),
        };

        for join in item.joins {
            let right = self.plan_table_factor(scope, join.relation)?;
            left = match join.join_operator {
                ast::JoinOperator::Inner(constraint) => {
                    let on = self.translate_expr(scope, on_expr(constraint)?)?;
                    ReadPlan::Join(Join {
                        left: Box::new(left),
                        right: Box::new(right),
                        join_type: JoinType::Inner,
                        on: on.lower_scalar()?,
                    })
                }
                ast::JoinOperator::CrossJoin => ReadPlan::CrossJoin(CrossJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                }),
                other => return Err(anyhow!("unsupported join operator: {:?}", other)),
            };
        }

        Ok(left)
    }

    /// Plan a table factor.
    ///
    /// E.g. a base table, a subquery, or a table function.
    ///
    /// Adds the columns in the table to the scope.
    fn plan_table_factor(&self, scope: &mut Scope, table: ast::TableFactor) -> Result<ReadPlan> {
        Ok(match table {
            ast::TableFactor::Table { name, alias, .. } => {
                let (reference, table_schema) = self.table_from_catalog(name)?;
                let schema = table_schema.to_schema();
                // TODO: Handle column aliases.
                scope.add_table(alias.map(|a| a.name.value), table_schema)?;
                ReadPlan::ScanSource(ScanSource {
                    table: reference,
                    filter: None, // Note that filters will be pushed down during optimization.
                    schema,
                })
            }
            ast::TableFactor::Derived {
                lateral: _,
                subquery,
                alias: _,
            } => {
                // TODO: Handle lateral and alias.
                self.plan_query(scope, *subquery)?
            }
            other => return Err(anyhow!("unsupported table factor: {:?}", other)),
        })
    }

    /// Get a table reference from the catalog.
    ///
    /// Errors if the table doesn't exist.
    fn table_from_catalog(&self, name: ast::ObjectName) -> Result<(TableReference, TableSchema)> {
        let catalog_name = self.catalog.current_catalog();
        let schema_name = self.catalog.current_schema();
        let (reference, schema) = match name.0.len() {
            1 => self
                .catalog
                .get_table_by_name(catalog_name, schema_name, &name.0[0].value)?
                .ok_or_else(|| anyhow!("missing table: {}", name))?,
            2 => self
                .catalog
                .get_table_by_name(catalog_name, &name.0[0].value, &name.0[1].value)?
                .ok_or_else(|| anyhow!("missing table: {}", name))?,
            3 => self
                .catalog
                .get_table_by_name(&name.0[2].value, &name.0[1].value, &name.0[0].value)?
                .ok_or_else(|| anyhow!("missing table: {}", name))?,
            _ => return Err(anyhow!("invalid object name: {}", name)),
        };

        Ok((reference, schema))
    }

    fn translate_expr(&self, scope: &Scope, sql: ast::Expr) -> Result<PlanExpr> {
        Ok(match sql {
            ast::Expr::Identifier(ident) => PlanExpr::Column(scope.resolve(None, &ident.value)?),

            ast::Expr::CompoundIdentifier(mut idents) => {
                if idents.len() != 2 {
                    return Err(anyhow!("unsupported compound ident: {:?}", idents));
                }
                let name = idents.pop().unwrap().value;
                let table = idents.pop().unwrap().value;
                PlanExpr::Column(scope.resolve(Some(&table), &name)?)
            }

            ast::Expr::Value(value) => match value {
                ast::Value::Null => PlanExpr::Constant(Value::Null),
                ast::Value::Boolean(b) => PlanExpr::Constant(Value::Bool(Some(b))),
                ast::Value::Number(num, _) => PlanExpr::Constant(parse_number(&num)?),
                ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => {
                    PlanExpr::Constant(Value::Utf8(Some(s)))
                }
                other => return Err(anyhow!("unsupported value expr: {}", other)),
            },

            ast::Expr::BinaryOp { left, op, right } => {
                let left = self.translate_expr(scope, *left)?;
                let right = self.translate_expr(scope, *right)?;
                let op = match op {
                    ast::BinaryOperator::Plus => BinaryOperation::Add,
                    ast::BinaryOperator::Minus => BinaryOperation::Sub,
                    ast::BinaryOperator::Multiply => BinaryOperation::Mul,
                    ast::BinaryOperator::Divide => BinaryOperation::Div,
                    ast::BinaryOperator::And => BinaryOperation::And,
                    ast::BinaryOperator::Or => BinaryOperation::Or,
                    ast::BinaryOperator::Eq => BinaryOperation::Eq,
                    ast::BinaryOperator::NotEq => BinaryOperation::Neq,
                    ast::BinaryOperator::Lt => BinaryOperation::Lt,
                    ast::BinaryOperator::LtEq => BinaryOperation::LtEq,
                    ast::BinaryOperator::Gt => BinaryOperation::Gt,
                    ast::BinaryOperator::GtEq => BinaryOperation::GtEq,
                    op => return Err(anyhow!("unsupported binary op: {}", op)),
                };

                PlanExpr::Binary {
                    op,
                    left: left.boxed(),
                    right: right.boxed(),
                }
            }

            ast::Expr::Function(function) => {
                // TODO: Check other types of functions too.

                let op = if function.name.0.len() == 1 {
                    match AggregateOperation::try_from_str(&function.name.0[0].value) {
                        Some(op) => op,
                        None => {
                            return Err(anyhow!("no aggregate for name: {:?}", &function.name.0))
                        }
                    }
                } else {
                    return Err(anyhow!("qualified function unsupported"));
                };

                let mut args = function
                    .args
                    .into_iter()
                    .map(|arg| self.function_arg_to_expr(scope, arg))
                    .collect::<Result<Vec<_>>>()?;

                let arg = match args.len() {
                    0 => return Err(anyhow!("got no args for aggregate")),
                    1 => args.pop().unwrap(),
                    _ => return Err(anyhow!("too many args for aggregate")),
                };

                PlanExpr::Aggregate {
                    op,
                    arg: arg.boxed(),
                }
            }

            other => return Err(anyhow!("unsupported expression: {:?}", other)),
        })
    }

    fn function_arg_to_expr(&self, scope: &Scope, arg: ast::FunctionArg) -> Result<PlanExpr> {
        match arg {
            ast::FunctionArg::Named { arg, .. } | ast::FunctionArg::Unnamed(arg) => match arg {
                ast::FunctionArgExpr::Expr(expr) => self.translate_expr(scope, expr),
                other => Err(anyhow!("unsupported function arg expr: {:?}", other)),
            },
        }
    }
}

fn parse_number(s: &str) -> Result<Value> {
    if let Ok(n) = s.parse::<i8>() {
        return Ok(Value::Int8(Some(n)));
    }
    if let Ok(n) = s.parse::<i32>() {
        return Ok(Value::Int32(Some(n)));
    }
    if let Ok(n) = s.parse::<f32>() {
        return Ok(Value::Float32(Some(n.into())));
    }
    Err(anyhow!("unable to parse into number: {}", s))
}

fn column_def_to_column(col: ast::ColumnDef) -> Result<Column> {
    let ty = match col.data_type {
        ast::DataType::Char(_)
        | ast::DataType::Varchar(_)
        | ast::DataType::Text
        | ast::DataType::String => ValueType::Utf8,
        ast::DataType::SmallInt(_) => ValueType::Int8,
        ast::DataType::Int(_) => ValueType::Int32,
        ast::DataType::Real | ast::DataType::Float(None) | ast::DataType::Float(Some(4)) => {
            ValueType::Float32
        }
        ast::DataType::Boolean => ValueType::Bool,
        other => return Err(anyhow!("invalid column data type: {}", other)),
    };

    let mut nullable = false;
    for option in col.options {
        match option.option {
            ast::ColumnOption::Null => nullable = true,
            ast::ColumnOption::NotNull => nullable = false,
            // TODO: Other options like indexes and whatnot.
            _ => (),
        }
    }

    Ok(Column {
        name: col.name.value,
        ty,
        nullable,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::dummy::DummyCatalog;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    fn parse(query: &str) -> ast::Statement {
        let mut stmts = Parser::parse_sql(&PostgreSqlDialect {}, query).unwrap();
        assert_eq!(1, stmts.len());
        stmts.pop().unwrap()
    }

    #[test]
    fn sanity_tests() {
        logutil::init_test();
        let catalog = DummyCatalog::new();
        let queries = vec![
            "select 1 + 1",
            "select * from (values (1), (2))",
            "select a from t1",
            "select * from t1",
            "select a from t1 where b < 10",
            "select * from t1 inner join t2 on t1.b = t2.b",
            "select * from t1 join t2 on t1.b = t2.b",
            "select sum(a) + 1, b from t1 group by b",
            "select sum(t3.a + 4), c from t1 as t3 inner join t2 as t4 on t3.b = t4.b where a > 5 group by c",
            "select a from t1 order by a",
            "select sum(a), b from t1 where a > 10 group by b order by b",
            "insert into t1 values (1, 2)",
        ];

        for query in queries {
            let planner = Planner::new(&catalog);

            println!("query: {}", query);
            let stmt = parse(query);

            let plan = planner.plan_statement(stmt).unwrap();
            println!("plan: {:#?}", plan);
        }
    }

    #[test]
    fn descriptions() {
        let catalog = DummyCatalog::new();
        let test_cases = vec![
            ("select 1+1", vec!["?"]),
            ("select a from t1", vec!["a"]),
            ("select * from t1", vec!["a", "b"]),
            (
                "select * from t1 inner join t2 on t1.b = t2.b",
                vec!["a", "b", "b", "c"],
            ),
            (
                "select t1.b, t1.b, t2.b from t1 inner join t2 on t1.b = t2.b",
                vec!["b", "b", "b"],
            ),
        ];

        for (query, col_names) in test_cases.into_iter() {
            let planner = Planner::new(&catalog);
            let stmt = parse(query);
            let plan = planner.plan_statement(stmt).unwrap();

            match plan {
                QueryPlan::Read { desc, .. } => {
                    let expected = Description {
                        columns: col_names.into_iter().map(String::from).collect(),
                    };
                    assert_eq!(expected, desc);
                }
                other => panic!("unexpected query plan: {:?}", other),
            }
        }
    }
}
