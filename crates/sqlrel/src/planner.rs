use crate::catalog::{Catalog, CatalogError, ResolvedTableReference};
use crate::relational::{CrossJoin, RelationalPlan, Scan, Values};
use coretypes::{
    datatype::{DataType, DataValue, NullableType, RelationSchema},
    expr::ScalarExpr,
};
use sqlparser::ast;
use sqlparser::parser::ParserError;
use std::collections::HashMap;

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error(transparent)]
    ParseFail(#[from] ParserError),
    #[error("failed to parse number: {0}")]
    FailedToParseNumber(String),

    #[error(transparent)]
    Catalog(#[from] CatalogError),

    #[error("internal: {0}")]
    Internal(String),
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

    fn plan_query(&self, query: ast::Query) -> Result<RelationalPlan, PlanError> {
        if query.with.is_some() {
            return Err(PlanError::Unsupported(String::from("CTEs")));
        }

        let body_plan = match query.body {
            ast::SetExpr::Values(values) => self.plan_values(values)?,
            set_expr => return Err(PlanError::Unsupported(set_expr.to_string())),
        };

        unimplemented!()
    }

    fn plan_select(
        &self,
        scope: &mut Scope,
        select: ast::Select,
    ) -> Result<RelationalPlan, PlanError> {
        // Plan FROM clause.
        let from_plan = self.plan_from(scope, select.from)?;

        unimplemented!()
    }

    fn plan_from(
        &self,
        scope: &mut Scope,
        froms: Vec<ast::TableWithJoins>,
    ) -> Result<RelationalPlan, PlanError> {
        let inner_scope = scope.clone();

        let mut froms = froms.into_iter();
        let leftmost = froms
            .next()
            .ok_or_else(|| PlanError::Internal("missing from item".to_string()))?;
        let mut plan = self.plan_from_item(scope, leftmost)?;

        for right in froms {
            let mut right_scope = inner_scope.clone();
            let right_plan = self.plan_from_item(&mut right_scope, right)?;
            plan = RelationalPlan::CrossJoin(CrossJoin {
                left: Box::new(plan),
                right: Box::new(right_plan),
            });
            scope.merge(right_scope);
        }

        Ok(plan)
    }

    fn plan_from_item(
        &self,
        scope: &mut Scope,
        table: ast::TableWithJoins,
    ) -> Result<RelationalPlan, PlanError> {
        let left = self.plan_from_table(scope, table.relation)?;

        if table.joins.len() > 0 {
            return Err(PlanError::Unsupported("joins".to_string()));
        }

        Ok(left)
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
                let resolved = self.catalog.resolve_table(tbl_ref)?;
                let schema = self.catalog.table_schema(&resolved)?;
                scope.add_table(resolved.clone(), schema.clone());
                RelationalPlan::Scan(Scan {
                    table: resolved,
                    projected_schema: schema,
                    project: None,
                    filters: None,
                })
            }
            factor => return Err(PlanError::Unsupported(format!("table factor: {}", factor))),
        })
    }

    fn plan_where_selection(
        &self,
        scope: &mut Scope,
        selection: Option<ast::Expr>,
    ) -> Result<RelationalPlan, PlanError> {
        unimplemented!()
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
            let types: Option<Vec<_>> = first
                .iter()
                .map(|expr| expr.output_type(&RelationSchema::empty()))
                .collect();
            let types = types.ok_or(PlanError::Internal(String::from(
                "unable to determine schema for values",
            )))?;
            RelationSchema::new(types)
        } else {
            RelationSchema::empty()
        };

        Ok(RelationalPlan::Values(Values { schema, values }))
    }
}

#[derive(Debug, Clone)]
struct Scope {
    tables: HashMap<ResolvedTableReference, RelationSchema>,
}

impl Scope {
    fn new() -> Scope {
        Scope {
            tables: HashMap::new(),
        }
    }

    /// Add a table to the scope.
    fn add_table(&mut self, table: ResolvedTableReference, schema: RelationSchema) {
        self.tables.insert(table, schema);
    }

    fn merge(&mut self, other: Scope) {
        unimplemented!()
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_query(s: &'static str) -> ast::Query {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let mut statements = sqlparser::parser::Parser::parse_sql(&dialect, s).unwrap();
        assert_eq!(1, statements.len());
        let statement = statements.pop().unwrap();
        match statement {
            ast::Statement::Query(query) => *query,
            statement => panic!("statement not a query: {:?}", statement),
        }
    }

    #[test]
    fn sanity_check() {
        let query = parse_query("select * from my_table");
        // let plan =
    }
}
