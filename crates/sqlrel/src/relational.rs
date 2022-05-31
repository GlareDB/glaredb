use crate::catalog::{Catalog, CatalogError, ResolvedTableReference, TableReference};
use coretypes::{
    datatype::{DataType, DataValue, NullableType, RelationSchema},
    expr::ScalarExpr,
};
use sqlparser::ast;
use sqlparser::parser::{Parser, ParserError};

#[derive(Debug, thiserror::Error)]
pub enum RelationalError {
    #[error("not a query: {0}")]
    NotAQuery(ast::Statement),
    #[error(transparent)]
    ParseFail(#[from] ParserError),
    #[error("unsupported sql: {0}")]
    UnsupportedSql(String),
    #[error("failed to parse number: {0}")]
    FailedToParseNumber(String),

    #[error(transparent)]
    Catalog(#[from] CatalogError),

    #[error("internal: {0}")]
    Internal(String),
}

/// Create a relational error indicating that something is unsupported sql.
fn unsupported(what: &str) -> RelationalError {
    RelationalError::UnsupportedSql(what.to_string())
}

#[derive(Debug)]
pub enum RelationalPlan {
    /// Evaluate a filter on all inputs.
    ///
    /// "WHERE ..."
    Filter(Filter),
    /// Project from inputs.
    ///
    /// "SELECT ..."
    Project(Project),
    /// A base table scan.
    Scan(Scan),
    /// Constant values.
    Values(Values),
}

#[derive(Debug)]
pub struct Filter {
    predicate: ScalarExpr,
    input: Box<RelationalPlan>,
}

#[derive(Debug)]
pub struct Project {
    /// A list of expressions to evaluate. The may introduce new values.
    expressions: Vec<ScalarExpr>,
    input: Box<RelationalPlan>,
}

#[derive(Debug)]
pub struct Scan {
    table: ResolvedTableReference,
    /// Schema describing the table with the projections applied.
    projected_schema: RelationSchema,
    /// An optional list of column indices to project.
    project: Option<Vec<usize>>,
    /// An optional list of filters to apply during scanning. Expressions should
    /// return booleans indicating if the row should be returned.
    filters: Option<Vec<ScalarExpr>>,
}

#[derive(Debug)]
pub struct Values {
    schema: RelationSchema,
    values: Vec<Vec<ScalarExpr>>,
}

impl RelationalPlan {
    /// Try to create a plan from a sql string. Errors if the string doesn't
    /// represent a query.
    ///
    /// This is mostly useful for testing, since only one statement is expected.
    pub fn from_query_str<C: Catalog>(
        query: &str,
        catalog: C,
    ) -> Result<RelationalPlan, RelationalError> {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};

        let mut statements = Parser::parse_sql(&dialect, query)?;
        if statements.len() != 1 {
            return Err(RelationalError::Internal(String::from(
                "too many statements",
            )));
        }

        let stmt = statements.remove(0);
        match stmt {
            ast::Statement::Query(query) => Self::from_query(*query, catalog),
            stmt => Err(RelationalError::NotAQuery(stmt)),
        }
    }

    /// Create a relational algebra tree representing the provided query.
    pub fn from_query<C: Catalog>(
        query: ast::Query,
        catalog: C,
    ) -> Result<RelationalPlan, RelationalError> {
        if query.with.is_some() {
            return Err(unsupported("cte"));
        }
        if query.order_by.len() != 0 {
            return Err(unsupported("order by"));
        }
        if query.limit.is_some() {
            return Err(unsupported("limit"));
        }
        if query.offset.is_some() {
            return Err(unsupported("offset"));
        }
        if query.fetch.is_some() {
            return Err(unsupported("fetch"));
        }
        if query.lock.is_some() {
            return Err(unsupported("lock"));
        }

        println!("query: {:?}", query);

        let builder = PlanBuilder::with_catalog(catalog);

        Ok(match query.body {
            ast::SetExpr::Select(select) => builder.plan_select(*select)?,
            ast::SetExpr::Values(values) => RelationalPlan::Values(builder.plan_values(values)?),
            set_expr => return Err(unsupported(&set_expr.to_string())),
        })
    }
}

pub struct PlanBuilder<C> {
    catalog: C,
}

impl<C: Catalog> PlanBuilder<C> {
    fn with_catalog(catalog: C) -> Self {
        PlanBuilder { catalog }
    }

    /// Convert a select statement to a relational plan.
    fn plan_select(&self, select: ast::Select) -> Result<RelationalPlan, RelationalError> {
        // FROM ...
        let table_plans = select
            .from
            .into_iter()
            .map(|tbl| self.plan_table(tbl))
            .collect::<Result<Vec<_>, _>>()?;

        // WHERE ...
        let plan = self.plan_where_selection(select.selection, table_plans)?;

        unimplemented!()
    }

    /// Convert a table with optional joins to a relation plan.
    fn plan_table(&self, table: ast::TableWithJoins) -> Result<RelationalPlan, RelationalError> {
        if table.joins.len() > 0 {
            return Err(unsupported("joins"));
        }

        // This currently returns a base table scan with no projection or
        // filtering.
        match table.relation {
            ast::TableFactor::Table { name, .. } => {
                let tbl = TableReference::try_from(name)?;
                let resolved = self.catalog.resolve_table(tbl)?;
                let schema = self.catalog.table_schema(&resolved)?;
                Ok(RelationalPlan::Scan(Scan {
                    table: resolved,
                    projected_schema: schema,
                    project: None,
                    filters: None,
                }))
            }
            factor => Err(unsupported(&factor.to_string())),
        }
    }

    /// Plan the expression associated with the "WHERE" clause of a query.
    /// Accepts plans generated for the "FROM" clause.
    fn plan_where_selection(
        &self,
        selection: Option<ast::Expr>,
        from_plans: Vec<RelationalPlan>,
    ) -> Result<RelationalPlan, RelationalError> {
        let mut from_plans = from_plans;
        match selection {
            Some(expr) => {
                // TODO: Implement

                let plan = from_plans.remove(0);
                Ok(plan)
            }
            None => {
                let plan = from_plans.remove(0);
                // TODO: Handle cross joins.
                Ok(plan)
            }
        }
    }

    /// Convert a values expression returned by the sql parser into values that
    /// can be used in the relational plan.
    fn plan_values(&self, values: ast::Values) -> Result<Values, RelationalError> {
        let rows = values.0;
        let mut values = Vec::with_capacity(rows.len());
        for row in rows.into_iter() {
            let scalars: Result<Vec<_>, RelationalError> = row
                .into_iter()
                .map(|expr| match expr {
                    ast::Expr::Value(ast::Value::Number(s, _)) => parse_num(&s),
                    ast::Expr::Value(ast::Value::Boolean(b)) => Ok(ScalarExpr::Constant(
                        DataValue::Bool(b),
                        NullableType::new_nullable(DataType::Bool),
                    )),
                    ast::Expr::Value(ast::Value::Null) => {
                        Err(unsupported("null in values statement"))
                    } // TODO: Support this.
                    ast::Expr::Value(ast::Value::SingleQuotedString(s)) => {
                        Ok(ScalarExpr::Constant(
                            DataValue::Utf8(s),
                            NullableType::new_nullable(DataType::Utf8),
                        ))
                    }
                    expr => return Err(RelationalError::UnsupportedSql(expr.to_string())),
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
            let types = types.ok_or(RelationalError::Internal(String::from(
                "unable to determine schema for values",
            )))?;
            RelationSchema::new(types)
        } else {
            RelationSchema::empty()
        };

        Ok(Values { schema, values })
    }
}

/// Parse a string representing a number into a constant scalar expression.
fn parse_num(s: &str) -> Result<ScalarExpr, RelationalError> {
    // TODO: Big decimal?
    let (n, t) = match s.parse::<i64>() {
        Ok(n) => (DataValue::Int64(n), DataType::Int64),
        Err(_) => match s.parse::<f64>() {
            Ok(n) => {
                let f = n
                    .try_into()
                    .map_err(|_| RelationalError::FailedToParseNumber(s.to_string()))?;
                (DataValue::Float64(f), DataType::Float64)
            }
            Err(_) => return Err(RelationalError::FailedToParseNumber(s.to_string())),
        },
    };
    Ok(ScalarExpr::Constant(n, NullableType::new_nullable(t)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::TableReference;
    use std::collections::HashMap;

    #[derive(Default)]
    struct DummyCatalog {
        schemas: HashMap<ResolvedTableReference, RelationSchema>,
    }

    impl Catalog for DummyCatalog {
        fn resolve_table(
            &self,
            tbl: crate::catalog::TableReference,
        ) -> Result<ResolvedTableReference, CatalogError> {
            Ok(tbl.resolve_with_defaults("dummy", "dummy"))
        }

        fn table_schema(
            &self,
            tbl: &ResolvedTableReference,
        ) -> Result<RelationSchema, CatalogError> {
            self.schemas
                .get(tbl)
                .cloned()
                .ok_or(CatalogError::MissingTable(tbl.to_string()))
        }
    }

    #[test]
    fn sanity() {
        let c = DummyCatalog::default();
        let s = "select * from (values(1)) v";
        let plan = RelationalPlan::from_query_str(s, c).unwrap();
        println!("plan: {:?}", plan);
    }
}
