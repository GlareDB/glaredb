use crate::catalog::{Catalog, CatalogError, ResolvedTableReference, TableReference, TableSchema};
use crate::relational::{CrossJoin, Filter, Project, RelationalPlan, Scan, Values};
use coretypes::{
    datatype::{DataType, DataValue, NullableType, RelationSchema},
    expr::ScalarExpr,
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
            scope.append(right_scope)?;
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
                let tbl_schema = self.catalog.table_schema(&tbl_ref)?;

                let schema = tbl_schema.get_schema().clone();
                let resolved = tbl_schema.get_reference().clone();
                let scan = Scan {
                    table: resolved,
                    projected_schema: schema,
                    project: None,
                    filters: None,
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

    fn projection_item_to_scalar(
        &self,
        scope: &Scope,
        item: ast::SelectItem,
    ) -> Result<Vec<ScalarExpr>, PlanError> {
        match item {
            ast::SelectItem::Wildcard => Ok(scope.resolve_wildcard()),
            item => Err(PlanError::Unsupported(format!("select item: {}", item))),
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
            ast::Expr::Value(ast::Value::SingleQuotedString(s)) => {
                Constant(DataValue::Utf8(s), DataType::Utf8.into())
            }

            // Use scope to identifier which column.
            // ast::Expr::Identifier(ident) =>
            expr => {
                return Err(PlanError::Unsupported(format!(
                    "unsupported expression: {}",
                    expr
                )))
            }
        })
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
enum TableAliasOrReference {
    Alias(String),
    Reference(TableReference),
}

impl fmt::Display for TableAliasOrReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableAliasOrReference::Alias(alias) => write!(f, "{} (alias)", alias),
            TableAliasOrReference::Reference(resolved) => write!(f, "{} (resolved)", resolved),
        }
    }
}

#[derive(Debug, Clone)]
struct Scope {
    /// Maps table aliases to a schema.
    tables: HashMap<TableAliasOrReference, usize>,
    /// All table schemas within the scope.
    schemas: Vec<TableSchema>,
}

impl Scope {
    fn new() -> Scope {
        Scope {
            tables: HashMap::new(),
            schemas: Vec::new(),
        }
    }

    /// Add a table with the given alias or reference to the scope.
    fn add_table(
        &mut self,
        alias: TableAliasOrReference,
        schema: TableSchema,
    ) -> Result<(), PlanError> {
        match self.tables.entry(alias) {
            Entry::Occupied(ent) => Err(PlanError::DuplicateTableReference(ent.key().to_string())),
            Entry::Vacant(ent) => {
                let idx = self.schemas.len();
                self.schemas.push(schema);
                ent.insert(idx);
                Ok(())
            }
        }
    }

    /// Append another scope to the right of this one. Errors if there's any
    /// duplicate table references.
    fn append(&mut self, mut other: Scope) -> Result<(), PlanError> {
        let diff = self.schemas.len();
        self.schemas.append(&mut other.schemas);
        for (alias, idx) in other.tables.into_iter() {
            match self.tables.entry(alias) {
                Entry::Occupied(ent) => {
                    return Err(PlanError::DuplicateTableReference(ent.key().to_string()))
                }
                Entry::Vacant(ent) => ent.insert(idx + diff),
            };
        }

        Ok(())
    }

    fn resolve_wildcard(&self) -> Vec<ScalarExpr> {
        let num_cols = self
            .schemas
            .iter()
            .flat_map(|t| t.get_schema().columns.iter())
            .count();

        let mut exprs = Vec::with_capacity(num_cols);
        for idx in 0..num_cols {
            exprs.push(ScalarExpr::Column(idx));
        }

        exprs
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
    use crate::catalog::TableReference;

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

    #[derive(Debug)]
    struct FakeCatalog {
        tables: Vec<TableSchema>,
    }

    impl FakeCatalog {
        fn new() -> FakeCatalog {
            let tables = vec![
                TableSchema::new(
                    ResolvedTableReference {
                        catalog: "db".to_string(),
                        schema: "schema".to_string(),
                        base: "table_1".to_string(),
                    },
                    vec!["col1".to_string(), "col2".to_string(), "col3".to_string()],
                    RelationSchema::new(vec![
                        NullableType::new_nullable(DataType::Int8),
                        NullableType::new_nullable(DataType::Int8),
                        NullableType::new_nullable(DataType::Int8),
                    ]),
                )
                .unwrap(),
                TableSchema::new(
                    ResolvedTableReference {
                        catalog: "db".to_string(),
                        schema: "schema".to_string(),
                        base: "table_2".to_string(),
                    },
                    vec!["col1".to_string(), "col2".to_string(), "col3".to_string()],
                    RelationSchema::new(vec![
                        NullableType::new_nullable(DataType::Int8),
                        NullableType::new_nullable(DataType::Int8),
                        NullableType::new_nullable(DataType::Int8),
                    ]),
                )
                .unwrap(),
            ];

            FakeCatalog { tables }
        }
    }

    impl Catalog for FakeCatalog {
        fn table_schema(&self, tbl: &TableReference) -> Result<TableSchema, CatalogError> {
            let resolved = tbl.clone().resolve_with_defaults("db", "schema");
            let schema = self
                .tables
                .iter()
                .find(|schema| schema.get_reference() == &resolved)
                .ok_or(CatalogError::MissingTable(tbl.to_string()))?;
            Ok(schema.clone())
        }
    }

    #[test]
    fn queries() {
        let query = parse_query("select * from table_1, table_2");
        let catalog = FakeCatalog::new();
        let planner = Planner::new(&catalog);

        let plan = planner.plan_query(query).unwrap();
        println!("plan:\n{}", plan);
    }
}
