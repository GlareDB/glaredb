use crate::catalog::DatabaseCatalog;
use crate::errors::{internal, Result};
use datafusion::logical_plan::LogicalPlan as DfLogicalPlan;
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use datafusion::sql::sqlparser::ast;
use datafusion::sql::TableReference;
use hashbrown::HashMap;

#[derive(Debug)]
pub enum LogicalPlan {
    /// DDL plans.
    Ddl(DdlPlan),
    /// Write plans.
    Write(WritePlan),
    /// Plans related to querying the underlying data store. This will run
    /// through datafusion.
    Query(DfLogicalPlan),
}

impl From<DfLogicalPlan> for LogicalPlan {
    fn from(plan: DfLogicalPlan) -> Self {
        LogicalPlan::Query(plan)
    }
}

#[derive(Debug)]
pub enum WritePlan {
    Insert(Insert),
}

#[derive(Debug)]
pub struct Insert {
    pub table_name: String,
    pub source: DfLogicalPlan,
}

/// Data defintion logical plans.
///
/// Note that while datafusion has some support for DDL, it's very much focused
/// on working with "external" data that won't be modified like parquet files.
#[derive(Debug)]
pub enum DdlPlan {}

pub struct Planner<'a, C> {
    context: &'a C,
}

impl<'a, C: ContextProvider> Planner<'a, C> {
    pub fn new(context: &'a C) -> Self {
        Planner { context }
    }

    pub fn plan(&self, statement: ast::Statement) -> Result<LogicalPlan> {
        match statement {
            ast::Statement::Query(query) => Ok(self.plan_query_df(*query)?.into()),
            ast::Statement::Insert {
                table_name,
                columns,
                source,
                on,
                ..
            } => {
                let table_name = table_name.to_string();
                let table = self
                    .context
                    .get_table_provider(table_name.as_str().into())?;
                // TODO: Column validation.
                // TODO: How to handle default values? Add an additional projection? When?
                let source = self.plan_query_df(*source)?;
                Ok(LogicalPlan::Write(WritePlan::Insert(Insert {
                    table_name,
                    source,
                })))
            }
            ast::Statement::CreateTable {
                or_replace,
                temporary,
                external,
                if_not_exists,
                name,
                columns,
                constraints,
                ..
            } => {
                unimplemented!()
            }
            stmt => Err(internal!("unsupported sql statement: {}", stmt)),
        }
    }

    /// Plan a query using datafusion.
    fn plan_query_df(&self, query: ast::Query) -> Result<DfLogicalPlan> {
        let df_planner = SqlToRel::new(self.context);
        let mut ctes = HashMap::new();
        let plan = df_planner.query_to_plan(query, &mut ctes)?;
        Ok(plan)
    }
}
