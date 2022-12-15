use crate::context::{ContextProviderAdapter, SessionContext};
use crate::errors::{ExecError, Result};
use crate::logical_plan::*;
use datafusion::sql::planner::SqlToRel;
use datafusion::sql::sqlparser::ast::{self, ObjectType};
use std::collections::HashMap;
use tracing::debug;

/// Plan SQL statements for a session.
pub struct SessionPlanner<'a> {
    ctx: &'a SessionContext,
}

impl<'a> SessionPlanner<'a> {
    pub fn new(ctx: &'a SessionContext) -> Self {
        SessionPlanner { ctx }
    }

    pub fn plan_sql(&self, statement: ast::Statement) -> Result<LogicalPlan> {
        debug!(%statement, "planning sql statement");
        let context = ContextProviderAdapter { context: self.ctx };
        let planner = SqlToRel::new(&context);

        match statement {
            ast::Statement::StartTransaction { .. } => Ok(TransactionPlan::Begin.into()),
            ast::Statement::Commit { .. } => Ok(TransactionPlan::Commit.into()),
            ast::Statement::Rollback { .. } => Ok(TransactionPlan::Abort.into()),

            ast::Statement::Query(query) => {
                let plan = planner.query_to_plan(*query, &mut HashMap::new())?;
                Ok(LogicalPlan::Query(plan))
            }

            ast::Statement::Explain {
                analyze,
                verbose,
                statement,
                ..
            } => {
                let plan = planner.explain_statement_to_plan(verbose, analyze, *statement)?;
                Ok(LogicalPlan::Query(plan))
            }

            ast::Statement::CreateSchema {
                schema_name,
                if_not_exists,
            } => Ok(DdlPlan::CreateSchema(CreateSchema {
                schema_name: schema_name.to_string(),
                if_not_exists,
            })
            .into()),

            // Normal tables.
            ast::Statement::CreateTable {
                external: false,
                if_not_exists,
                name,
                columns,
                query: None,
                ..
            } => {
                let mut arrow_cols = Vec::with_capacity(columns.len());
                for column in columns.into_iter() {
                    // let dt = convert_data_type(&column.data_type)?;
                    // let field = Field::new(&column.name.value, dt, false);
                    // arrow_cols.push(field);
                }

                Ok(DdlPlan::CreateTable(CreateTable {
                    table_name: name.to_string(),
                    columns: arrow_cols,
                    if_not_exists,
                })
                .into())
            }

            // External tables.
            ast::Statement::CreateTable {
                external: true,
                if_not_exists: false,
                name,
                file_format: Some(file_format),
                location: Some(location),
                query: None,
                ..
            } => {
                let file_type: FileType = file_format.try_into()?;
                Ok(DdlPlan::CreateExternalTable(CreateExternalTable {
                    table_name: name.to_string(),
                    location,
                    file_type,
                })
                .into())
            }

            // Tables generated from a source query.
            //
            // CREATE TABLE table2 AS (SELECT * FROM table1);
            ast::Statement::CreateTable {
                external: false,
                name,
                query: Some(query),
                ..
            } => {
                let source = planner.query_to_plan(*query, &mut HashMap::new())?;
                Ok(DdlPlan::CreateTableAs(CreateTableAs {
                    table_name: name.to_string(),
                    source,
                })
                .into())
            }

            stmt @ ast::Statement::Insert { .. } => {
                Err(ExecError::UnsupportedSQLStatement(stmt.to_string()))
            }

            // Drop tables
            ast::Statement::Drop {
                object_type: ObjectType::Table,
                if_exists,
                names,
                ..
            } => {
                let names = names
                    .into_iter()
                    .map(|name| name.to_string())
                    .collect::<Vec<_>>();

                Ok(DdlPlan::DropTable(DropTable { if_exists, names }).into())
            }

            // "SET ...", "SET SESSION ...", "SET LOCAL ..."
            //
            // NOTE: Only session local variables are supported. Transaction
            // local variables behave the same as session local (they're not
            // reset on transaction abort/commit).
            ast::Statement::SetVariable {
                variable, value, ..
            } => Ok(RuntimePlan::SetParameter(SetParameter {
                variable,
                values: value,
            })
            .into()),

            // "SHOW ..."
            ast::Statement::ShowVariable { variable } => {
                Ok(RuntimePlan::ShowParameter(ShowParameter {
                    variable: ast::ObjectName(variable),
                })
                .into())
            }

            stmt => Err(ExecError::UnsupportedSQLStatement(stmt.to_string())),
        }
    }
}
