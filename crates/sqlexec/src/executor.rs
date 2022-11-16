use crate::errors::{internal, Result};
use crate::logical_plan::*;
use crate::session::Session;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::sql::sqlparser::ast;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use std::collections::VecDeque;
use std::fmt;

/// Results from a sql statement execution.
pub enum ExecutionResult {
    /// The stream for the output of a query.
    Query { stream: SendableRecordBatchStream },
    /// Transaction started.
    Begin,
    /// Transaction committed,
    Commit,
    /// Transaction rolled abck.
    Rollback,
    /// Data successfully written.
    WriteSuccess,
    /// Table created.
    CreateTable,
    /// Schema created.
    CreateSchema,
    /// A client local variable was set.
    SetLocal,
    /// Tables dropped.
    DropTables,
}

impl fmt::Debug for ExecutionResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutionResult::Query { stream } => write!(f, "query (schema: {:?})", stream.schema()),
            ExecutionResult::Begin => write!(f, "begin"),
            ExecutionResult::Commit => write!(f, "commit"),
            ExecutionResult::Rollback => write!(f, "rollback"),
            ExecutionResult::WriteSuccess => write!(f, "write success"),
            ExecutionResult::CreateTable => write!(f, "create table"),
            ExecutionResult::CreateSchema => write!(f, "create schema"),
            ExecutionResult::SetLocal => write!(f, "set local"),
            ExecutionResult::DropTables => write!(f, "drop tables"),
        }
    }
}

/// A thin wrapper around a session responsible for pull-based execution for a
/// sql statement.
///
/// The underlying session will go through the following phases on every call to
/// "next".
/// - Logical planning and optimization
/// - Physical query execution
///
/// Depending on the type of query being executed, the execution result itself
/// may also contains a stream. If the caller does not consume the returned
/// stream, there are no guarantees about the results of any of the following
/// executions.
pub struct Executor<'a> {
    /// All parsed statements.
    statements: VecDeque<ast::Statement>,
    session: &'a mut Session,
}

impl<'a> Executor<'a> {
    /// Create a new executor with the provided sql string and session.
    pub fn new(sql: &'a str, session: &'a mut Session) -> Result<Self> {
        let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql)?
            .into_iter()
            .collect();
        // TODO: Implicit transaction.
        Ok(Executor {
            statements,
            session,
        })
    }

    pub fn statements_remaining(&self) -> usize {
        self.statements.len()
    }

    /// Execute the next statement.
    ///
    /// Returns `None` if there's no more statements to execute.
    pub async fn execute_next(&mut self) -> Option<Result<ExecutionResult>> {
        let statement = self.statements.pop_front()?;
        Some(self.execute_statement(statement).await)
    }

    async fn execute_statement(&mut self, stmt: ast::Statement) -> Result<ExecutionResult> {
        let logical_plan = self.session.plan_sql(stmt)?;
        match logical_plan {
            LogicalPlan::Ddl(DdlPlan::CreateTable(plan)) => {
                self.session.create_table(plan)?;
                Ok(ExecutionResult::CreateTable)
            }
            LogicalPlan::Ddl(DdlPlan::CreateExternalTable(plan)) => {
                self.session.create_external_table(plan).await?;
                Ok(ExecutionResult::CreateTable)
            }
            LogicalPlan::Ddl(DdlPlan::CreateTableAs(plan)) => {
                self.session.create_table_as(plan).await?;
                Ok(ExecutionResult::CreateTable)
            }
            LogicalPlan::Ddl(DdlPlan::CreateSchema(plan)) => {
                self.session.create_schema(plan).await?;
                Ok(ExecutionResult::CreateSchema)
            }
            LogicalPlan::Write(WritePlan::Insert(plan)) => {
                self.session.insert(plan).await?;
                Ok(ExecutionResult::WriteSuccess)
            }
            LogicalPlan::Query(plan) => {
                let physical = self.session.create_physical_plan(plan).await?;
                let stream = self.session.execute_physical(physical)?;
                Ok(ExecutionResult::Query { stream })
            }
            LogicalPlan::Runtime(RuntimePlan::SetParameter(plan)) => {
                self.session.set_parameter(plan)?;
                Ok(ExecutionResult::SetLocal)
            }
            other => Err(internal!("unimplemented logical plan: {:?}", other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::DatabaseCatalog;
    use access::runtime::AccessRuntime;
    use common::access::{AccessConfig, ObjectStoreKind};
    use common::config::DbConfig;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use futures::StreamExt;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    async fn simple() {
        let db_name = String::from("test");
        let catalog = DatabaseCatalog::new(db_name.clone());
        catalog.insert_default_schema().unwrap();
        let cache_dir = TempDir::new().unwrap();

        let config = DbConfig {
            access: Arc::new(AccessConfig {
                db_name,
                object_store: ObjectStoreKind::Local,
                cached: true,
                max_object_store_cache_size: Some(4 * 1024 * 1024 * 1024),
                cache_path: Some(PathBuf::from(cache_dir.path())),
            }),
        };

        let access = Arc::new(AccessRuntime::new(config.access.clone()).await.unwrap());
        let mut session = Session::new(Arc::new(catalog), Arc::new(RuntimeEnv::default()), access);
        let mut executor = Executor::new("select 1+1", &mut session).unwrap();

        let result = executor
            .execute_next()
            .await
            .expect("statement result")
            .expect("didn't error");

        match result {
            ExecutionResult::Query { stream } => {
                let mut results = stream.collect::<Vec<_>>().await;
                assert_eq!(1, results.len());
                let batch = results
                    .pop()
                    .expect("one result")
                    .expect("executed correctly");
                assert_eq!(1, batch.num_rows());
            }
            other => panic!("unexpected result: {:?}", other),
        }
    }
}
