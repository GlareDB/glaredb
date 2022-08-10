use crate::catalog::{CatalogReader, TableReference, TableSchema};
use crate::plan::QueryPlan;
use anyhow::{anyhow, Result};
use futures::{StreamExt};
use lemur::execute::stream::source::{
    DataSource, ReadExecutor, ReadTx, TxInteractivity, WriteExecutor, WriteTx,
};
use lemur::repr::df::DataFrame;
use lemur::repr::expr::ExplainRelationExpr;
use serde::{Deserialize, Serialize};
use sqlparser::ast;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::fmt;


use tracing::debug;

#[derive(Debug)]
pub struct Engine<S> {
    source: S,
}

impl<S: DataSource> Engine<S> {
    pub fn new(source: S) -> Self {
        Engine { source }
    }

    /// Create a new session with no active transaction.
    pub fn begin_session(&self) -> Result<Session<S>> {
        Ok(Session {
            source: self.source.clone(),
            tx: None,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ExecutionResult {
    Commit,
    Rollback,
    Begin,
    WriteSuccess, // TODO: Give more detail about the write.
    QueryResult {
        // TODO: Column names.
        df: DataFrame,
    },
    Explain(ExplainRelationExpr), // TODO: How to show pre-lowered plans?
}

/// Utility union to hold either a reference to a transaction, or an owned
/// transaction.
enum RefOrOwnedTx<'a, T> {
    Ref(&'a T),
    Owned(T),
}

impl<'a, T> RefOrOwnedTx<'a, T> {
    fn as_ref(&self) -> &T {
        match self {
            RefOrOwnedTx::Ref(t) => t,
            RefOrOwnedTx::Owned(t) => &t,
        }
    }
}

impl<'a, T> From<&'a T> for RefOrOwnedTx<'a, T> {
    fn from(tx: &'a T) -> Self {
        RefOrOwnedTx::Ref(tx)
    }
}

impl<'a, T> From<T> for RefOrOwnedTx<'a, T> {
    fn from(tx: T) -> Self {
        RefOrOwnedTx::Owned(tx)
    }
}

pub struct Session<S: DataSource> {
    source: S,
    tx: Option<S::Tx>,
}

impl<S: DataSource> Session<S> {
    /// Execute a query which may parse into multiple statements.
    pub async fn execute_query(&mut self, query: &str) -> Result<Vec<ExecutionResult>> {
        debug!("executing query: {}", query);
        let statements = Parser::parse_sql(&PostgreSqlDialect {}, query)?;

        let mut results = Vec::with_capacity(statements.len());
        for statement in statements.into_iter() {
            let result = self.execute(statement).await?;
            results.push(result);
        }

        Ok(results)
    }

    /// Execute a statement.
    #[tracing::instrument]
    async fn execute(&mut self, stmt: ast::Statement) -> Result<ExecutionResult> {
        // Handle statements not directly relating to querying/mutating the
        // database (e.g. beginning and committing transactions) outside of
        // the planner.
        //
        // TODO: Handle transaction modes.
        match stmt {
            ast::Statement::StartTransaction { .. } => {
                if self.tx.is_some() {
                    return Err(anyhow!("nested transactions unsupported"));
                }
                let tx = self.source.begin(TxInteractivity::Interactive).await?;
                self.tx = Some(tx);
                Ok(ExecutionResult::Begin)
            }
            ast::Statement::Commit { .. } => {
                match self.tx.take() {
                    Some(tx) => tx.commit().await?,
                    None => return Err(anyhow!("cannot commit when not in a transaction")),
                }
                Ok(ExecutionResult::Commit)
            }
            ast::Statement::Rollback { .. } => {
                match self.tx.take() {
                    Some(tx) => tx.rollback().await?,
                    None => return Err(anyhow!("cannot rollback when not in a transaction")),
                }
                Ok(ExecutionResult::Rollback)
            }
            ast::Statement::Explain {
                analyze, statement, ..
            } => {
                if analyze {
                    return Err(anyhow!("explain analyze unsupported"));
                }
                let tx = self.get_tx().await?;
                let plan = QueryPlan::plan(*statement, tx.as_ref())?;
                let explained: ExplainRelationExpr = match plan {
                    QueryPlan::Read(plan) => plan.lower()?.into(),
                    QueryPlan::Write(plan) => plan.lower()?.into(),
                };
                Ok(ExecutionResult::Explain(explained))
            }
            stmt => {
                let tx = self.get_tx().await?;
                let plan = QueryPlan::plan(stmt, tx.as_ref())?;
                // TODO: Physical optimization.
                let mut stream = match plan {
                    QueryPlan::Read(plan) => {
                        let lowered = plan.lower()?;
                        lowered.execute_read(tx.as_ref()).await?
                    }
                    QueryPlan::Write(plan) => {
                        let lowered = plan.lower()?;
                        match lowered.execute_write(tx.as_ref()).await? {
                            Some(stream) => stream,
                            None => return Ok(ExecutionResult::WriteSuccess),
                        }
                    }
                };

                let mut df = match stream.next().await {
                    Some(result) => result?,
                    None => {
                        debug!("empty stream");
                        return Ok(ExecutionResult::QueryResult {
                            df: DataFrame::empty(),
                        });
                    }
                };

                // TODO: Stream to client. Does psql support streaming?
                while let Some(result) = stream.next().await {
                    df = df.vstack(result?)?;
                }

                Ok(ExecutionResult::QueryResult { df })
            }
        }
    }

    /// Get a transaction to use during the query.
    ///
    /// If the session is currently in an interactive transaction, that
    /// transaction will be returned. Otherwise a non-interactive transaction
    /// will be used.
    async fn get_tx(&self) -> Result<RefOrOwnedTx<'_, S::Tx>> {
        Ok(match &self.tx {
            Some(tx) => tx.into(),
            None => {
                let tx = self.source.begin(TxInteractivity::NonInteractive).await?;
                tx.into()
            }
        })
    }
}

impl<S: DataSource> fmt::Debug for Session<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: Put more debug info in sessions. Mostly implementing this for
        // tracing instrumentation.
        write!(
            f,
            "session: {}",
            match &self.tx {
                Some(_) => "(interactive)",
                None => "(none)",
            }
        )
    }
}

impl<T: ReadTx> CatalogReader for T {
    fn get_table(&self, _reference: &TableReference) -> Result<Option<TableSchema>> {
        todo!()
    }

    fn get_table_by_name(
        &self,
        _catalog: &str,
        _schema: &str,
        _name: &str,
    ) -> Result<Option<(TableReference, TableSchema)>> {
        todo!()
    }

    fn current_catalog(&self) -> &str {
        todo!()
    }

    fn current_schema(&self) -> &str {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lemur::execute::stream::source::MemoryDataSource;
    use lemur::repr::value::{Row, Value};

    /// Check if two dataframes are "visually" equal. Two dataframes with zero
    /// rows each are considered visually equal even if the number of columns
    /// differ.
    ///
    /// This just makes asserting in tests easier since there's currently no way
    /// to represent a dataframe containing some number of columns with one
    /// empty row.
    fn assert_dfs_visually_eq(left: &DataFrame, right: &DataFrame) {
        if left.num_rows() == 0 && right.num_rows() == 0 {
            return;
        }

        assert!(left == right, "left: {:?}, right: {:?}", left, right);
    }

    fn unwrap_df(exec_result: &ExecutionResult) -> &DataFrame {
        match exec_result {
            ExecutionResult::QueryResult { df } => df,
            other => panic!("not a query result: {:?}", other),
        }
    }

    #[tokio::test]
    async fn simple_queries_no_catalog() {
        let source = MemoryDataSource::new();
        let engine = Engine::new(source);
        let mut session = engine.begin_session().unwrap();

        let tests = vec![
            ("select 1", vec![vec![Value::Int8(Some(1))]]),
            (
                "select 1, 2",
                vec![vec![Value::Int8(Some(1)), Value::Int8(Some(2))]],
            ),
            ("select 1 where false", vec![vec![]]),
            (
                "select 1 group by 2+3 order by 4+5",
                vec![vec![Value::Int8(Some(1))]],
            ),
            ("select 1+2", vec![vec![Value::Int8(Some(3))]]),
            (
                "select * from (values (1), (2)) cross join (values (3), (4))",
                vec![
                    vec![Value::Int8(Some(1)), Value::Int8(Some(3))],
                    vec![Value::Int8(Some(1)), Value::Int8(Some(4))],
                    vec![Value::Int8(Some(2)), Value::Int8(Some(3))],
                    vec![Value::Int8(Some(2)), Value::Int8(Some(4))],
                ],
            ),
        ];

        for test in tests.into_iter() {
            let query = test.0;
            let expected = DataFrame::from_rows(test.1.into_iter().map(Row::from)).unwrap();

            let results = session.execute_query(query).await.unwrap();
            let got = unwrap_df(results.get(0).unwrap());
            assert_dfs_visually_eq(&expected, got);
        }
    }
}
