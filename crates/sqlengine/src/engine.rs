use crate::catalog::{CatalogReader, TableReference, TableSchema, CatalogWriter};
use crate::plan::QueryPlan;
use crate::plan::data_definition::DataDefinitionPlan;
use crate::system::{system_tables, ColumnsTable, SystemTable};
use anyhow::{anyhow, Result};
use futures::{StreamExt, executor};
use lemur::execute::stream::source::{
    DataSource, ReadExecutor, ReadTx, TxInteractivity, WriteExecutor, WriteTx,
};
use lemur::repr::df::DataFrame;
use lemur::repr::expr::{ExplainRelationExpr, ScalarExpr, RelationKey};
use lemur::repr::value::{Value, ValueVec};
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

    pub async fn ensure_system_tables(&mut self) -> Result<()> {
        let tx = self.source.begin(TxInteractivity::NonInteractive).await?;
        for table in system_tables().into_iter() {
            let table_ref = table.generate_table_reference();
            let schema = tx.get_table(&table_ref)?;
            if schema.is_none() {
                let schema = table.generate_table_schema();
                tx.create_table(table_ref.into(), schema.into()).await?;
            }
        }

        Ok(())
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
                    QueryPlan::DataDefinition(_) => todo!(),
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
                    QueryPlan::DataDefinition(plan) => {
                        match plan {
                            DataDefinitionPlan::CreateTable(plan) => {
                                let source = tx.as_ref();
                                let schema: TableSchema = plan.into();
                                // TODO: Make use of different catalog/schema
                                let table_ref = TableReference { 
                                    catalog: ColumnsTable.catalog().to_string(),
                                    schema: ColumnsTable.schema().to_string(),
                                    table: schema.name.clone(),
                                };
                                source.add_table(&table_ref, schema)?;
                                return Ok(ExecutionResult::WriteSuccess);
                            }
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
            },
        )
    }
}

impl<T: ReadTx> CatalogReader for T {
    fn get_table(&self, reference: &TableReference) -> Result<Option<TableSchema>> {
        executor::block_on(async move {
            let table = self.scan_values_equal(
                &ColumnsTable.generate_table_reference().into(),
                &vec![(0, Value::Utf8(Some(reference.to_string())))],
            ).await?;
            match table {
                Some(mut stream) => {
                    while let Some(stream_result) = stream.next().await {
                        let df = stream_result?;
                        let schema = df.get_column_ref(1).ok_or_else(|| {
                            anyhow!("table schema not found")
                        })?;
                        match schema.as_ref() {
                            &ValueVec::Binary(ref binary_vec) => {
                                let bytes = binary_vec.get_value(0).ok_or_else(|| {
                                    anyhow!("table schema not found")
                                })?;
                                let decoded: TableSchema = bincode::deserialize(&bytes[..])?;
                                return Ok(Some(decoded));
                            }
                            _ => {
                                return Err(anyhow!("table schema not found"));
                            }
                        }
                    }
                    Ok(None)
                }
                None => {
                    Ok(None)
                }
            }
        })
    }

    fn get_table_by_name(
        &self,
        catalog: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<(TableReference, TableSchema)>> {
        let table_ref = TableReference {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: name.to_string(),
        };
        let table = self.get_table(&table_ref)?;
        Ok(table.map(|table| (table_ref, table)))
    }

    fn current_catalog(&self) -> &str {
        ColumnsTable.catalog()
    }

    fn current_schema(&self) -> &str {
        ColumnsTable.schema()
    }
}

impl<T: WriteTx> CatalogWriter for T {
    fn add_table(&self, reference: &TableReference, schema: TableSchema) -> Result<()> {
        executor::block_on(async {
            let key: RelationKey = reference.into();

            // construct a dataframe with the data as a row
            let df = DataFrame::from_row(vec![
                Value::Utf8(Some(key.clone())),
                Value::Binary(Some(bincode::serialize(&schema)?)),
            ])?;
            // store the table in the columns table
            let columns_key: RelationKey = ColumnsTable.generate_table_reference().into();
            self.insert(&columns_key, df).await?;

            // allocate the table in the source
            self.create_table(key, schema.into()).await?;

            Ok(())
        })
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

    async fn prepare_session<S: DataSource>(source: S) -> Session<S> {
        let mut engine = Engine::new(source);
        engine.ensure_system_tables().await.unwrap();
        let session = engine.begin_session().unwrap();
        session
    }

    #[tokio::test]
    async fn create_table() {
        let mut session = prepare_session(MemoryDataSource::new()).await;

        let query = "create table foo (a int, b int)";
        let create_table = session.execute_query(query).await.unwrap();

        let query = "create table bar (a int, b int)";
        let create_table = session.execute_query(query).await.unwrap();
        println!("session: {:?}", session);
        let record_a = session.execute_query("insert into foo values (10000001, 10000002)").await.unwrap();
        let record_b = session.execute_query("insert into bar values (10000003, 10000002)").await.unwrap();
        let select = session.execute_query("select * from foo").await.unwrap();
        println!("select: {:?}", select);
        let select = session.execute_query("select * from bar join foo on foo.b = bar.b").await.unwrap();
        println!("select: {:?}", select);

    }

    #[tokio::test]
    async fn insert_into_table() {
        let mut session = prepare_session(MemoryDataSource::new()).await;

        session.execute_query("create table foo (a int, b int)").await.unwrap();
        session.execute_query("insert into foo values (10000001, 10000002)").await.unwrap();
        let results = session.execute_query("select * from foo").await.unwrap();

        let expected = DataFrame::from_row(vec![
            Value::Int32(Some(10000001)),
            Value::Int32(Some(10000002)),
        ]).unwrap();
        let got = unwrap_df(results.get(0).unwrap());
        assert_dfs_visually_eq(&expected, got);
    }

    #[tokio::test]
    async fn join_tables() {
        let mut session = prepare_session(MemoryDataSource::new()).await;

        session.execute_query("create table foo (a int, b int)").await.unwrap();
        session.execute_query("insert into foo values (10000001, 10000002)").await.unwrap();
        session.execute_query("create table bar (a int, b int)").await.unwrap();
        session.execute_query("insert into bar values (10000003, 10000002)").await.unwrap();
        let results = session.execute_query("select * from foo join bar on foo.b = bar.b").await.unwrap();

        let expected = DataFrame::from_rows(vec![
            Row::from(vec![
                Value::Int32(Some(10000001)),
                Value::Int32(Some(10000002)),
                Value::Int32(Some(10000003)),
                Value::Int32(Some(10000002)),
            ]),
        ]).unwrap();
        let got = unwrap_df(results.get(0).unwrap());
        assert_dfs_visually_eq(&expected, got);
    }
}
