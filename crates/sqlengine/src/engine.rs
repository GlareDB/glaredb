use crate::catalog::{Catalog, ResolvedTableReference, TableReference, TableSchema};
use crate::logical::RelationalPlan;
use crate::physical::PhysicalPlan;
use crate::planner::Planner;
use anyhow::{anyhow, Result};
use coretypes::batch::{Batch, BatchRepr};
use coretypes::datatype::Row;
use coretypes::stream::BatchStream;
use diststore::engine::{Interactivity, StorageEngine, StorageTransaction};
use futures::executor::block_on;
use futures::stream::StreamExt;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;

#[derive(Debug)]
pub struct Engine<S> {
    storage: S,
}

impl<S: StorageEngine> Engine<S> {
    pub fn new(storage: S) -> Self {
        Engine { storage }
    }

    pub fn start_session(&self) -> Result<Session<S>> {
        let tx = self.storage.begin(Interactivity::None)?;
        Ok(Session { tx: Arc::new(tx) })
    }
}

#[derive(Debug)]
pub enum ExecutionResult {
    CreateTable,
    QueryResult { batches: Vec<BatchRepr> },
}

#[derive(Debug)]
pub struct Session<S: StorageEngine> {
    tx: Arc<S::Transaction>,
}

impl<S: StorageEngine + 'static> Session<S> {
    /// Execute a user-provided query.
    ///
    /// The query string may include multiple statements. All statements will be
    /// planned, then all plan executed, stopping after the first error.
    ///
    /// A vector of execution results will be returned, with each result
    /// corresponding to a statement in the original query string.
    pub async fn execute_query(&mut self, query: &str) -> Result<Vec<ExecutionResult>> {
        let dialect = PostgreSqlDialect {};
        let statements = Parser::parse_sql(&dialect, query)?;

        let planner = Planner::new(self);

        let mut plans = Vec::with_capacity(statements.len());
        // Not using `into_iter` + `map` + `collect` since we should exit
        // immediately after the first error instead of continuing to plan the
        // result of the statements.
        for statement in statements.into_iter() {
            let plan = planner.plan_statement(statement)?;
            plans.push(plan);
        }

        // TODO: Optimize logical

        let plans = plans
            .into_iter()
            .map(|plan| PhysicalPlan::from_logical(plan))
            .collect::<Result<Vec<_>>>()?;

        // TODO: Optimize physical

        let mut results = Vec::with_capacity(plans.len());

        // Execute everything in order (not concurrently).
        for plan in plans.into_iter() {
            let mut stream = plan.build_stream(self.tx.clone()).await?;
            let mut batches = Vec::new();
            for result in stream.next().await {
                match result {
                    Ok(batch) => batches.push(batch),
                    Err(e) => return Err(e),
                }
            }
            results.push(ExecutionResult::QueryResult { batches })
        }

        Ok(results)
    }
}

impl<S: StorageEngine> Catalog for Session<S> {
    fn create_table(&mut self, tbl: TableSchema) -> Result<()> {
        let name = tbl.reference.to_string();
        let tx = Arc::get_mut(&mut self.tx).ok_or(anyhow!("can't get mut tx"))?;
        block_on(tx.create_relation(&name, tbl.schema))?;
        Ok(())
    }

    fn drop_table(&mut self, tbl: &TableReference) -> Result<()> {
        let name = tbl.to_string();
        let tx = Arc::get_mut(&mut self.tx).ok_or(anyhow!("can't get mut tx"))?;
        Ok(())
    }

    fn get_table(&self, tbl: &TableReference) -> Result<TableSchema> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use diststore::engine::local::LocalEngine;
    use diststore::store::Store;

    #[tokio::test]
    async fn create_table() {
        let store = Store::new();
        let engine = Engine::new(LocalEngine::new(store));

        let mut sess = engine.start_session().unwrap();

        let results = sess
            .execute_query("create table test_table (a int, b int)")
            .await
            .unwrap();
        println!("results: {:?}", results);
        assert_eq!(1, results.len());
    }
}
