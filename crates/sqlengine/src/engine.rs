use crate::catalog::{Catalog, ResolvedTableReference, TableReference, TableSchema};
use crate::logical::RelationalPlan;
use crate::physical::PhysicalPlan;
use crate::planner::Planner;
use crate::system::{self, system_tables, SystemTable};
use anyhow::{anyhow, Result};
use coretypes::batch::{Batch, BatchRepr};
use coretypes::datatype::{DataValue, Row};
use coretypes::stream::BatchStream;
use diststore::engine::{Interactivity, StorageEngine, StorageTransaction};
use futures::executor;
use futures::stream::StreamExt;
use log::info;
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
        Ok(Session { tx })
    }

    fn ensure_system_tables(&mut self) -> Result<()> {
        let tx = self.storage.begin(Interactivity::None)?;
        executor::block_on(async move {
            for table in system_tables().into_iter() {
                let name = table.resolved_reference().to_string();
                info!("check system table: {}", name);
                let schema = tx.get_relation(&name).await?;
                if schema.is_none() {
                    info!("creating system table: {}", name);
                    tx.create_relation(&name, table.generate_relation_schema())
                        .await?;
                }
            }
            Ok(())
        })
    }
}

#[derive(Debug)]
pub enum ExecutionResult {
    Other,
    QueryResult { batches: Vec<Batch> },
}

#[derive(Debug)]
pub struct Session<S: StorageEngine> {
    tx: S::Transaction,
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
            match plan.execute_stream(&self.tx).await? {
                Some(mut stream) => {
                    let mut batches = Vec::new();
                    for result in stream.next().await {
                        match result {
                            Ok(batch) => batches.push(batch),
                            Err(e) => return Err(e),
                        }
                    }
                    results.push(ExecutionResult::QueryResult {
                        batches: batches
                            .into_iter()
                            .map(|batch| batch.into_shrunk_batch())
                            .collect(),
                    })
                }
                None => {
                    results.push(ExecutionResult::Other);
                }
            };
        }

        Ok(results)
    }

    pub fn current_schema(&self) -> &str {
        "public"
    }

    pub fn current_database(&self) -> &str {
        "glaredb"
    }
}

impl<S: StorageEngine + 'static> Catalog for Session<S> {
    fn get_table(&self, tbl: &TableReference) -> Result<TableSchema> {
        executor::block_on(async move {
            let resolved = tbl
                .clone()
                .resolve_with_defaults(self.current_database(), self.current_schema());
        });

        unimplemented!()
    }

    fn create_table(&mut self, tbl: TableSchema) -> Result<()> {
        executor::block_on(async move {
            // Insert column names (and eventually other stuff) into the
            // attributes table.
            let mut col_names = tbl.columns.into_iter();
            let first_col = col_names
                .next()
                .ok_or(anyhow!("table must have at least one column"))?;
            let mut row: Row = vec![
                DataValue::Utf8(tbl.reference.catalog.clone()),
                DataValue::Utf8(tbl.reference.schema.clone()),
                DataValue::Utf8(tbl.reference.base.clone()),
                DataValue::Utf8(first_col),
            ]
            .into();

            // TODO: Bulk inserts.
            // TODO: More efficient table identifiers.
            let attrs_table = system::Attributes;
            let attrs_ref = attrs_table.resolved_reference().to_string();
            self.tx.insert(&attrs_ref, &row).await?;

            let col_name_idx = 3;
            for col_name in col_names {
                row.0[col_name_idx] = DataValue::Utf8(col_name); // TODO: Reuse original string.
                self.tx.insert(&attrs_ref, &row).await?;
            }

            // Create the actual relation.
            let name = tbl.reference.to_string();
            let schema = tbl.schema;
            self.tx.create_relation(&name, schema).await?;

            Ok(())
        })
    }

    fn drop_table(&mut self, tbl: &TableReference) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use diststore::engine::local::LocalEngine;
    use diststore::store::Store;

    #[tokio::test]
    async fn basic_queries() {
        let store = Store::new();
        let engine = Engine::new(LocalEngine::new(store));

        let mut sess = engine.start_session().unwrap();

        let query = r#"
            create table test_table (a int, b int);
            insert into test_table (a, b) values (1, 2);
            select * from test_table;
        "#;

        let results = sess.execute_query(query).await.unwrap();
        println!("results: {:?}", results);
        assert_eq!(3, results.len());
    }
}
