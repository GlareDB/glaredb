use crate::context::SessionContext;
use crate::errors::{CatalogError, Result};
use crate::system::{
    builtin_types::BUILTIN_TYPES_TABLE_NAME,
    schemas::{self, SCHEMAS_TABLE_NAME},
    SystemSchema, SYSTEM_SCHEMA_ID, SYSTEM_SCHEMA_NAME,
};
use access::runtime::AccessRuntime;
use access::strategy::SinglePartitionStrategy;
use access::table::PartitionedTable;
use catalog_types::keys::{PartitionId, SchemaId, TableId, TableKey};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::catalog::{CatalogList, CatalogProvider};
use datafusion::catalog::schema::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::physical_plan::ExecutionPlan;
use futures::executor;
use futures::{Stream, StreamExt, TryStreamExt};
use std::future::Future;
use std::sync::Arc;

/// The top-level catalog.
pub struct DatabaseCatalog {
    dbname: String,
    system: Arc<SystemSchema>,
    runtime: Arc<AccessRuntime>,
}

impl DatabaseCatalog {
    /// Open up a catalog to some persistent storage.
    pub async fn open(
        dbname: impl Into<String>,
        runtime: Arc<AccessRuntime>,
    ) -> Result<DatabaseCatalog> {
        // TODO: Check system tables exist, bootstrap.
        let system = Arc::new(SystemSchema::bootstrap()?);
        Ok(DatabaseCatalog {
            dbname: dbname.into(),
            system,
            runtime,
        })
    }

    pub async fn begin(&self, sess_ctx: Arc<SessionContext>) -> Result<QueryCatalog> {
        Ok(QueryCatalog {
            dbname: self.dbname.clone(),
            sess_ctx,
            system: self.system.clone(),
            runtime: self.runtime.clone(),
        })
    }
}

/// Query catalog provides an adapter for GlareDB's core catalog with the
/// interfaces exposed by datafusion.
///
/// Note that these should be created per session.
///
/// FUTURE: This catalog will hold information to allow for transactional
/// queries.
///
/// FUTURE: There will eventually be session local catalog caching.
#[derive(Clone)]
pub struct QueryCatalog {
    dbname: String,
    sess_ctx: Arc<SessionContext>,
    system: Arc<SystemSchema>,
    runtime: Arc<AccessRuntime>,
}

impl CatalogList for QueryCatalog {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        // Purposely unimplemented, we will only ever have one catalog.
        unimplemented!()
    }

    fn catalog_names(&self) -> Vec<String> {
        vec![self.dbname.clone()]
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        if name == self.dbname {
            Some(Arc::new(self.clone()))
        } else {
            None
        }
    }
}

impl CatalogProvider for QueryCatalog {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let result = executor::block_on(schemas::scan_schema_names(
            &self.sess_ctx,
            &self.runtime,
            &self.system,
        ));
        match result {
            Ok(mut schemas) => {
                // Include built-in schemas.
                schemas.push(SYSTEM_SCHEMA_NAME.to_string());
                schemas
            }
            _other => todo!(),
        }
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        match name {
            SYSTEM_SCHEMA_NAME => Some(Arc::new(self.system.provider(self.runtime.clone()))),
            _name => {
                // User-defined schema.
                todo!()
            }
        }
    }
}

/// A wrapper around the query catalog for providing a schema.
pub struct QueryCatalogSchemaProvider {
    dbname: String,
    system: Arc<SystemSchema>,
    runtime: Arc<AccessRuntime>,
}

impl SchemaProvider for QueryCatalogSchemaProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        todo!()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        todo!()
    }

    fn table_exist(&self, name: &str) -> bool {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use access::runtime::AccessConfig;

    const TEST_DB_NAME: &str = "test_db";

    struct TestDatabase {
        database: DatabaseCatalog,
        sess_ctx: Arc<SessionContext>,
        query: QueryCatalog,
    }

    impl TestDatabase {
        async fn new() -> TestDatabase {
            let conf = AccessConfig::default();
            let runtime = Arc::new(AccessRuntime::new(conf).await.unwrap());
            let database = DatabaseCatalog::open(TEST_DB_NAME, runtime).await.unwrap();
            let sess_ctx = Arc::new(SessionContext::new());
            let query = database.begin(sess_ctx.clone()).await.unwrap();
            TestDatabase {
                database,
                sess_ctx,
                query,
            }
        }
    }

    #[tokio::test]
    async fn list_builtin_schemas() {
        logutil::init_test();

        let db = TestDatabase::new().await;
        let catalog = db.query.catalog(TEST_DB_NAME).unwrap();
        let mut schemas = catalog.schema_names();
        let mut expected = vec![SYSTEM_SCHEMA_NAME.to_string()];

        schemas.sort();
        expected.sort();

        assert_eq!(expected, schemas);
    }

    #[tokio::test]
    async fn list_tables_in_system_schema() {
        logutil::init_test();

        let db = TestDatabase::new().await;
        let catalog = db.query.catalog(TEST_DB_NAME).unwrap();
        let system = catalog.schema(SYSTEM_SCHEMA_NAME).unwrap();

        let tables = system.table_names();
        assert!(!tables.is_empty());
    }
}
