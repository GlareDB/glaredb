use crate::errors::{CatalogError, Result};
use crate::system::{schemas, SystemSchema, SYSTEM_SCHEMA_NAME};
use access::runtime::AccessRuntime;
use async_trait::async_trait;
use catalog_types::context::SessionContext;
use catalog_types::interfaces::MutableCatalogProvider;
use datafusion::catalog::catalog::{CatalogList, CatalogProvider};
use datafusion::catalog::schema::SchemaProvider;
use datafusion::datasource::TableProvider;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::task;

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
        // TODO: Execute this on a dedicated per-session thread.
        let result = task::block_in_place(move || {
            Handle::current().block_on(schemas::scan_schema_names(
                &self.sess_ctx,
                &self.runtime,
                &self.system,
            ))
        });
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

#[async_trait]
impl MutableCatalogProvider for QueryCatalog {
    type Error = CatalogError;

    async fn create_schema(&self, ctx: &SessionContext, name: &str) -> Result<()> {
        schemas::insert_schema(ctx, &self.runtime, &self.system, name).await?;
        Ok(())
    }

    async fn drop_schema(&self, ctx: &SessionContext, name: &str) -> Result<()> {
        todo!()
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
    use common::access::AccessConfig;

    // Note that all tests need to use a multi-threaded runtime. This is because
    // we're using `block_on` to make certain things async. A per-session
    // runtime may or may not remove this requirement.

    const TEST_DB_NAME: &str = "test_db";

    struct TestDatabase {
        database: DatabaseCatalog,
        sess_ctx: Arc<SessionContext>,
        query: QueryCatalog,
    }

    impl TestDatabase {
        async fn new() -> TestDatabase {
            let conf = AccessConfig::default();
            let runtime = Arc::new(AccessRuntime::new(Arc::new(conf)).await.unwrap());
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn list_tables_in_system_schema() {
        logutil::init_test();

        let db = TestDatabase::new().await;
        let catalog = db.query.catalog(TEST_DB_NAME).unwrap();
        let system = catalog.schema(SYSTEM_SCHEMA_NAME).unwrap();

        let tables = system.table_names();
        assert!(!tables.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_schema() {
        logutil::init_test();

        let db = TestDatabase::new().await;
        db.query
            .create_schema(&db.sess_ctx, "test_schema_1")
            .await
            .unwrap();
        db.query
            .create_schema(&db.sess_ctx, "test_schema_2")
            .await
            .unwrap();

        let catalog = db.query.catalog(TEST_DB_NAME).unwrap();
        let schemas = catalog.schema_names();

        assert!(schemas.contains(&"test_schema_1".to_string()));
        assert!(schemas.contains(&"test_schema_2".to_string()));
    }
}
