use crate::errors::{internal, CatalogError, Result};
use crate::system::{
    attributes::AttributeRows, relations::RelationRow, schemas::SchemaRow, sequences, SystemSchema,
    SYSTEM_SCHEMA_NAME,
};
use access::runtime::AccessRuntime;
use async_trait::async_trait;
use catalog_types::context::SessionContext;
use catalog_types::interfaces::{MutableCatalogProvider, MutableSchemaProvider};
use catalog_types::keys::SchemaId;
use datafusion::arrow::datatypes::Schema;
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

impl QueryCatalog {
    pub async fn user_schema(&self, name: &str) -> Result<QueryCatalogSchemaProvider> {
        let schema =
            match SchemaRow::scan_by_name(&self.sess_ctx, &self.runtime, &self.system, name).await?
            {
                Some(schema) => schema,
                None => return Err(internal!("missing schema for name: {}", name)),
            };
        Ok(QueryCatalogSchemaProvider {
            dbname: self.dbname.clone(),
            schema: schema.id,
            sess_ctx: self.sess_ctx.clone(),
            system: self.system.clone(),
            runtime: self.runtime.clone(),
        })
    }
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
        let result: Result<_> = task::block_in_place(move || {
            Handle::current().block_on(async move {
                let schemas =
                    SchemaRow::scan_many(&self.sess_ctx, &self.runtime, &self.system).await?;
                let names: Vec<_> = schemas.into_iter().map(|row| row.name).collect();
                Ok(names)
            })
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
        let schema = SchemaRow {
            id: sequences::dummy_next() as u32, // TODO
            name: name.to_string(),
        };
        schema
            .insert(&self.sess_ctx, &self.runtime, &self.system)
            .await?;
        Ok(())
    }

    async fn drop_schema(&self, ctx: &SessionContext, name: &str) -> Result<()> {
        todo!()
    }
}

/// A wrapper around the query catalog for providing a schema.
pub struct QueryCatalogSchemaProvider {
    dbname: String,
    schema: SchemaId,
    sess_ctx: Arc<SessionContext>,
    system: Arc<SystemSchema>,
    runtime: Arc<AccessRuntime>,
}

impl SchemaProvider for QueryCatalogSchemaProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        // TODO: Execute this on a dedicated per-session thread.
        let result: Result<_> = task::block_in_place(move || {
            Handle::current().block_on(async move {
                let relations = RelationRow::scan_many_in_schema(
                    &self.sess_ctx,
                    &self.runtime,
                    &self.system,
                    self.schema,
                )
                .await?;
                let names: Vec<_> = relations.into_iter().map(|row| row.table_name).collect();
                Ok(names)
            })
        });
        match result {
            Ok(names) => names,
            err => {
                // Where to store error?
                panic!("{:?}", err);
            }
        }
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        todo!()
    }

    fn table_exist(&self, name: &str) -> bool {
        // TODO: Be more efficient, query the system table directly.
        self.table_names().contains(&String::from(name))
    }
}

#[async_trait]
impl MutableSchemaProvider for QueryCatalogSchemaProvider {
    type Error = CatalogError;

    async fn create_table(&self, ctx: &SessionContext, name: &str, schema: &Schema) -> Result<()> {
        let table_id = sequences::dummy_next() as u32; // TODO
        let relation = RelationRow {
            schema_id: self.schema,
            table_id,
            table_name: name.to_string(),
        };
        relation
            .insert(&self.sess_ctx, &self.runtime, &self.system)
            .await?;
        let attrs = AttributeRows::try_from_arrow_schema(self.schema, table_id, schema)?;
        attrs
            .insert_all(&self.sess_ctx, &self.runtime, &self.system)
            .await?;
        Ok(())
    }

    async fn drop_table(&self, ctx: &SessionContext, name: &str) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::access::AccessConfig;

    // NOTE: All tests need to use a multi-threaded runtime. This is because
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_table() {
        logutil::init_test();
        let db = TestDatabase::new().await;

        db.query
            .create_schema(&db.sess_ctx, "my_schema_1")
            .await
            .unwrap();
        db.query
            .create_schema(&db.sess_ctx, "my_schema_2")
            .await
            .unwrap();

        let schema1 = db.query.user_schema("my_schema_1").await.unwrap();
        schema1
            .create_table(&db.sess_ctx, "table_1", &Schema::empty())
            .await
            .unwrap();

        let schema2 = db.query.user_schema("my_schema_2").await.unwrap();
        schema2
            .create_table(&db.sess_ctx, "table_2", &Schema::empty())
            .await
            .unwrap();

        let tables = schema1.table_names();
        assert!(tables.contains(&String::from("table_1")));
        assert!(!tables.contains(&String::from("table_2")));
    }
}
