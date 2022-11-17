use crate::errors::{internal, CatalogError, Result};
use crate::system::{
    attributes::AttributeRows, relations::RelationRow, schemas::SchemaRow, SystemSchema,
    SYSTEM_SCHEMA_NAME,
};
use access::runtime::AccessRuntime;
use access::strategy::SinglePartitionStrategy;
use access::table::PartitionedTable;
use async_trait::async_trait;
use catalog_types::context::SessionContext;
use catalog_types::interfaces::{MutableCatalogProvider, MutableSchemaProvider};
use catalog_types::keys::{SchemaId, TableKey};
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
        let system = SystemSchema::new()?;
        system.bootstrap(&runtime).await?;
        Ok(DatabaseCatalog {
            dbname: dbname.into(),
            system: Arc::new(system),
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
            id: self.system.next_id(ctx, &self.runtime).await? as u32,
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
        // TODO: Execute this on a dedicated per-session thread.
        let result: Result<_> = task::block_in_place(move || {
            Handle::current().block_on(async move {
                let relation = RelationRow::scan_one_by_name(
                    &self.sess_ctx,
                    &self.runtime,
                    &self.system,
                    self.schema,
                    name,
                )
                .await?;
                match relation {
                    Some(relation) => {
                        let attrs = AttributeRows::scan_for_table(
                            &self.sess_ctx,
                            &self.runtime,
                            &self.system,
                            self.schema,
                            relation.table_id,
                        )
                        .await?;
                        let arrow_schema: Schema = attrs.try_into()?;
                        let key = TableKey {
                            schema_id: self.schema,
                            table_id: relation.table_id,
                        };
                        let table = PartitionedTable::new(
                            key,
                            Box::new(SinglePartitionStrategy),
                            self.runtime.clone(),
                            Arc::new(arrow_schema),
                        );
                        Ok(Some(table))
                    }
                    None => Ok(None),
                }
            })
        });
        match result {
            Ok(Some(table)) => Some(Arc::new(table)),
            Ok(None) => None,
            err => {
                // TODO: Handle
                panic!("{:?}", err);
            }
        }
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
        let table_id = self.system.next_id(ctx, &self.runtime).await? as u32;
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
    use datafusion::arrow::datatypes::{DataType, Field};

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_table() {
        logutil::init_test();
        let db = TestDatabase::new().await;

        db.query
            .create_schema(&db.sess_ctx, "my_schema")
            .await
            .unwrap();

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("f1", DataType::UInt32, false),
            Field::new("f2", DataType::UInt32, true),
            Field::new("f3", DataType::Utf8, false),
        ]));
        let schema = db.query.user_schema("my_schema").await.unwrap();
        schema
            .create_table(&db.sess_ctx, "my_table", &arrow_schema)
            .await
            .unwrap();

        // Shouldn't be able to get table that doesn't exist.
        let got = schema.table("nope");
        assert!(got.is_none());

        // Should get the table we just created.
        let table = schema.table("my_table").unwrap();
        let got_schema = table.schema();
        assert_eq!(arrow_schema, got_schema);
    }
}
