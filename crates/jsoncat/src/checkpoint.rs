use crate::catalog::{Catalog, Schema};
use crate::entry::{schema::SchemaEntry, table::TableEntry, view::ViewEntry};
use crate::errors::Result;
use crate::transaction::Context;
use bytes::Bytes;
use object_store::{path::Path as ObjectPath, Error as ObjectError, ObjectStore};
use serde_json::{de::SliceRead, StreamDeserializer};
use std::sync::Arc;
use tracing::debug;

/// Represents a single catalog file for some type of entry.
#[derive(Debug)]
struct EntryFile(&'static str);

impl EntryFile {
    pub fn object_path(&self, db_name: &str) -> ObjectPath {
        let path = format!("{}/catalog/{}", db_name, self.0);
        ObjectPath::from(path)
    }
}

const SCHEMAS_FILE: EntryFile = EntryFile("schemas");
const TABLES_FILE: EntryFile = EntryFile("tables");
const VIEWS_FILE: EntryFile = EntryFile("views");

pub struct CheckpointReader {
    db_name: String,
    store: Arc<dyn ObjectStore>,
    catalog: Catalog,
}

impl CheckpointReader {
    pub fn new(db_name: String, store: Arc<dyn ObjectStore>, catalog: Catalog) -> CheckpointReader {
        CheckpointReader {
            db_name,
            store,
            catalog,
        }
    }

    pub fn into_catalog(self) -> Catalog {
        self.catalog
    }

    /// Load the catalog from storage.
    pub async fn load_from_storage<C: Context>(&self, ctx: &C) -> Result<()> {
        if !self.checkpoint_exists().await? {
            debug!("no checkpoint, skipping...");
            return Ok(());
        }

        self.load_schemas(ctx).await?;
        self.load_tables(ctx).await?;
        self.load_views(ctx).await?;
        Ok(())
    }

    async fn checkpoint_exists(&self) -> Result<bool> {
        debug!("checking for checkpoint");
        let result = self
            .store
            .head(&SCHEMAS_FILE.object_path(&self.db_name))
            .await;

        Ok(match result {
            Ok(_) => true,
            Err(ObjectError::NotFound { .. }) => false,
            Err(e) => return Err(e.into()),
        })
    }

    async fn load_schemas<C: Context>(&self, ctx: &C) -> Result<()> {
        debug!("loading schemas");
        let buf = self.object_bytes(&SCHEMAS_FILE).await?;

        let stream = StreamDeserializer::<_, SchemaEntry>::new(SliceRead::new(&buf));
        for result in stream {
            let ent = result?;
            self.catalog.create_schema(ctx, ent)?;
        }

        Ok(())
    }

    async fn load_tables<C: Context>(&self, ctx: &C) -> Result<()> {
        debug!("loading tables");
        let buf = self.object_bytes(&TABLES_FILE).await?;

        let stream = StreamDeserializer::<_, TableEntry>::new(SliceRead::new(&buf));
        for result in stream {
            let ent = result?;
            self.catalog.create_table(ctx, ent)?;
        }

        Ok(())
    }

    async fn load_views<C: Context>(&self, ctx: &C) -> Result<()> {
        debug!("loading views");
        let buf = self.object_bytes(&VIEWS_FILE).await?;

        let stream = StreamDeserializer::<_, ViewEntry>::new(SliceRead::new(&buf));
        for result in stream {
            let ent = result?;
            self.catalog.create_view(ctx, ent)?;
        }

        Ok(())
    }

    async fn object_bytes(&self, file: &EntryFile) -> Result<Bytes> {
        debug!(?file, "getting bytes for file");
        let buf = self
            .store
            .get(&TABLES_FILE.object_path(&self.db_name))
            .await?
            .bytes()
            .await?;
        Ok(buf)
    }
}

pub struct CheckpointWriter<'a> {
    db_name: String,
    store: Arc<dyn ObjectStore>,
    catalog: &'a Catalog,
}

impl<'a> CheckpointWriter<'a> {
    pub fn new(
        db_name: String,
        store: Arc<dyn ObjectStore>,
        catalog: &'a Catalog,
    ) -> CheckpointWriter {
        CheckpointWriter {
            db_name,
            store,
            catalog,
        }
    }

    pub async fn write_to_storage<C: Context>(&self, ctx: &C) -> Result<()> {
        let mut schema_ents: Vec<SchemaEntry> = Vec::new();
        // Write out catalog entries for each schema.
        for schema in self.catalog.schemas.iter(ctx) {
            // Don't persist internal schemas. These are recreated on startup.
            if schema.internal {
                continue;
            }

            schema_ents.push(schema.as_ref().into());

            let writer = SchemaCheckpointWriter {
                db_name: &self.db_name,
                store: &self.store,
                schema,
            };

            writer.write_tables(ctx).await?;
            writer.write_views(ctx).await?;
        }

        // Write the schema entries.
        let mut buf = Vec::new();
        for schema in schema_ents {
            serde_json::to_writer(&mut buf, &schema)?;
        }
        self.store
            .put(&SCHEMAS_FILE.object_path(&self.db_name), buf.into())
            .await?;

        Ok(())
    }
}

struct SchemaCheckpointWriter<'a> {
    db_name: &'a str,
    store: &'a Arc<dyn ObjectStore>,
    schema: Arc<Schema>,
}

impl<'a> SchemaCheckpointWriter<'a> {
    async fn write_tables<C: Context>(&self, ctx: &C) -> Result<()> {
        let mut buf = Vec::new();
        for table in self.schema.tables.iter(ctx) {
            serde_json::to_writer(&mut buf, table.as_ref())?;
        }
        self.store
            .put(&TABLES_FILE.object_path(self.db_name), buf.into())
            .await?;
        Ok(())
    }

    async fn write_views<C: Context>(&self, ctx: &C) -> Result<()> {
        let mut buf = Vec::new();
        for view in self.schema.views.iter(ctx) {
            serde_json::to_writer(&mut buf, view.as_ref())?;
        }
        self.store
            .put(&VIEWS_FILE.object_path(self.db_name), buf.into())
            .await?;
        Ok(())
    }
}
