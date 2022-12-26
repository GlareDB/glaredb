use crate::catalog::{Catalog, PhysicalSchema};
use crate::entry::{schema::SchemaEntry, table::TableEntry, view::ViewEntry};
use crate::errors::{internal, Result};
use crate::transaction::Context;
use serde::{Deserialize, Serialize};
use stablestore::{Blob, StableStorage, VersionReadOption};
use std::sync::Arc;
use tracing::debug;

const CHECKPOINT_INFO_BLOB_NAME: &str = "checkpoint";

#[derive(Debug, Serialize, Deserialize)]
struct SerializedCheckpointInfo {}

impl Blob for SerializedCheckpointInfo {}

const SCHEMAS_BLOB_NAME: &str = "catalog/schemas";
const TABLES_BLOB_NAME: &str = "catalog/tables";
const VIEWS_BLOB_NAME: &str = "catalog/views";

#[derive(Debug, Serialize, Deserialize)]
struct SerializedSchemas {
    schemas: Vec<SchemaEntry>,
}

impl Blob for SerializedSchemas {}

#[derive(Debug, Serialize, Deserialize)]
struct SerializedTables {
    tables: Vec<TableEntry>,
}

impl Blob for SerializedTables {}

#[derive(Debug, Serialize, Deserialize)]
struct SerializedViews {
    views: Vec<ViewEntry>,
}

impl Blob for SerializedViews {}

/// Reads a catalog checkpoint from some underlying stable storage.
pub struct CheckpointReader<S> {
    catalog: Catalog,
    storage: S,
}

impl<S: StableStorage> CheckpointReader<S> {
    pub fn new(storage: S, catalog: Catalog) -> CheckpointReader<S> {
        CheckpointReader { storage, catalog }
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
        debug!("checking if checkpoint exists");

        let v: Option<SerializedCheckpointInfo> = self
            .storage
            .read(CHECKPOINT_INFO_BLOB_NAME, VersionReadOption::Latest)
            .await?;

        Ok(v.is_some())
    }

    async fn load_schemas<C: Context>(&self, ctx: &C) -> Result<()> {
        debug!("loading schemas");

        let v: SerializedSchemas = self
            .storage
            .read(SCHEMAS_BLOB_NAME, VersionReadOption::Latest)
            .await?
            .ok_or_else(|| internal!("missing serialized schemas"))?;

        for schema in v.schemas {
            self.catalog.create_schema(ctx, schema)?;
        }

        Ok(())
    }

    async fn load_tables<C: Context>(&self, ctx: &C) -> Result<()> {
        debug!("loading tables");

        let v: SerializedTables = self
            .storage
            .read(TABLES_BLOB_NAME, VersionReadOption::Latest)
            .await?
            .ok_or_else(|| internal!("missing serialized tables"))?;

        for table in v.tables {
            self.catalog.create_table(ctx, table)?;
        }

        Ok(())
    }

    async fn load_views<C: Context>(&self, ctx: &C) -> Result<()> {
        debug!("loading views");

        let v: SerializedViews = self
            .storage
            .read(VIEWS_BLOB_NAME, VersionReadOption::Latest)
            .await?
            .ok_or_else(|| internal!("missing serialized views"))?;

        for view in v.views {
            self.catalog.create_view(ctx, view)?;
        }

        Ok(())
    }
}

pub struct CheckpointWriter<S> {
    storage: S,
    catalog: Arc<Catalog>,
}

impl<S: StableStorage> CheckpointWriter<S> {
    pub fn new(storage: S, catalog: Arc<Catalog>) -> CheckpointWriter<S> {
        CheckpointWriter { storage, catalog }
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
                storage: &self.storage,
                schema,
            };

            writer.write_tables(ctx).await?;
            writer.write_views(ctx).await?;
        }

        // Write the schema entries.
        let v = SerializedSchemas {
            schemas: schema_ents,
        };
        self.storage.append(SCHEMAS_BLOB_NAME, &v).await?;

        // Write out checkpoint into.
        self.storage
            .append(CHECKPOINT_INFO_BLOB_NAME, &SerializedCheckpointInfo {})
            .await?;

        Ok(())
    }
}

struct SchemaCheckpointWriter<'a, S> {
    storage: &'a S,
    schema: Arc<PhysicalSchema>,
}

impl<'a, S: StableStorage> SchemaCheckpointWriter<'a, S> {
    async fn write_tables<C: Context>(&self, ctx: &C) -> Result<()> {
        let tables: Vec<_> = self
            .schema
            .tables
            .iter(ctx)
            .map(|ent| ent.as_ref().clone())
            .collect();
        let v = SerializedTables { tables };
        self.storage.append(TABLES_BLOB_NAME, &v).await?;
        Ok(())
    }

    async fn write_views<C: Context>(&self, ctx: &C) -> Result<()> {
        let views: Vec<_> = self
            .schema
            .views
            .iter(ctx)
            .map(|ent| ent.as_ref().clone())
            .collect();
        let v = SerializedViews { views };
        self.storage.append(VIEWS_BLOB_NAME, &v).await?;
        Ok(())
    }
}
