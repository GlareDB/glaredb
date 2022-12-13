use crate::catalog::{Catalog, Context, TableEntry, ViewEntry};
use crate::errors::{CatalogError, Result};
use bytes::Bytes;
use object_store::{path::Path as ObjectPath, Error as ObjectError, ObjectStore};
use serde::{Deserialize, Serialize};
use serde_json::{de::SliceRead, StreamDeserializer};
use std::sync::Arc;
use tracing::debug;

/// Represents a single catalog file for some type of entry.
#[derive(Debug)]
struct CatalogFile(&'static str);

impl CatalogFile {
    pub fn object_path(&self, db_name: &str) -> ObjectPath {
        let path = format!("{}/catalog/{}", db_name, self.0);
        ObjectPath::from(path)
    }
}

const CHECKPOINT_FILE: CatalogFile = CatalogFile("checkpoint");
const TABLES_FILE: CatalogFile = CatalogFile("tables");
const VIEWS_FILE: CatalogFile = CatalogFile("views");

#[derive(Debug, Serialize, Deserialize)]
pub struct CheckpointMarker {}

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

        self.load_tables(ctx).await?;
        self.load_views(ctx).await?;
        Ok(())
    }

    async fn checkpoint_exists(&self) -> Result<bool> {
        debug!("checking for checkpoint");
        let result = self
            .store
            .head(&CHECKPOINT_FILE.object_path(&self.db_name))
            .await;

        Ok(match result {
            Ok(_) => true,
            Err(ObjectError::NotFound { .. }) => false,
            Err(e) => return Err(e.into()),
        })
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

    async fn object_bytes(&self, file: &CatalogFile) -> Result<Bytes> {
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
