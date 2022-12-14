//! The JSON catalog.
pub mod access;
pub mod adapter;
pub mod catalog;
pub mod checkpoint;
pub mod entry;
pub mod errors;
pub mod transaction;

mod convert;
mod entryset;
mod system;

use catalog::{Catalog, Context};
use checkpoint::{CheckpointReader, CheckpointWriter};
use entry::{schema::SchemaEntry, table::TableEntry, view::ViewEntry};
use errors::Result;
use object_store::ObjectStore;
use std::sync::Arc;

pub async fn load_catalog<C: Context>(
    ctx: &C,
    db_name: String,
    store: Arc<dyn ObjectStore>,
) -> Result<Catalog> {
    let catalog = Catalog::empty();

    // Insert defaults.
    let schemas = SchemaEntry::generate_defaults();
    for schema in schemas {
        catalog.create_schema(ctx, schema)?;
    }
    let tables = TableEntry::generate_defaults();
    for table in tables {
        catalog.create_table(ctx, table)?;
    }

    let reader = CheckpointReader::new(db_name, store, catalog);
    reader.load_from_storage(ctx).await?;
    Ok(reader.into_catalog())
}

pub async fn checkpoint_catalog<C: Context>(
    ctx: &C,
    db_name: String,
    store: Arc<dyn ObjectStore>,
    catalog: &Catalog,
) -> Result<()> {
    let writer = CheckpointWriter::new(db_name, store, catalog);
    writer.write_to_storage(ctx).await?;
    Ok(())
}
