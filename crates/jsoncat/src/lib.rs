//! The JSON catalog.
#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::new_without_default)]
pub mod access;
pub mod catalog;
pub mod checkpoint;
pub mod constants;
pub mod dispatch;
pub mod entry;
pub mod errors;
pub mod transaction;

mod entryset;
mod system;

use catalog::Catalog;
use checkpoint::{CheckpointReader, CheckpointWriter};
use entry::{schema::SchemaEntry, table::TableEntry, view::ViewEntry};
use errors::Result;
use stablestore::StableStorage;
use std::sync::Arc;
use transaction::Context;

/// Load the catalog from some object store.
pub async fn load_catalog<C: Context, S: StableStorage>(ctx: &C, storage: S) -> Result<Catalog> {
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
    let views = ViewEntry::generate_defaults();
    for view in views {
        catalog.create_view(ctx, view)?;
    }

    let reader = CheckpointReader::new(storage, catalog);
    reader.load_from_storage(ctx).await?;
    Ok(reader.into_catalog())
}

/// Checkpoint the catalog to some object store.
///
/// Checkpointing is allowed to happen concurrently with other operations in the
/// system (user queries).
pub async fn checkpoint_catalog<C: Context, S: StableStorage>(
    ctx: &C,
    storage: S,
    catalog: Arc<Catalog>,
) -> Result<()> {
    let writer = CheckpointWriter::new(storage, catalog);
    writer.write_to_storage(ctx).await?;
    Ok(())
}
