#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::new_without_default)]
pub mod access;
pub mod checkpoint;
pub mod constants;
pub mod dispatch;
pub mod entry;
pub mod errors;
pub mod transaction;

mod entryset;
mod system;

use checkpoint::{CheckpointReader, CheckpointWriter};
use entry::{schema::SchemaEntry, table::TableEntry, view::ViewEntry};
use entryset::EntrySet;
use errors::{internal, CatalogError, Result};
use stablestore::StableStorage;
use std::sync::atomic::AtomicU64;
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

/// Types of entries the catalog holds.
#[derive(Debug)]
pub enum EntryType {
    Schema,
    Table,
    View,
    Index,
    Sequence,
}

#[derive(Debug)]
pub struct DropEntry {
    /// Entry type to drop.
    pub typ: EntryType,
    /// Schema the entry is in.
    pub schema: String,
    /// Name of the entry to drop. If dropping a schema, must be the same name
    /// as the schema.
    pub name: String,
}

/// The in-memory catalog.
pub struct Catalog {
    /// Catalog version, incremented on change.
    pub(crate) version: AtomicU64, // TODO: Currently unused. This will be used to assign object ids.
    /// Catalog schemas.
    pub(crate) schemas: EntrySet<PhysicalSchema>,
}

impl Catalog {
    /// Create an empty catalog.
    pub fn empty() -> Catalog {
        Catalog {
            version: AtomicU64::new(0),
            schemas: EntrySet::new(),
        }
    }

    pub fn create_view<C: Context>(&self, ctx: &C, view: ViewEntry) -> Result<()> {
        let schema = self.get_schema(ctx, &view.schema)?;
        schema.create_view(ctx, view)
    }

    pub fn create_table<C: Context>(&self, ctx: &C, table: TableEntry) -> Result<()> {
        let schema = self.get_schema(ctx, &table.schema)?;
        schema.create_table(ctx, table)
    }

    pub fn create_schema<C: Context>(&self, ctx: &C, schema_ent: SchemaEntry) -> Result<()> {
        let schema = PhysicalSchema {
            name: schema_ent.schema.clone(),
            views: EntrySet::new(),
            tables: EntrySet::new(),
            internal: schema_ent.internal,
        };
        self.schemas
            .create_entry(ctx, schema_ent.schema, schema)?
            .expect_inserted()
    }

    /// Check if a schema exists.
    pub fn schema_exists<C: Context>(&self, ctx: &C, schema: &str) -> bool {
        self.schemas.get_entry(ctx, schema).is_some()
    }

    pub fn drop_entry<C: Context>(&self, ctx: &C, drop: DropEntry) -> Result<()> {
        if matches!(drop.typ, EntryType::Schema) {
            if drop.schema != drop.name {
                return Err(internal!(
                    "dropping a schema requires 'schema' and 'name' to match"
                ));
            }
            self.schemas.drop_entry(ctx, &drop.schema)?;
        } else {
            let schema = self.get_schema(ctx, &drop.schema)?;
            schema.drop_entry(ctx, drop)?;
        }
        Ok(())
    }

    fn get_schema<C: Context>(&self, ctx: &C, name: &str) -> Result<Arc<PhysicalSchema>> {
        self.schemas
            .get_entry(ctx, name)
            .ok_or(CatalogError::MissingEntry {
                typ: "schema",
                name: name.to_string(),
            })
    }
}

pub(crate) struct PhysicalSchema {
    pub(crate) name: String, // TODO: Don't store name here.
    pub(crate) internal: bool,
    pub(crate) views: EntrySet<ViewEntry>,
    pub(crate) tables: EntrySet<TableEntry>,
}

impl PhysicalSchema {
    fn create_view<C: Context>(&self, ctx: &C, view: ViewEntry) -> Result<()> {
        self.views
            .create_entry(ctx, view.name.clone(), view)?
            .expect_inserted()
    }

    fn create_table<C: Context>(&self, ctx: &C, table: TableEntry) -> Result<()> {
        self.tables
            .create_entry(ctx, table.name.clone(), table)?
            .expect_inserted()
    }

    fn drop_entry<C: Context>(&self, ctx: &C, drop: DropEntry) -> Result<()> {
        match drop.typ {
            EntryType::View => self.views.drop_entry(ctx, &drop.name)?,
            EntryType::Table => self.tables.drop_entry(ctx, &drop.name)?,
            other => return Err(internal!("invalid drop type: {:?}", other)),
        };
        Ok(())
    }
}

impl From<&PhysicalSchema> for SchemaEntry {
    fn from(s: &PhysicalSchema) -> Self {
        SchemaEntry {
            schema: s.name.clone(),
            internal: s.internal,
        }
    }
}
