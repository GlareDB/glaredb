#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::new_without_default)]
pub mod access;
pub mod builtins;
pub mod constants;
pub mod entry;
pub mod errors;
pub mod transaction;

mod entryset;

use builtins::{BuiltinSchema, BuiltinTable, BuiltinView};
use entry::{ConnectionEntry, DropEntry, EntryType, SchemaEntry, TableEntry, ViewEntry};
use entryset::EntrySet;
use errors::{internal, CatalogError, Result};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use transaction::CatalogContext;
use transaction::StubCatalogContext;

/// The in-memory catalog.
pub struct Catalog {
    /// Catalog version, incremented on change.
    pub(crate) version: AtomicU64, // TODO: Currently unused. This will be used to assign object ids.
    /// Catalog schemas.
    pub(crate) schemas: EntrySet<PhysicalSchema>,
}

impl Catalog {
    /// Open a catalog.
    pub fn open() -> Result<Catalog> {
        let catalog = Catalog {
            version: AtomicU64::new(0),
            schemas: EntrySet::new(),
        };

        let ctx = &StubCatalogContext;

        for schema in BuiltinSchema::builtins() {
            catalog.create_schema(ctx, schema.into())?;
        }
        for table in BuiltinTable::builtins() {
            catalog.create_table(ctx, table.into())?;
        }
        for view in BuiltinView::builtins() {
            catalog.create_view(ctx, view.into())?;
        }

        // TODO: We'll eventually load from storage here.

        Ok(catalog)
    }

    pub fn create_view<C: CatalogContext>(&self, ctx: &C, view: ViewEntry) -> Result<()> {
        let schema = self.get_schema(ctx, &view.schema)?;
        schema.create_view(ctx, view)
    }

    pub fn create_table<C: CatalogContext>(&self, ctx: &C, table: TableEntry) -> Result<()> {
        let schema = self.get_schema(ctx, &table.schema)?;
        schema.create_table(ctx, table)
    }

    pub fn create_schema<C: CatalogContext>(&self, ctx: &C, schema_ent: SchemaEntry) -> Result<()> {
        let schema = PhysicalSchema {
            name: schema_ent.name.clone(),
            connections: EntrySet::new(),
            views: EntrySet::new(),
            tables: EntrySet::new(),
        };
        self.schemas
            .create_entry(ctx, schema_ent.name, schema)?
            .expect_inserted()
    }

    pub fn create_connection<C: CatalogContext>(
        &self,
        ctx: &C,
        conn: ConnectionEntry,
    ) -> Result<()> {
        let schema = self.get_schema(ctx, &conn.schema)?;
        schema.create_connection(ctx, conn)
    }

    /// Check if a schema exists.
    pub fn schema_exists<C: CatalogContext>(&self, ctx: &C, schema: &str) -> bool {
        self.schemas.get_entry(ctx, schema).is_some()
    }

    pub fn drop_entry<C: CatalogContext>(&self, ctx: &C, drop: DropEntry) -> Result<()> {
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

    fn get_schema<C: CatalogContext>(&self, ctx: &C, name: &str) -> Result<Arc<PhysicalSchema>> {
        unimplemented!()
        // self.schemas
        //     .get_entry(ctx, name)
        //     .ok_or(CatalogError::MissingEntry {
        //         typ: "schema",
        //         name: name.to_string(),
        //     })
    }
}

pub(crate) struct PhysicalSchema {
    pub(crate) name: String, // TODO: Don't store name here.
    pub(crate) connections: EntrySet<ConnectionEntry>,
    pub(crate) views: EntrySet<ViewEntry>,
    pub(crate) tables: EntrySet<TableEntry>,
}

impl PhysicalSchema {
    fn create_view<C: CatalogContext>(&self, ctx: &C, view: ViewEntry) -> Result<()> {
        self.views
            .create_entry(ctx, view.name.clone(), view)?
            .expect_inserted()
    }

    fn create_table<C: CatalogContext>(&self, ctx: &C, table: TableEntry) -> Result<()> {
        self.tables
            .create_entry(ctx, table.name.clone(), table)?
            .expect_inserted()
    }

    fn create_connection<C: CatalogContext>(&self, ctx: &C, conn: ConnectionEntry) -> Result<()> {
        self.connections
            .create_entry(ctx, conn.name.clone(), conn)?
            .expect_inserted()
    }

    fn drop_entry<C: CatalogContext>(&self, ctx: &C, drop: DropEntry) -> Result<()> {
        match drop.typ {
            EntryType::View => self.views.drop_entry(ctx, &drop.name)?,
            EntryType::Table => self.tables.drop_entry(ctx, &drop.name)?,
            other => return Err(internal!("invalid drop type: {:?}", other)),
        };
        Ok(())
    }
}
