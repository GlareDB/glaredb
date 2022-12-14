use crate::entry::{schema::SchemaEntry, table::TableEntry, view::ViewEntry};
use crate::entryset::EntrySet;
use crate::errors::{internal, CatalogError, Result};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub trait Context: Sync + Send + Clone {}

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
    pub typ: EntryType,
    pub schema: String,
    pub name: String,
}

pub struct Catalog {
    /// Catalog version, incremented on change.
    pub(crate) version: AtomicU64,
    /// Catalog schemas.
    pub(crate) schemas: EntrySet<Schema>,
}

impl Catalog {
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
        let schema = Schema {
            name: schema_ent.schema.clone(),
            views: EntrySet::new(),
            tables: EntrySet::new(),
            internal: schema_ent.internal,
        };
        self.schemas
            .create_entry(ctx, schema_ent.schema, schema)?
            .expect_inserted()
    }

    pub fn drop_entry<C: Context>(&self, ctx: &C, drop: DropEntry) -> Result<()> {
        if matches!(drop.typ, EntryType::Schema) {
            self.schemas.drop_entry(ctx, &drop.schema)?;
        } else {
            let schema = self.get_schema(ctx, &drop.schema)?;
            schema.drop_entry(ctx, drop)?;
        }
        Ok(())
    }

    fn get_schema<C: Context>(&self, ctx: &C, name: &str) -> Result<Arc<Schema>> {
        self.schemas
            .get_entry(ctx, name)
            .ok_or(CatalogError::MissingEntry {
                typ: "schema",
                name: name.to_string(),
            })
    }
}

pub(crate) struct Schema {
    pub(crate) name: String, // TODO: Don't store name here.
    pub(crate) internal: bool,
    pub(crate) views: EntrySet<ViewEntry>,
    pub(crate) tables: EntrySet<TableEntry>,
}

impl Schema {
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

impl From<&Schema> for SchemaEntry {
    fn from(s: &Schema) -> Self {
        SchemaEntry {
            schema: s.name.clone(),
            internal: s.internal,
        }
    }
}
