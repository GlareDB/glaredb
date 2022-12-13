use crate::entryset::EntrySet;
use crate::errors::{internal, CatalogError, Result};
use async_trait::async_trait;
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub trait Context: Sync + Send {}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableEntry {
    pub schema: String,
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ViewEntry {
    pub schema: String,
    pub name: String,
}

// #[async_trait]
// pub trait Catalog: Sync + Send {
//     type Context: Context;

//     async fn create_schema(&self, ctx: &Self::Context, info: &CreateSchemaInfo) -> Result<()>;
//     async fn create_table(&self, ctx: &Self::Context, info: &CreateSchemaInfo) -> Result<()>;
//     async fn create_view(&self, ctx: &Self::Context, info: &CreateViewInfo) -> Result<()>;
//     async fn create_sequence(&self, ctx: &Self::Context, info: &CreateSequenceInfo) -> Result<()>;

//     async fn iter_schemas<'a, S: Stream<Item = &'a SchemaEntry>>(&self) -> Result<S>;

//     async fn drop(&self, ctx: &Self::Context, info: &DropInfo) -> Result<()>;
// }

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
    version: AtomicU64,
    /// Catalog schemas.
    schemas: EntrySet<Schema>,
}

impl Catalog {
    pub fn create_view<C: Context>(&self, ctx: &C, view: ViewEntry) -> Result<()> {
        let schema = self.get_schema(ctx, &view.schema)?;
        schema.create_view(ctx, view)
    }

    pub fn create_table<C: Context>(&self, ctx: &C, table: TableEntry) -> Result<()> {
        let schema = self.get_schema(ctx, &table.schema)?;
        schema.create_table(ctx, table)
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

struct Schema {
    views: EntrySet<ViewEntry>,
    tables: EntrySet<TableEntry>,
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
