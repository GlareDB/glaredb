use crate::entryset::EntrySet;
use crate::errors::{CatalogError, Result};
use async_trait::async_trait;
use futures::Stream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub trait Context: Sync + Send {}

#[derive(Debug)]
pub struct TableEntry {}

#[derive(Debug)]
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
    tables: EntrySet<TableEntry>,
    views: EntrySet<ViewEntry>,
}

impl Schema {
    fn create_view<C: Context>(&self, ctx: &C, view: ViewEntry) -> Result<()> {
        self.views
            .create_entry(ctx, view.name.clone(), view)?
            .expect_inserted()
    }
}
