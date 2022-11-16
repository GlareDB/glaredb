//! Common catalog related interfaces.
//!
//! These interfaces provide mutable analogues to datafusion's catalog related
//! interfaces. These exist outside the `catalog` crate to avoid cyclic
//! dependencies. Specifically `catalog` depends on `access`, and types in
//! `access` implement these interfaces to make them available to `catalog`.
//!
//! Datafusion's interfaces may be evolving in the future. We'll want to watch
//! out for <https://github.com/apache/arrow-datafusion/issues/3777> and
//! incorporate associated changes from that.
use crate::context::SessionContext;
use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::catalog::CatalogProvider;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::datasource::TableProvider;

#[async_trait]
pub trait MutableCatalogProvider: CatalogProvider {
    type Error;

    /// Create a schema with the given name.
    async fn create_schema(&self, ctx: &SessionContext, name: &str) -> Result<(), Self::Error>;

    /// Drop the schema with the given name.
    async fn drop_schema(&self, ctx: &SessionContext, name: &str) -> Result<(), Self::Error>;
}

#[async_trait]
pub trait MutableSchemaProvider: SchemaProvider {
    type Error;

    /// Create a table with the given name and schema.
    async fn create_table(
        &self,
        ctx: &SessionContext,
        name: &str,
        schema: &Schema,
    ) -> Result<(), Self::Error>;

    /// Drop the table with the given name.
    async fn drop_table(&self, ctx: &SessionContext, name: &str) -> Result<(), Self::Error>;
}

#[async_trait]
pub trait MutableTableProvider: TableProvider {
    type Error;

    /// Insert a batch into the table.
    async fn insert(&self, ctx: &SessionContext, batch: RecordBatch) -> Result<(), Self::Error>;
}
