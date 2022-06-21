use crate::{Result, StoreError};
use async_trait::async_trait;
use coretypes::column::{ColumnVec, NullableColumnVec};
use coretypes::datatype::{RelationSchema, Row};
use coretypes::expr::ScalarExpr;

/// Client interface to an underlying storage engine.
#[async_trait]
pub trait Client: Sync + Send {
    /// Create a new relation with the given name.
    async fn create_relation(&self, name: &str, schema: RelationSchema) -> Result<()>;

    /// Delete a relation with the given name. Errors if the relation does not
    /// exist.
    async fn delete_relation(&self, name: &str) -> Result<()>;

    /// Get a relation schema.
    async fn get_relation(&self, name: &str) -> Result<Option<RelationSchema>>;

    async fn insert(&self, table: &str, row: &Row) -> Result<()>;

    async fn scan(
        &self,
        projections: Vec<ScalarExpr>,
        filter: Option<ScalarExpr>,
        limit: usize,
    ) -> Result<Vec<NullableColumnVec>>;
}
