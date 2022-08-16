use anyhow::{Result};
use lemur::repr::{df::Schema, expr::RelationKey};
use lemur::repr::value::ValueType;
use serde::{Deserialize, Serialize};
use std::fmt;

pub mod system;
pub mod dummy;

pub trait CatalogReader: Sync + Send {
    /// Get a table by a reference, returning `None` if no table exists (or
    /// isn't visible).
    fn get_table(&self, reference: &TableReference) -> Result<Option<TableSchema>>;

    fn get_table_by_name(
        &self,
        catalog: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<(TableReference, TableSchema)>>;

    fn current_catalog(&self) -> &str;
    fn current_schema(&self) -> &str;
}

pub trait CatalogWriter: CatalogReader {
    /// Add a new table.
    fn add_table(&self, reference: &TableReference, schema: TableSchema) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>,
    pub pk_idxs: Vec<usize>,
}

impl TableSchema {
    pub fn to_schema(&self) -> Schema {
        self.columns
            .iter()
            .map(|col| col.ty.clone())
            .collect::<Vec<_>>()
            .into()
    }
}

impl From<TableSchema> for Schema {
    fn from(ts: TableSchema) -> Self {
        ts.to_schema()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub ty: ValueType,
    pub nullable: bool,
}

/// A fully qualified table reference.
///
/// This reference must be unique across the entire system.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TableReference {
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

impl fmt::Display for TableReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.catalog, self.schema, self.table)
    }
}

impl From<TableReference> for RelationKey {
    fn from(tr: TableReference) -> Self {
        format!("{}", tr)
    }
}

impl From<&TableReference> for RelationKey {
    fn from(tr: &TableReference) -> Self {
        format!("{}", &tr)
    }
}
