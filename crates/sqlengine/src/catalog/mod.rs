use anyhow::{anyhow, Result};
use lemur::repr::df::Schema;
use lemur::repr::value::ValueType;
use serde::{Deserialize, Serialize};
use std::fmt;

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

    fn current_catalog(&self) -> (CatalogId, &str);
    fn current_schema(&self) -> (SchemaId, &str);
}

pub trait CatalogWriter: CatalogReader {
    /// Add a new table.
    fn add_table(&self, reference: &TableReference, schema: TableSchema) -> Result<()>;
}

pub type CatalogId = u64;
pub type SchemaId = u64;
pub type TableId = u64;
pub type ColumnId = u64;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>,
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
    pub catalog: CatalogId,
    pub schema: SchemaId,
    pub table: TableId,
}

impl fmt::Display for TableReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.catalog, self.schema, self.table)
    }
}

/// A fully qualified column reference.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnReference {
    pub catalog: CatalogId,
    pub schema: SchemaId,
    pub table: TableId,
    pub column: ColumnId,
}
