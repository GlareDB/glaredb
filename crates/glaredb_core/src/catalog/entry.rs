use std::fmt;
use std::sync::Arc;

use glaredb_error::{DbError, Result};

use crate::arrays::field::Field;
use crate::functions::function_set::{AggregateFunctionSet, ScalarFunctionSet, TableFunctionSet};
use crate::storage::storage_manager::StorageTableId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogEntryType {
    Table,
    Schema,
    View,
    ScalarFunction,
    AggregateFunction,
    TableFunction,
    CopyToFunction,
    CastFunction,
}

impl fmt::Display for CatalogEntryType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Table => write!(f, "table"),
            Self::Schema => write!(f, "schema"),
            Self::View => write!(f, "view"),
            Self::ScalarFunction => write!(f, "scalar function"),
            Self::AggregateFunction => write!(f, "aggregate function"),
            Self::TableFunction => write!(f, "table function"),
            Self::CopyToFunction => write!(f, "copy to function"),
            Self::CastFunction => write!(f, "cast function"),
        }
    }
}

/// Describe a single entry in the catalog.
#[derive(Debug)]
pub struct CatalogEntry {
    pub name: String,
    pub entry: CatalogEntryInner,
    pub child: Option<Arc<CatalogEntry>>,
}

#[derive(Debug)]
pub enum CatalogEntryInner {
    Table(TableEntry),
    Schema(SchemaEntry),
    View(ViewEntry),
    ScalarFunction(ScalarFunctionEntry),
    AggregateFunction(AggregateFunctionEntry),
    TableFunction(TableFunctionEntry),
}

#[derive(Debug)]
pub struct ScalarFunctionEntry {
    pub function: ScalarFunctionSet,
}

#[derive(Debug)]
pub struct AggregateFunctionEntry {
    pub function: AggregateFunctionSet,
}

#[derive(Debug)]
pub struct TableFunctionEntry {
    /// The table function.
    pub function: TableFunctionSet,
}

#[derive(Debug, Clone)]
pub struct TableEntry {
    /// Columns in this table.
    pub columns: Vec<Field>,
    /// Scan function to use for reading the table.
    ///
    /// This should currently expect to accept 'catalog', 'schema', and 'table'
    /// as string arguments.
    pub function: TableFunctionSet,
    /// Identifier for getting the data table from the storage manager.
    // TODO: This should be opaque. Different storage managers should be able to
    // different identifiers here.
    pub storage_id: StorageTableId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewEntry {
    pub column_aliases: Option<Vec<String>>,
    pub query_sql: String,
}

#[derive(Debug, PartialEq, Eq)]
pub struct SchemaEntry {}

impl CatalogEntry {
    pub fn entry_type(&self) -> CatalogEntryType {
        match &self.entry {
            CatalogEntryInner::Table(_) => CatalogEntryType::Table,
            CatalogEntryInner::Schema(_) => CatalogEntryType::Schema,
            CatalogEntryInner::View(_) => CatalogEntryType::View,
            CatalogEntryInner::ScalarFunction(_) => CatalogEntryType::ScalarFunction,
            CatalogEntryInner::AggregateFunction(_) => CatalogEntryType::AggregateFunction,
            CatalogEntryInner::TableFunction(_) => CatalogEntryType::TableFunction,
        }
    }

    pub fn try_as_table_entry(&self) -> Result<&TableEntry> {
        match &self.entry {
            CatalogEntryInner::Table(ent) => Ok(ent),
            _ => Err(DbError::new("Entry not a table")),
        }
    }

    pub fn try_as_view_entry(&self) -> Result<&ViewEntry> {
        match &self.entry {
            CatalogEntryInner::View(ent) => Ok(ent),
            _ => Err(DbError::new("Entry not a view")),
        }
    }

    pub fn try_as_schema_entry(&self) -> Result<&SchemaEntry> {
        match &self.entry {
            CatalogEntryInner::Schema(ent) => Ok(ent),
            _ => Err(DbError::new("Entry not a schema")),
        }
    }

    pub fn try_as_scalar_function_entry(&self) -> Result<&ScalarFunctionEntry> {
        match &self.entry {
            CatalogEntryInner::ScalarFunction(ent) => Ok(ent),
            _ => Err(DbError::new("Entry not a scalar function")),
        }
    }

    pub fn try_as_aggregate_function_entry(&self) -> Result<&AggregateFunctionEntry> {
        match &self.entry {
            CatalogEntryInner::AggregateFunction(ent) => Ok(ent),
            _ => Err(DbError::new("Entry not an aggregate function")),
        }
    }

    pub fn try_as_table_function_entry(&self) -> Result<&TableFunctionEntry> {
        match &self.entry {
            CatalogEntryInner::TableFunction(ent) => Ok(ent),
            _ => Err(DbError::new("Entry not a table function")),
        }
    }
}
