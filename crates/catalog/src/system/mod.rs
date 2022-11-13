//! Built-in system catalog tables.
pub mod builtin_types;
pub mod relations;
pub mod schemas;
pub mod sequences;

use crate::errors::{CatalogError, Result};
use access::runtime::AccessRuntime;
use access::table::PartitionedTable;
use catalog_types::keys::SchemaId;
use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::MemTable;
use std::collections::HashMap;
use std::sync::Arc;

use builtin_types::BuiltinTypesTable;
use relations::RelationsTable;
use schemas::SchemasTable;
use sequences::SequencesTable;

/// Maximum reserved schema id. All user schemas are required to be greater than
/// this.
pub const MAX_RESERVED_SCHEMA_ID: SchemaId = 100;

pub const SYSTEM_SCHEMA_NAME: &str = "system_catalog";
pub const SYSTEM_SCHEMA_ID: SchemaId = 0;

/// The types of table that a "system" table can be.
///
/// NOTE: This will eventually included a "cloud" table type for interactive
/// with the Cloud service.
pub enum SystemTable {
    /// Completely in-memory and nothing is persisted. Presented a view to the
    /// user.
    View(MemTable),
    /// A persistent table backed by a partitioned table.
    Base(PartitionedTable),
}

impl SystemTable {
    pub fn get_partitioned_table(&self) -> Result<&PartitionedTable> {
        match self {
            SystemTable::View(_) => Err(CatalogError::TableReadonly),
            SystemTable::Base(table) => Ok(table),
        }
    }
}

impl From<MemTable> for SystemTable {
    fn from(table: MemTable) -> Self {
        SystemTable::View(table)
    }
}

impl From<PartitionedTable> for SystemTable {
    fn from(table: PartitionedTable) -> Self {
        SystemTable::Base(table)
    }
}

pub trait SystemTableAccessor {
    /// Return the schema of the table.
    fn schema(&self) -> &Schema;

    /// Return the constant name for the table.
    fn name(&self) -> &'static str;

    /// Returns whether or not this table is read-only.
    fn is_readonly(&self) -> bool;

    /// Get the underlying table type.
    fn get_table(&self, runtime: Arc<AccessRuntime>) -> SystemTable;
}

pub struct SystemSchema {
    tables: HashMap<&'static str, Arc<dyn SystemTableAccessor>>,
}

impl SystemSchema {
    pub fn bootstrap() -> Result<SystemSchema> {
        let tables: &[Arc<dyn SystemTableAccessor>] = &[
            Arc::new(BuiltinTypesTable::new()),
            Arc::new(RelationsTable::new()),
            Arc::new(SchemasTable::new()),
            Arc::new(SequencesTable::new()),
        ];

        let tables: HashMap<_, _> = tables
            .into_iter()
            .map(|table| (table.name(), table.clone()))
            .collect();

        Ok(SystemSchema { tables })
    }

    pub fn get_system_table(&self, name: &str) -> Option<&Arc<dyn SystemTableAccessor>> {
        self.tables.get(name)
    }
}
